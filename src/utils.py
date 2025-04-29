import logging
import re
import os
import json
import math # Added for parse_size if needed, can remove if only integer math is used
import time # <-- Added import

# --- Logging Setup ---
# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
# Get logger (individual modules can get their own logger using logging.getLogger(__name__))
log = logging.getLogger("json_splitter") # Use a common root logger name

# --- Configuration & Constants ---
PROGRESS_REPORT_INTERVAL = 10000 # Report progress every N items

# Define valid strategies
VALID_SPLIT_STRATEGIES = {'count', 'size', 'key'}

# --- Helper Functions ---

def parse_size(size_str):
    """Parses a size string (e.g., '100MB', '1G') into bytes.

    Handles common units (B, KB, MB, GB, TB) case-insensitively.
    Allows integer and floating-point numbers.
    Defaults to bytes if no unit is specified.

    Raises:
        ValueError: If the format is invalid or the unit is unknown.
    """
    size_str = str(size_str).strip().upper()
    original_input_for_error = size_str # Store for potential error messages
    if not size_str:
        raise ValueError("Size string cannot be empty.")

    # Check for unit-only input (like "MB")
    if size_str in ('B', 'KB', 'MB', 'GB', 'TB', 'K', 'M', 'G', 'T'):
        raise ValueError(f"Missing numeric value before suffix in '{original_input_for_error}'")

    # Check for negative input - use the original non-upper()'d string for the error message if desired
    # Let's refine the error check based on the test expectation
    # The test for "-1MB" expected: ValueError("Invalid numeric value '-1' in size string '-1MB'")
    # This implies the number itself is the problem *within* the context of parsing.
    # Let's match a potentially negative number first.
    pre_match = re.match(r'^([-+]?)(\d+(\.\d+)?)\s*([KMGT]?B?)$', size_str)

    if pre_match:
        sign = pre_match.group(1)
        num_part_check = pre_match.group(2)
        if sign == '-':
            # Raise the specific error the test expects for negative numbers during parse time
            # We need the *original* case string here ideally, but the test used "-1MB" which is uppercase anyway.
            # Let's reconstruct the failing part for the message based on parsed components.
            raise ValueError(f"Invalid numeric value '-{num_part_check}' in size string '{original_input_for_error}'")
            # Note: parse_size("-1MB") -> original_input_for_error = "-1MB", num_part_check = "1" -> Error("Invalid numeric value '-1' in size string '-1MB'") - This matches!

    # Regex to extract POSITIVE numeric part and optional unit (now that negative is handled)
    match = re.match(r'^(\d+(\.\d+)?)\s*([KMGT]?B?)$', size_str)
    if not match:
        # Fallback for plain numbers (assume bytes)
        if re.match(r'^\d+(\.\d+)?$', size_str):
            num_part = size_str
            unit = 'B' # Assume bytes if no unit
        else:
            # Restore the original general error message for formats not caught by specific checks.
            raise ValueError(f"Invalid size format: '{original_input_for_error}'. Use formats like 100, 100KB, 50.5MB, 1GB.")
    else:
        num_part = match.group(1)
        unit = match.group(3) if match.group(3) else 'B' # Default to Bytes if unit is missing

    try:
        val = float(num_part)
    except ValueError:
        # This should technically not happen due to regex, but as a safeguard
        raise ValueError(f"Could not parse numeric value from '{num_part}' in '{original_input_for_error}'")

    if val < 0:
        raise ValueError("Size cannot be negative.")

    multipliers = {
        'B': 1,
        'KB': 1024,
        'MB': 1024**2,
        'GB': 1024**3,
        'TB': 1024**4
    }

    # Adjust unit if only K, M, G, T is provided
    if unit in ('K', 'M', 'G', 'T'):
        unit += 'B'

    if unit not in multipliers:
        # Should not happen if regex is correct, but good to have
        raise ValueError(f"Unknown size unit '{unit}' in '{original_input_for_error}'. Use B, KB, MB, GB, TB.")

    return int(val * multipliers[unit])


def sanitize_filename(filename):
    """Removes or replaces characters potentially problematic in filenames,
    matching the expectations of the original test suite.
    """
    # Convert to string first
    filename = str(filename)

    # --- Modifications to align with tests ---
    # 1. Strip leading/trailing whitespace FIRST
    filename = filename.strip()

    # 2. Replace problematic chars (including sequences) with a single underscore
    # Keep alphanumeric, underscore, hyphen, dot. Replace others.
    # Need to be careful with the order. Replace disallowed first.
    # Old regex: [^a-zA-Z0-9_.-]+
    # New regex: Only remove known problematic chars, control chars, and whitespace.
    # Allows unicode letters like 'é' to pass through.
    # Added \s to handle spaces correctly as per test_sanitize_spaces and collapsing sequences like ' / '.
    sanitized = re.sub(r'[<>:"/\\|?*\x00-\x1F\s]+', '_', filename)

    # 3. Strip leading/trailing underscores AFTER replacement
    sanitized = sanitized.strip('_')

    # 4. Limit length to 100 bytes (as per test_sanitize_long_filename_truncation)
    max_bytes = 100
    if len(sanitized.encode('utf-8')) > max_bytes:
        # Truncate carefully to respect multi-byte character boundaries
        truncated = ''
        current_bytes = 0
        for char in sanitized:
            char_bytes = len(char.encode('utf-8'))
            if current_bytes + char_bytes <= max_bytes:
                truncated += char
                current_bytes += char_bytes
            else:
                break
        sanitized = truncated
        # Re-strip underscores in case truncation created trailing ones
        sanitized = sanitized.strip('_')

    # 5. Handle empty result (as per test_sanitize_empty_result)
    # Check *after* potential truncation and final stripping
    if not sanitized:
        return "__empty__" # Test expects this specific string
    # --- End Modifications ---

    return sanitized

# --- Progress Tracking --- # <-- Added Section Header
class ProgressTracker:
    """Tracks and reports progress of processing operations."""

    def __init__(self, logger, report_interval=10000):
        """
        Initializes the tracker.

        Args:
            logger: The logging instance to use for reporting.
            report_interval (int): Report progress every N items.
        """
        self.total_items = 0
        self.last_reported_item_count = 0 # Track items at last report
        self.start_time = time.time()
        self.report_interval = report_interval
        self.log = logger # Store the logger instance

    def update(self, current_total_items):
        """Update progress and report if interval reached."""
        self.total_items = current_total_items # Update total count
        # Report if the number of *new* items since last report meets/exceeds interval
        if (self.total_items - self.last_reported_item_count) >= self.report_interval:
            elapsed = time.time() - self.start_time
            # Calculate rate based on total items over total time
            rate = self.total_items / elapsed if elapsed > 0 else 0
            self.log.info(f"  Processed {self.total_items:,} items... ({rate:.2f} items/sec)")
            self.last_reported_item_count = self.total_items # Update marker

    def finalize(self):
        """Report final statistics."""
        elapsed = time.time() - self.start_time
        # Ensure we don't report 0 items if nothing was processed
        if self.total_items > 0:
             rate = self.total_items / elapsed if elapsed > 0 else 0
             self.log.info(f"Complete: Processed {self.total_items:,} items in {elapsed:.2f}s ({rate:.2f} items/sec)")
        else:
             self.log.info(f"Complete: Processed 0 items in {elapsed:.2f}s")


# --- Input Validation --- # <-- Adjusted Section Header
def validate_inputs(input_file, output_prefix, split_by, value=None, path=None):
    """Validate all inputs before starting the splitting process.

    Returns:
        list: A list of error messages. Empty if all checks pass.
    """
    errors = []

    # 1. Input File Validation
    if not input_file:
        errors.append("Input file path is required.")
    elif not os.path.isfile(input_file):
        errors.append(f"Input file not found: {input_file}")
    elif not os.access(input_file, os.R_OK):
        errors.append(f"Input file is not readable: {input_file}")
    else:
        # Optional: Check if file is empty
        try:
            if os.path.getsize(input_file) == 0:
                log.warning(f"Input file is empty: {input_file}") # Warning, not error
        except OSError as e:
            errors.append(f"Could not get size of input file {input_file}: {e}")
            # Prevent further checks on this file if size check fails
            return errors # Return early

        # Optional: Basic JSON check (reads first few bytes/chars)
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                # Try to peek at the first non-whitespace character
                content_start = ""
                whitespace_only = True
                # Read reasonably small chunk to find first non-whitespace char
                chunk = f.read(512)
                for char in chunk:
                     if not char.isspace():
                         content_start = char
                         whitespace_only = False
                         break
                if not chunk and not whitespace_only: # File might be truly empty, handled above
                     pass
                elif whitespace_only and os.path.getsize(input_file) > 0: # File > 0 bytes but read chunk was all whitespace
                     log.warning(f"Input file seems to contain only whitespace: {input_file}")
                elif content_start not in ('{', '['):
                    # Provide more context in the error message
                    preview = chunk[:50].replace('\\n', '\\\\n').replace('\\r', '\\\\r') # Show first 50 chars
                    errors.append(f"Input file does not appear to start with a valid JSON structure ('{{' or '['). Found start: '{preview}...'. File: {input_file}")
        except UnicodeDecodeError:
             errors.append(f"Input file is not valid UTF-8 encoded: {input_file}")
        except Exception as e:
             log.warning(f"Could not perform basic JSON check on {input_file}: {e}") # Non-fatal


    # 2. Output Prefix Validation
    if not output_prefix:
         errors.append("Output prefix is required.")
    else:
        output_dir = os.path.dirname(output_prefix)
        # Handle case where prefix is just a filename in the current dir
        if not output_dir:
             output_dir = "."

        # Check if path component exists *before* trying to create
        if not os.path.exists(output_dir):
            try:
                log.info(f"Output directory does not exist. Attempting to create: {output_dir}")
                os.makedirs(output_dir, exist_ok=True)
                # Check writability *after* creation attempt
                if not os.access(output_dir, os.W_OK):
                     errors.append(f"Created output directory is not writable: {output_dir}")
            except OSError as e:
                errors.append(f"Cannot create output directory '{output_dir}': {e}")
            except Exception as e: # Catch other potential errors during makedirs
                 errors.append(f"An unexpected error occurred creating directory '{output_dir}': {e}")
        # If dir exists, check if it's actually a dir and writable
        elif not os.path.isdir(output_dir):
             errors.append(f"Output path exists but is not a directory: {output_dir}")
        elif not os.access(output_dir, os.W_OK):
            errors.append(f"Output directory is not writable: {output_dir}")

        # Check filename part of prefix for invalid characters (basic check)
        # This depends heavily on the target OS, but '/' and '\\' are common issues
        output_basename = os.path.basename(output_prefix)
        # Check for empty basename which can happen if prefix ends in '/'
        if not output_basename and output_prefix.endswith(('/', '\\')):
             errors.append("Output prefix cannot end with a directory separator ('/' or '\\'). It should include a filename base.")
        elif '/' in output_basename or '\\' in output_basename:
             errors.append(f"Output prefix's filename component '{output_basename}' contains invalid path separators.")
        # Add more checks here if needed (e.g., null bytes, control chars)


    # 3. Split Strategy Validation
    if not split_by:
        errors.append("Split strategy (--split-by) is required.")
    elif split_by not in VALID_SPLIT_STRATEGIES:
        valid_options_str = "', '".join(sorted(list(VALID_SPLIT_STRATEGIES)))
        errors.append(f"Invalid split strategy '{split_by}'. Valid options are: '{valid_options_str}'")
    else:
        # 4. Value Validation (depends on split_by)
        # Use specific arg names (e.g., --count, --size, --key-name) in errors if possible
        if split_by == 'count':
            if value is None:
                errors.append("A positive integer value (e.g., --count 1000) is required for --split-by 'count'.")
            else:
                try:
                    count_val = int(value)
                    if count_val <= 0:
                        errors.append(f"Value for 'count' must be a positive integer, got: {value}")
                except (ValueError, TypeError):
                    errors.append(f"Value for 'count' must be a valid integer, got: '{value}' ({type(value).__name__})")
        elif split_by == 'size':
            if value is None:
                 errors.append("A size value (e.g., --size 10MB) is required for --split-by 'size'.")
            else:
                try:
                    # Use the improved parse_size function
                    size_val = parse_size(value)
                    if size_val <= 0:
                        # parse_size should ideally catch non-positive numeric parts,
                        # but maybe the value was '0MB'. This adds robustness.
                         errors.append(f"Value for 'size' must represent a positive size, evaluated to {size_val} bytes from: '{value}'")
                except ValueError as e:
                    errors.append(f"Invalid value format for 'size': {e}. Use formats like 100KB, 50MB, 1GB. Got: '{value}'")
                except Exception as e: # Catch unexpected errors from parse_size
                    errors.append(f"Unexpected error parsing size value '{value}': {e}")

        elif split_by == 'key':
             # For 'key' split, the 'value' parameter holds the key name
             if value is None:
                 errors.append("A key name (e.g., --key-name 'user_id') is required for --split-by 'key'.")
             elif not isinstance(value, str) or not value.strip():
                 errors.append(f"The key name for 'key' split must be a non-empty string, got: '{value}' ({type(value).__name__})")
             # Key name syntax validation is complex, maybe check for obviously bad chars?
             # elif re.search(r'[\\/\0]', value): # Example: check for backslash, slash, null byte
             #     errors.append(f"The key name '{value}' contains invalid characters.")


    # 5. Path Validation (JSON Path)
    # JSON Path validation is complex. A simple check might be:
    if path is not None:
        if not isinstance(path, str):
             errors.append(f"JSON path (--path) must be a string, got: {type(path).__name__}")
        # Basic structural checks for common JSON Path patterns?
        # E.g., should likely start with '$' or be empty/None for root.
        # elif path.strip() and not path.startswith(('$', '.' ,'[')):
        #    errors.append(f"JSON path ('{path}') should typically start with '$', '.', or '['.")
        # Note: ijson might handle path variations; keep validation minimal unless specific formats are required.
        pass # Keep path validation simple for now


    return errors


# --- Main Application Logic (Example Placeholder) ---
# Example: Function that might use the validation
def run_split(args):
    # Assume args is an object or dict with attributes like
    # args.input_file, args.output_prefix, args.split_by, args.value, args.path
    validation_errors = validate_inputs(
        getattr(args, 'input_file', None),
        getattr(args, 'output_prefix', None),
        getattr(args, 'split_by', None),
        getattr(args, 'value', None), # Pass the appropriate value based on split_by
        getattr(args, 'path', None)
    )

    if validation_errors:
        log.error("Input validation failed:")
        for error in validation_errors:
            log.error(f"  - {error}")
        # Exit or raise exception
        raise ValueError("Input validation failed.") # Or sys.exit(1)

    log.info("Input validation successful. Proceeding with split...")
    # ... rest of the splitting logic ...

# --- Testing / Example Usage (Optional) ---
if __name__ == '__main__':
    # Example of calling parse_size
    print(f"100MB = {parse_size('100MB')} bytes")
    print(f"1.5GB = {parse_size('1.5GB')} bytes")
    print(f"512   = {parse_size('512')} bytes")
    try:
        parse_size("100 K") # Invalid format example
    except ValueError as e:
        print(f"Caught expected error: {e}")

    # Example of calling validate_inputs
    class MockArgs:
        input_file = "test_data/sample.json" # Assume exists for test
        output_prefix = "output/split_"
        split_by = "count"
        value = "100"
        path = "$.items[*]"

    # Create dummy files/dirs for testing if needed
    if not os.path.exists("test_data"): os.makedirs("test_data")
    if not os.path.exists("output"): os.makedirs("output")
    if not os.path.exists(MockArgs.input_file):
         with open(MockArgs.input_file, 'w') as f: f.write('[{"id": 1}]') # Create dummy json

    errors = validate_inputs(
        MockArgs.input_file, MockArgs.output_prefix, MockArgs.split_by,
        MockArgs.value, MockArgs.path
    )
    if errors:
        print("\nValidation Errors Found:")
        for err in errors: print(f" - {err}")
    else:
        print("\nValidation Successful!")

    # Example with invalid input
    print("\nTesting invalid input:")
    errors_invalid = validate_inputs("nonexistent.json", "/invalid/path/pref", "bad_strategy", "-5")
    for err in errors_invalid: print(f" - {err}")