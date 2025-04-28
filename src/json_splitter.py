import argparse
import json
import ijson
import os
import re # Needed for filename sanitization
import logging # Added for logging

# --- Logging Setup ---
# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
# Get logger for this module
log = logging.getLogger(__name__)
# ---

def split_by_count(input_file, output_prefix, count, path, output_format, max_records=None, max_size_bytes=None, filename_format="{prefix}_{type}_{index:04d}{part}.{ext}"):
    """Splits a JSON array based on element count, with optional secondary size/record limits."""
    # Determine the effective primary record limit per chunk/file part
    effective_primary_count = count
    if max_records is not None and max_records < count:
        log.warning(f"--max-records ({max_records}) is less than primary count ({count}). Effective primary split count per file part will be {max_records}.")
        effective_primary_count = max_records

    try:
        log.info(f"Splitting '{input_file}' at path '{path}' primarily by count={count}...")
        if max_records: log.info(f"  Secondary limit: Max {max_records} records per file.")
        if max_size_bytes: log.info(f"  Secondary limit: Max ~{max_size_bytes / (1024*1024):.2f} MB per file.") # Show size in MB

        with open(input_file, 'rb') as f:
            items_iterator = ijson.items(f, path)
            chunk = []
            primary_chunk_index = 0 # Index for the main count-based groups
            part_file_index = 0   # Index for files split by secondary limits within a primary group
            item_count_total = 0
            current_part_size_bytes = 0
            # Estimate overhead based on format
            base_overhead = 2 if output_format == 'json' else 0 # [] or nothing
            per_item_overhead = 4 if output_format == 'json' else 1 # ,\\n indented or \\n
            last_progress_report_item = 0
            PROGRESS_REPORT_INTERVAL = 10000 # Report progress every N items

            for item_count_total, item in enumerate(items_iterator, 1): # Start count from 1
                # --- Progress Reporting ---
                if item_count_total % PROGRESS_REPORT_INTERVAL == 0:
                    log.info(f"  Processed {item_count_total} items...")
                    last_progress_report_item = item_count_total
                # ---

                item_size = 0
                # Estimate size if max_size_bytes is set
                if max_size_bytes:
                    try:
                        item_bytes = json.dumps(item).encode('utf-8')
                        item_size = len(item_bytes)
                    except TypeError as e:
                        log.warning(f"Could not serialize item {item_count_total} to estimate size: {e}. Skipping size check for this item.")
                        item_size = 0

                # Determine if the *current file part* needs to end before adding this item
                split_needed = False
                primary_split_occurred = False # Flag to track if the split is due to the primary count
                if chunk:
                    # 1. Primary count limit for the *part* reached
                    if len(chunk) == effective_primary_count:
                        log.debug(f"Primary count limit ({effective_primary_count}) reached for chunk {primary_chunk_index}, part {part_file_index}.")
                        split_needed = True
                        primary_split_occurred = True # Mark this split as primary
                    # 2. Secondary max_size limit reached (only if primary count not reached)
                    elif max_size_bytes and (current_part_size_bytes + item_size + per_item_overhead) > max_size_bytes:
                        log.debug(f"Secondary size limit (~{max_size_bytes / (1024*1024):.2f}MB) reached for chunk {primary_chunk_index}, part {part_file_index}.")
                        split_needed = True
                        # primary_split_occurred remains False here

                if split_needed:
                    # Write the current part
                    _write_chunk(output_prefix, primary_chunk_index, chunk, output_format, part_index=part_file_index, filename_format=filename_format)

                    # If the split was due to reaching the *primary* count limit,
                    # it means we are starting a new primary group.
                    if primary_split_occurred:
                         log.debug(f"Starting new primary chunk {primary_chunk_index + 1}.")
                         primary_chunk_index += 1
                         part_file_index = 0 # Reset part index for the new primary group
                    else:
                         # Otherwise, it was a secondary split (size), just increment the part index
                         log.debug(f"Starting new part {part_file_index + 1} for primary chunk {primary_chunk_index}.")
                         part_file_index += 1

                    # Reset variables for the next part (whether primary or secondary)
                    chunk = []
                    current_part_size_bytes = base_overhead


                # Add item to the current chunk/part
                chunk.append(item)
                current_part_size_bytes += item_size + (per_item_overhead if len(chunk)>1 else base_overhead)


            # Write any remaining items in the last chunk
            if chunk:
                _write_chunk(output_prefix, primary_chunk_index, chunk, output_format, part_index=part_file_index, filename_format=filename_format)

            # Report final count if not perfectly divisible by interval
            if item_count_total > last_progress_report_item:
                log.info(f"  Processed {item_count_total} items total.")

            log.info(f"Splitting complete. Total items processed: {item_count_total}. Files created: Check output directory.")

    except FileNotFoundError:
        log.error(f"Error: Input file '{input_file}' not found.")
    except ijson.JSONError as e:
        # Attempt to extract position info from the error
        position_info = getattr(e, 'pos', None)
        line, col = getattr(e, 'lineno', None), getattr(e, 'colno', None)
        pos_str = f" near position {position_info}" if position_info is not None else ""
        line_col_str = f" around line {line}, column {col}" if line is not None and col is not None else ""
        log.error(f"Error parsing JSON{pos_str}{line_col_str}: {e}.\nCheck input file validity and path '{path}'.")
    except Exception as e:
        log.exception(f"An unexpected error occurred during count splitting:") # Use log.exception to include traceback


def split_by_size(input_file, output_prefix, max_size_bytes, path, output_format, max_records=None, filename_format="{prefix}_{type}_{index:04d}{part}.{ext}"):
    """Splits JSON elements based on approximate output file size using streaming.
       Also supports secondary max_records limit.

    Note: Size estimation involves serializing each item individually, which adds
    overhead and is an approximation. Actual file sizes may vary slightly.
    JSON output includes estimated overhead for brackets and commas/newlines.
    JSONL output size is closer to the sum of item sizes.
    """
    try:
        log.info(f"Splitting '{input_file}' at path '{path}' primarily by size=~{max_size_bytes / (1024*1024):.2f} MB...")
        if max_records: log.info(f"  Secondary limit: Max {max_records} records per file.")

        with open(input_file, 'rb') as f:
            effective_path = path if path else ''
            items_iterator = ijson.items(f, effective_path)

            chunk = []
            current_chunk_size_bytes = 0
            file_index = 0       # Primary file index
            part_file_index = 0 # Index for parts if secondary limit hit
            item_count_total = 0
            base_overhead = 2 if output_format == 'json' else 0
            per_item_overhead = 4 if output_format == 'json' else 1
            last_progress_report_item = 0
            PROGRESS_REPORT_INTERVAL = 10000 # Report progress every N items

            for item_count_total, item in enumerate(items_iterator, 1): # Start count from 1
                # --- Progress Reporting ---
                if item_count_total % PROGRESS_REPORT_INTERVAL == 0:
                    log.info(f"  Processed {item_count_total} items...")
                    last_progress_report_item = item_count_total
                # ---

                item_size = 0
                try:
                    item_bytes = json.dumps(item).encode('utf-8')
                    item_size = len(item_bytes)
                except TypeError as e:
                    log.warning(f"Could not serialize item {item_count_total} to estimate size: {e}. Skipping size check.")
                    item_size = 0

                # Determine if a split is needed before adding this item
                split_needed = False
                primary_split_occurred = False # Track if split is due to primary (size) or secondary (count) limit
                estimated_next_size = 0 # Initialize
                if chunk:
                    estimated_next_size = current_chunk_size_bytes + item_size + per_item_overhead
                    # 1. Primary size limit reached
                    if estimated_next_size > max_size_bytes:
                        log.debug(f"Primary size limit (~{max_size_bytes / (1024*1024):.2f}MB) reached for file {file_index}, part {part_file_index}.")
                        split_needed = True
                        primary_split_occurred = True
                    # 2. Secondary records limit reached
                    elif max_records and len(chunk) == max_records:
                        log.debug(f"Secondary record limit ({max_records}) reached for file {file_index}, part {part_file_index}.")
                        split_needed = True
                        # primary_split_occurred remains False

                if split_needed:
                    _write_chunk(output_prefix, file_index, chunk, output_format, part_index=part_file_index, filename_format=filename_format)

                    # If the split was due to the primary size limit, start a new primary file group
                    if primary_split_occurred:
                         log.debug(f"Starting new primary file {file_index + 1}.")
                         file_index += 1
                         part_file_index = 0 # Reset part index for new primary file
                    else:
                        # Otherwise, it was a secondary split (count), just increment the part index
                        log.debug(f"Starting new part {part_file_index + 1} for primary file {file_index}.")
                        part_file_index += 1

                    # Reset variables for the next part
                    chunk = []
                    current_chunk_size_bytes = base_overhead

                # Add item to the current chunk
                chunk.append(item)
                current_chunk_size_bytes += item_size + (per_item_overhead if len(chunk)>1 else base_overhead)


            # Write any remaining items in the last chunk
            if chunk:
                _write_chunk(output_prefix, file_index, chunk, output_format, part_index=part_file_index, filename_format=filename_format)

            # Report final count if not perfectly divisible by interval
            if item_count_total > last_progress_report_item:
                log.info(f"  Processed {item_count_total} items total.")

            log.info(f"Splitting complete. Total items processed: {item_count_total}. Files created: Check output directory.")

    except FileNotFoundError:
        log.error(f"Error: Input file '{input_file}' not found.")
    except ijson.JSONError as e:
        position_info = getattr(e, 'pos', None)
        line, col = getattr(e, 'lineno', None), getattr(e, 'colno', None)
        pos_str = f" near position {position_info}" if position_info is not None else ""
        line_col_str = f" around line {line}, column {col}" if line is not None and col is not None else ""
        log.error(f"Error parsing JSON{pos_str}{line_col_str}: {e}.\nCheck input file validity and path '{path}'.")
    except Exception as e:
        log.exception(f"An unexpected error occurred during size splitting:")


def main():
    parser = argparse.ArgumentParser(description="Split large JSON files using streaming.")
    parser.add_argument("input_file", help="Path to the input JSON file.")
    parser.add_argument("output_prefix", help="Prefix for the output files (e.g., 'output/chunk').")
    parser.add_argument("--split-by", required=True, choices=['count', 'size', 'key'], help="Criterion to split by ('count', 'size', or 'key').")
    parser.add_argument("--value", required=True, type=str, help="Value for the splitting criterion (e.g., number of items for 'count', size like '100MB' for 'size', the key name for 'key').")
    parser.add_argument("--path", required=True, help="JSON path to the array/objects to split (e.g., 'item' for root array, 'data.records.item'). For 'key' splitting, this should point to the objects containing the key.")
    parser.add_argument("--output-format", choices=['json', 'jsonl'], default='json', help="Output format ('json' or 'jsonl'). Default: json. Note: 'key' splitting currently forces 'jsonl'.")
    # Add secondary constraints
    parser.add_argument("--max-records", type=int, default=None, help="Secondary constraint: Maximum records per output file.")
    parser.add_argument("--max-size", type=str, default=None, help="Secondary constraint: Maximum approximate size per output file (e.g., '50MB').")
    # Key splitting specific options
    parser.add_argument("--on-missing-key", choices=['group', 'skip', 'error'], default='group', help="Action for items missing the specified key when splitting by key (default: group). 'group' puts them in '__missing_key__' file.")
    parser.add_argument("--on-invalid-item", choices=['warn', 'skip', 'error'], default='warn', help="Action for items at target path that are not objects when splitting by key (default: warn). 'warn' prints a message and skips.")
    # Logging control
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose debug logging.")
    # Filename formatting
    parser.add_argument("--filename-format", type=str,
                        default="{prefix}_{type}_{index:04d}{part}.{ext}",
                        help="Format string for output filenames. Available placeholders: "
                             "{prefix}, {type} ('chunk' or 'key'), {index} (primary index/key value), "
                             "{part} (_part_XXXX or empty), {ext} (json/jsonl). Example: chunk_{index}.{ext}")

    args = parser.parse_args()

    # --- Configure Logging Level ---
    if args.verbose:
        log.setLevel(logging.DEBUG)
        log.debug("Verbose logging enabled.")
    else:
        log.setLevel(logging.INFO)
    # ---

    # --- Input Validation ---
    # 1. Check input file existence and readability
    if not os.path.isfile(args.input_file):
        log.error(f"Input file not found: {args.input_file}")
        return
    if not os.access(args.input_file, os.R_OK):
        log.error(f"Input file is not readable (check permissions): {args.input_file}")
        return

    # --- Argument Parsing & Setup ---
    log.debug(f"Input file: {args.input_file}")
    log.debug(f"Output prefix: {args.output_prefix}")
    log.debug(f"Split by: {args.split_by}, Value: {args.value}, Path: {args.path}")
    log.debug(f"Output format: {args.output_format}")
    log.debug(f"Secondary constraints: max_records={args.max_records}, max_size={args.max_size}")
    if args.split_by == 'key':
        log.debug(f"Key split options: on-missing={args.on_missing_key}, on-invalid={args.on_invalid_item}")

    # Parse secondary constraints
    max_records = args.max_records
    max_size_bytes = None
    if args.max_size:
        try:
            max_size_bytes = _parse_size(args.max_size)
            if max_size_bytes <= 0:
                 raise ValueError("Max size must be positive.")
        except ValueError as e:
            log.error(f"Invalid --max-size value: {e}. Use formats like 100KB, 50MB, 1GB.")
            return

    # Create output directory if it doesn't exist and check writability
    output_dir = os.path.dirname(args.output_prefix)
    if output_dir:
        try:
            if not os.path.exists(output_dir):
                log.info(f"Creating output directory: {output_dir}")
                os.makedirs(output_dir, exist_ok=True) # Use exist_ok=True
            # 2. Check if output directory is writable
            if not os.access(output_dir, os.W_OK):
                 log.error(f"Output directory is not writable (check permissions): {output_dir}")
                 return
        except OSError as e:
             log.error(f"Failed to create or access output directory '{output_dir}': {e}")
             return # Exit if cannot create dir
    elif not os.access(os.getcwd(), os.W_OK):
        # If no output dir specified (writing to cwd), check cwd writability
        log.error(f"Current working directory is not writable (check permissions): {os.getcwd()}")
        return

    # Enforce jsonl for key splitting for now
    if args.split_by == 'key' and args.output_format == 'json':
            log.warning("Key-based splitting currently enforces JSON Lines ('jsonl') format for efficiency and reduced memory usage. Overriding --output-format to 'jsonl'.")
            args.output_format = 'jsonl'

    if args.split_by == 'count':
        try:
            count_val = int(args.value)
            if count_val <= 0:
                raise ValueError("Count must be a positive integer.")
            split_by_count(args.input_file, args.output_prefix, count_val, args.path, args.output_format,
                           max_records=max_records, max_size_bytes=max_size_bytes,
                           filename_format=args.filename_format)
        except ValueError:
             log.error(f"Invalid --value for count: '{args.value}'. Must be a positive integer.")
             return

    elif args.split_by == 'size':
        try:
            size_bytes = _parse_size(args.value)
            if size_bytes <= 0:
                raise ValueError("Size must be positive.")
            # Call the size split function
            split_by_size(args.input_file, args.output_prefix, size_bytes, args.path, args.output_format,
                          max_records=max_records,
                          filename_format=args.filename_format) # max_size_bytes is primary here, so less relevant as secondary
        except ValueError as e:
            log.error(f"Invalid --value for size: {e}. Use formats like 100KB, 50MB, 1GB.")
            return
    elif args.split_by == 'key':
        key_name = args.value
        if not key_name:
            log.error("--value must provide a non-empty key name for key-based splitting.")
            return
        split_by_key(args.input_file, args.output_prefix, key_name, args.path, args.output_format,
                       max_records=max_records, max_size_bytes=max_size_bytes,
                       on_missing_key=args.on_missing_key, on_invalid_item=args.on_invalid_item,
                       filename_format=args.filename_format)

    else:
        # This case should not be reachable due to choices constraint
        log.error(f"Internal error: Splitting by '{args.split_by}' is not implemented.")

# Helper function to parse size strings (e.g., "100MB")
def _parse_size(size_str):
    size_str = size_str.strip().upper() # Strip whitespace before parsing
    if size_str.endswith('KB'):
        return int(size_str[:-2]) * 1024
    elif size_str.endswith('MB'):
        return int(size_str[:-2]) * 1024 * 1024
    elif size_str.endswith('GB'):
        return int(size_str[:-2]) * 1024 * 1024 * 1024
    elif size_str.isdigit():
        # Check if it's just digits (bytes)
        return int(size_str)
    else:
        # Handle potential non-numeric prefix before unit
        match = re.match(r'^(\d+)(KB|MB|GB)$', size_str)
        if match:
            val = int(match.group(1))
            unit = match.group(2)
            if unit == 'KB': return val * 1024
            if unit == 'MB': return val * 1024 * 1024
            if unit == 'GB': return val * 1024 * 1024 * 1024
        # If no pattern matches
        raise ValueError(f"Invalid size format: '{size_str}'. Use numbers optionally followed by KB, MB, GB.")

# Helper function to write chunks consistently
def _write_chunk(output_prefix, primary_index, chunk_data, output_format, part_index=None,
                   filename_format="{prefix}_{type}_{index:04d}{part}.{ext}"):
    """Writes a chunk of data to a uniquely named file using a format string."""

    extension = 'jsonl' if output_format == 'jsonl' else 'json'
    # Part suffix is formatted separately for inclusion
    part_suffix = f"_part_{part_index:04d}" if part_index is not None and part_index > 0 else ""

    # Prepare dictionary for format string
    format_args = {
        'prefix': output_prefix,
        'type': 'chunk', # This function is used by count/size which we call 'chunk' based files
        'index': primary_index, # Contains the primary chunk index (0000, 0001 etc)
        'part': part_suffix, # Contains the formatted part string (_part_0000 etc) or empty string
        'ext': extension
    }

    try:
        output_filename = filename_format.format(**format_args)
        # Basic validation of resulting filename (optional but good)
        if not output_filename or '/' in os.path.basename(output_filename) or '\\' in os.path.basename(output_filename):
             log.warning(f"Generated filename '{output_filename}' from format '{filename_format}' seems invalid. Using default naming.")
             # Fallback to old naming
             output_filename = f"{output_prefix}_chunk_{primary_index:04d}{part_suffix}.{extension}"

    except KeyError as e:
        log.error(f"Invalid placeholder '{e}' in --filename-format: '{filename_format}'. Using default naming.")
        # Fallback to old naming
        output_filename = f"{output_prefix}_chunk_{primary_index:04d}{part_suffix}.{extension}"
    except Exception as e:
        log.error(f"Error formatting filename with format '{filename_format}': {e}. Using default naming.")
        # Fallback to old naming
        output_filename = f"{output_prefix}_chunk_{primary_index:04d}{part_suffix}.{extension}"


    log.info(f"  Writing chunk to {output_filename} ({len(chunk_data)} items)...")
    log.debug(f"    Output format: {output_format}, Primary index: {primary_index}, Part index: {part_index}")
    try:
        # Use 'w' mode; each call to _write_chunk creates/overwrites a distinct file part
        with open(output_filename, 'w', encoding='utf-8') as outfile:
            if output_format == 'jsonl':
                for item in chunk_data:
                    json.dump(item, outfile)
                    outfile.write('\n')
            else: # Default to json
                # Ensure the output is a valid JSON structure (usually an array)
                json.dump(chunk_data, outfile, indent=4)
    except IOError as e:
        log.error(f"Error writing to file {output_filename}: {e}")
    except TypeError as e:
         log.error(f"Error serializing data for {output_filename} (potentially non-serializable data): {e}")

# Helper function to sanitize key values for filenames
def _sanitize_filename(value):
    """Removes or replaces characters problematic for filenames.

    Also handles empty values, leading/trailing whitespace/underscores,
    and attempts to truncate based on UTF-8 byte length to avoid exceeding
    common filesystem limits (approx 100 bytes), respecting character boundaries.

    Args:
        value: The value to sanitize (will be converted to string).

    Returns:
        str: A sanitized string suitable for use in filenames.
    """
    # Convert to string
    s_value = str(value)
    # Remove leading/trailing whitespace
    s_value = s_value.strip()
    # Replace spaces and problematic characters with underscores
    # Added '+' to handle sequences of problematic chars as one underscore
    s_value = re.sub(r'[\\s\\\\/:*?\"<>|]+', '_', s_value)
    # Remove any leading/trailing underscores that might result
    s_value = s_value.strip('_')
    # Ensure filename is not empty after sanitization
    if not s_value:
        s_value = "__empty__"
    # Limit length to avoid issues on some filesystems (e.g., 255 bytes common limit)
    # Encode to check byte length, common limit is often byte-based
    try:
        encoded_value = s_value.encode('utf-8')
        max_len = 100 # Keep reasonably short for readability, adjust if needed
        if len(encoded_value) > max_len:
            # Find cut-off point respecting UTF-8 character boundaries
            while len(encoded_value[:max_len]) > max_len:
                 max_len -=1
            s_value = encoded_value[:max_len].decode('utf-8', 'ignore') # Decode back safely
            log.debug(f"Sanitized filename truncated to max length: '{s_value}'")
    except Exception as e:
         log.warning(f"Could not properly truncate sanitized filename '{s_value}': {e}")
         s_value = s_value[:100] # Fallback to simple char length limit

    return s_value

def split_by_key(input_file, output_prefix, key_name, path, output_format,
                   max_records=None, max_size_bytes=None,
                   on_missing_key='group', on_invalid_item='warn',
                   filename_format="{prefix}_key_{index}{part}.{ext}"): # Default format specific to key
    """Splits JSON objects based on the value of a specified key using streaming."""
    # Dictionary to hold state per key value encountered
    # Key: sanitized_key_value
    # Value: { 'handle': file_handle, 'count': current_record_count, 'size': current_estimated_size, 'part': current_part_index }
    key_states = {}
    total_items_processed = 0
    # WARNING: Storing state for every unique key can consume significant memory if the number of unique keys is very large.
    # Consider pre-processing or filtering if memory usage becomes an issue.
    MAX_UNIQUE_KEYS_WARN_THRESHOLD = 1000 # Threshold to warn about potential memory issues
    warned_about_keys = False

    file_format_extension = 'jsonl' # Hardcoded for key splitting
    # Overhead estimation for JSONL
    base_overhead = 0
    per_item_overhead = 1 # Just newline
    last_progress_report_item = 0
    PROGRESS_REPORT_INTERVAL = 10000 # Report progress every N items

    try:
        log.info(f"Splitting '{input_file}' at path '{path}' by key '{key_name}' (format: {file_format_extension})...")
        log.info(f"  Config: on-missing-key={on_missing_key}, on-invalid-item={on_invalid_item}")
        if max_records: log.info(f"  Secondary limit: Max {max_records} records per key file part.")
        if max_size_bytes: log.info(f"  Secondary limit: Max ~{max_size_bytes / (1024*1024):.2f} MB per key file part.")

        with open(input_file, 'rb') as f:
            effective_path = path if path else ''
            items_iterator = ijson.items(f, effective_path)

            for item_count_total, item in enumerate(items_iterator, 1): # Start count from 1
                # --- Progress Reporting ---
                if item_count_total % PROGRESS_REPORT_INTERVAL == 0:
                    log.info(f"  Processed {item_count_total} items...")
                    last_progress_report_item = item_count_total
                # ---

                # Handle non-dict items based on configuration
                if not isinstance(item, dict):
                    msg = f"Item {item_count_total} at path '{path}' is not an object/dict (type: {type(item)})."
                    if on_invalid_item == 'error':
                        log.error(msg)
                        raise TypeError(msg)
                    elif on_invalid_item == 'skip':
                        log.warning(f"Skipping: {msg}")
                        continue
                    else: # Default: warn
                        log.warning(f"{msg} Skipping key check for this item.")
                        continue # Still skip processing this item

                try:
                    key_value = item.get(key_name)
                    sanitized_value = ""
                    should_process = True

                    if key_value is None:
                        # Handle missing key based on configuration
                        if on_missing_key == 'error':
                            msg = f"Key '{key_name}' not found in item {item_count_total}."
                            log.error(msg)
                            raise KeyError(msg)
                        elif on_missing_key == 'skip':
                            log.warning(f"Skipping item {item_count_total}: Key '{key_name}' not found.")
                            should_process = False
                        else: # Default: group
                            sanitized_value = "__missing_key__"
                            log.debug(f"Item {item_count_total}: Key '{key_name}' missing, grouping as '{sanitized_value}'.")
                    elif isinstance(key_value, (dict, list)):
                        # Handle complex types - use a generic name but log warning
                        complex_type_name = type(key_value).__name__
                        sanitized_value = f"__complex_type_{_sanitize_filename(complex_type_name)}__"
                        log.warning(f"Key '{key_name}' in item {item_count_total} has complex type ({complex_type_name}). Grouping into '{sanitized_value}'.")
                    else:
                        sanitized_value = _sanitize_filename(key_value)
                        log.debug(f"Item {item_count_total}: Key '{key_name}' value '{key_value}' sanitized to '{sanitized_value}'.")

                    if not should_process:
                        continue

                    # Get or initialize state for this key value
                    if sanitized_value not in key_states:
                        # Check if we are exceeding the key threshold before adding a new one
                        if not warned_about_keys and len(key_states) >= MAX_UNIQUE_KEYS_WARN_THRESHOLD:
                            log.warning(f"Processing a large number (> {MAX_UNIQUE_KEYS_WARN_THRESHOLD}) of unique key values ('{key_name}').")
                            log.warning("  This may consume significant memory. Consider pre-filtering data or increasing available memory.")
                            warned_about_keys = True # Show warning only once

                        log.debug(f"Initializing state for new key value: '{sanitized_value}' (original: '{key_value}')")
                        key_states[sanitized_value] = {
                            'handle': None, # Will be opened later
                            'count': 0,
                            'size': base_overhead,
                            'part': 0
                        }
                    state = key_states[sanitized_value]

                    # Estimate item size and prepare serialized string if needed
                    item_size = 0
                    item_str = None # Store serialized string for potential reuse
                    if max_size_bytes:
                        try:
                            # Serialize once
                            item_str = json.dumps(item)
                            item_bytes = item_str.encode('utf-8') # Encode only for size calculation
                            item_size = len(item_bytes)
                        except TypeError as e:
                            log.warning(f"Could not serialize item {item_count_total} (key: {sanitized_value}) to estimate size: {e}. Skipping size check.")
                            item_size = 0
                            item_str = None # Ensure it's None if serialization failed

                    # Check if secondary limits require a new file part *for this key*
                    split_needed = False
                    if state['handle'] is not None: # Don't split if file not even open yet
                        # 1. Max records limit reached for this key's current part
                        if max_records and state['count'] >= max_records:
                            log.debug(f"Max records ({max_records}) reached for key '{sanitized_value}', part {state['part']}.")
                            split_needed = True
                        # 2. Max size limit reached for this key's current part (use pre-calculated size)
                        elif max_size_bytes and (state['size'] + item_size + per_item_overhead) > max_size_bytes:
                            log.debug(f"Max size (~{max_size_bytes / (1024*1024):.2f}MB) reached for key '{sanitized_value}', part {state['part']}.")
                            split_needed = True

                    if split_needed:
                        # Close current file, increment part index, reset state
                        if state['handle'] and not state['handle'].closed:
                            log.debug(f"Closing file for key '{sanitized_value}', part {state['part']}.")
                            state['handle'].close()
                        state['part'] += 1
                        log.debug(f"Starting new part {state['part']} for key '{sanitized_value}'.")
                        state['count'] = 0
                        state['size'] = base_overhead
                        state['handle'] = None # Mark handle as needing reopening

                    # Open file if needed (first time or after a split)
                    if state['handle'] is None:
                        part_suffix = f"_part_{state['part']:04d}" if state['part'] > 0 else ""
                        # Prepare args for key-based filename formatting
                        format_args = {
                            'prefix': output_prefix,
                            'type': 'key',
                            'index': sanitized_value, # Key value itself serves as the index here
                            'part': part_suffix,
                            'ext': file_format_extension
                        }
                        try:
                            output_filename = filename_format.format(**format_args)
                            if not output_filename or '/' in os.path.basename(output_filename) or '\\' in os.path.basename(output_filename):
                                 log.warning(f"Generated filename '{output_filename}' seems invalid. Using default key naming.")
                                 output_filename = f"{output_prefix}_key_{sanitized_value}{part_suffix}.{file_format_extension}"

                        except KeyError as e:
                            log.error(f"Invalid placeholder '{e}' in --filename-format: '{filename_format}'. Using default key naming.")
                            output_filename = f"{output_prefix}_key_{sanitized_value}{part_suffix}.{file_format_extension}"
                        except Exception as e:
                            log.error(f"Error formatting filename for key '{sanitized_value}': {e}. Using default key naming.")
                            output_filename = f"{output_prefix}_key_{sanitized_value}{part_suffix}.{file_format_extension}"


                        log.info(f"  Opening/Appending to file for key value '{key_value}' (Group: '{sanitized_value}'): {output_filename}")
                        try:
                            # Use append mode 'a' - handles both creation and continuing after split
                            state['handle'] = open(output_filename, 'a', encoding='utf-8')
                            state['count'] = 0 # Reset count for new file/part
                            state['size'] = base_overhead # Reset size for new file/part
                        except IOError as e:
                             log.error(f"Failed to open file '{output_filename}' for key '{sanitized_value}': {e}. Skipping item {item_count_total}.")
                             # Don't process this item if file open failed
                             state['handle'] = None # Ensure handle remains None
                             continue # Move to next item

                    # Write the item as a JSON line
                    # Use pre-serialized string if available, otherwise serialize now
                    try:
                        if item_str is not None:
                            state['handle'].write(item_str)
                        else:
                            json.dump(item, state['handle']) # Fallback if size wasn't checked or serialization failed before
                        state['handle'].write('\n')
                        state['count'] += 1
                        state['size'] += item_size + per_item_overhead # Use pre-calculated item_size
                    except IOError as e:
                        log.error(f"Error writing item {item_count_total} for key value '{sanitized_value}' to {state['handle'].name}: {e}. Attempting to continue.")
                        # Mark handle as needing potential reopening/checking later if needed
                        if state['handle']:
                            try:
                                state['handle'].close() # Attempt to close cleanly
                            except IOError:
                                pass # Ignore error on close if write already failed
                            state['handle'] = None # Mark as needing reopen on next write for this key

                except KeyError as e:
                    # This might now be triggered by on_missing_key='error'
                    log.error(f"Processing stopped at item {item_count_total} due to missing key error: {e}")
                    raise # Re-raise the error to stop processing
                except TypeError as e:
                    # Handle error from isinstance check or other type issues
                    msg = f"Error processing item {item_count_total}: {e}"
                    if on_invalid_item == 'error': # Check if error came from initial type check
                         log.error(msg)
                         raise
                    else:
                         log.warning(f"{msg}. Skipping item.")
                # Removed the IOError catch here as it's handled within the write block now


        log.info("\nSplitting complete.")
        # Report final count if not perfectly divisible by interval
        if total_items_processed > last_progress_report_item:
            log.info(f"  Processed {total_items_processed} items total.")
        log.info(f"Total items processed: {total_items_processed}")
        # Summarize files (more complex now with parts)
        log.info("File summary (check output directory for parts based on secondary limits):")
        key_groups = sorted(list(key_states.keys())) # Sort for consistent output
        for value in key_groups:
            base_filename = f"{output_prefix}_key_{value}"
            log.info(f"  - Key Value Group '{value}': Started with {base_filename}*.jsonl")
        if "__missing_key__" in key_states:
             log.info(f"    (Items where key '{key_name}' was missing were grouped into '__missing_key__' files)")

    except FileNotFoundError:
        log.error(f"Input file '{input_file}' not found.")
    except ijson.JSONError as e:
        position_info = getattr(e, 'pos', None)
        line, col = getattr(e, 'lineno', None), getattr(e, 'colno', None)
        pos_str = f" near position {position_info}" if position_info is not None else ""
        line_col_str = f" around line {line}, column {col}" if line is not None and col is not None else ""
        log.error(f"Error parsing JSON{pos_str}{line_col_str}: {e}.\nCheck input file validity and path '{path}'.")
    except (KeyError, TypeError) as e: # Catch errors raised by config options
        log.error(f"Processing stopped due to configuration rule violation: {e}")
    except Exception as e:
        log.exception(f"An unexpected error occurred during key splitting:") # Use log.exception for traceback
    finally:
        # Ensure all files are closed gracefully
        log.info("\nFinalizing file handles...")
        closed_count = 0
        error_count = 0
        # Use sorted keys for consistent closing order in logs
        sorted_keys = sorted(list(key_states.keys()))
        for key in sorted_keys:
            state = key_states[key]
            handle = state.get('handle')
            if handle and not handle.closed:
                 try:
                    log.debug(f"Closing file for key '{key}' ({handle.name}).")
                    handle.close()
                    state['handle'] = None # Clear handle in state
                    closed_count += 1
                 except Exception as e:
                     error_count += 1
                     log.warning(f"Error closing output file for key '{key}': {e}")
        if closed_count > 0:
            log.info(f"Closed {closed_count} output file(s) successfully.")
        if error_count > 0:
            log.warning(f"Encountered errors closing {error_count} file(s). Data might be partially written.")


if __name__ == "__main__":
    main() 