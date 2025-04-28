import argparse
import json
import ijson
import os
import re # Needed for filename sanitization
import logging # Added for logging
import sys # Added to check command-line arguments
from cachetools import LRUCache # Added for key splitting memory management

# --- Logging Setup ---
# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
# Get logger for this module
log = logging.getLogger(__name__)
# ---

def split_by_count(input_file, output_prefix, count, path, output_format, max_records=None, max_size_bytes=None, filename_format="{prefix}_{type}_{index:04d}{part}.{ext}", created_files_set=None):
    """Splits a JSON array based on element count, with optional secondary size/record limits."""
    # Determine the effective splitting mode
    split_by_max_records_only = False
    effective_record_limit = count # Default to primary count

    if max_records is not None:
        log.info(f"--max-records ({max_records}) provided.")
        if max_size_bytes is None:
            # If ONLY max_records is given (or count), it becomes the sole splitting criterion
            log.info(f"Splitting strictly by max_records={max_records} per file.")
            split_by_max_records_only = True
            effective_record_limit = max_records
        else:
            # Both max_records and max_size are present - complex case handled below
            log.info(f"Primary count={count}, secondary max_records={max_records}, secondary max_size set.")
            # Use the smaller of count and max_records for part splitting check if needed?
            # This interaction is complex. Let's stick to the logic where count is primary
            # and max_records/max_size are secondary part limits for now.
            # The logic below handles this.
            pass

    try:
        # Simplified logging based on understanding above
        if split_by_max_records_only:
             log.info(f"Splitting '{input_file}' at path '{path}' strictly by record count={effective_record_limit}...")
        else:
            log.info(f"Splitting '{input_file}' at path '{path}' primarily by count={count}...")
            if max_records: log.info(f"  Secondary limit: Max {max_records} records per file part.")
            if max_size_bytes: log.info(f"  Secondary limit: Max ~{max_size_bytes / (1024*1024):.2f} MB per file part.")

        with open(input_file, 'rb') as f:
            items_iterator = ijson.items(f, path)
            chunk = []
            primary_chunk_index = 0 # Index for the main count-based groups or max_records groups
            items_in_primary_chunk = 0 # Only used when NOT split_by_max_records_only
            part_file_index = 0   # Only used when NOT split_by_max_records_only
            item_count_total = 0
            current_part_size_bytes = 0
            base_overhead = 2 if output_format == 'json' else 0
            per_item_overhead = 4 if output_format == 'json' else 1
            last_progress_report_item = 0
            PROGRESS_REPORT_INTERVAL = 10000

            for item_count_total, item in enumerate(items_iterator, 1):
                # --- Progress Reporting ---
                if item_count_total % PROGRESS_REPORT_INTERVAL == 0:
                    log.info(f"  Processed {item_count_total} items...")
                    last_progress_report_item = item_count_total
                # ---

                # --- Mode 1: Split strictly by max_records --- #
                if split_by_max_records_only:
                    chunk.append(item)
                    if len(chunk) == effective_record_limit:
                        _write_chunk(output_prefix, primary_chunk_index, chunk, output_format, part_index=None, filename_format=filename_format, created_files_set=created_files_set)
                        primary_chunk_index += 1
                        chunk = [] # Reset for next file
                    continue # Skip rest of the loop for this mode

                # --- Mode 2: Split by primary count with secondary limits --- #
                # (Logic from previous successful edit for test_split_count_with_max_size)
                # 1. Estimate size if needed
                item_size = 0
                item_str = None
                if max_size_bytes:
                    try:
                        item_str = json.dumps(item)
                        item_bytes = item_str.encode('utf-8')
                        item_size = len(item_bytes)
                    except TypeError as e:
                        log.warning(f"Could not serialize item {item_count_total} to estimate size: {e}. Skipping size check.")
                        item_size = 0
                        item_str = None

                # 2. Add item to the current chunk/part first
                chunk.append(item)
                items_in_primary_chunk += 1
                current_part_size_bytes += item_size + (per_item_overhead if len(chunk) > 1 else base_overhead)
                if len(chunk) == 1:
                     current_part_size_bytes = base_overhead + item_size # Correct size for first item

                # 3. Determine if a split is needed AFTER adding the item
                part_split_needed = False
                primary_split_needed = False
                write_full_chunk = True # Assume we write the whole chunk unless size limit forces carry-over
                item_to_carry_over = None

                # Check secondary limits first (for the current part file)
                # Use max_records directly here if provided as secondary limit
                if max_records and len(chunk) == max_records:
                    log.debug(f"Part record limit ({max_records}) reached for chunk {primary_chunk_index}, part {part_file_index}.")
                    part_split_needed = True
                elif max_size_bytes and current_part_size_bytes > max_size_bytes and len(chunk) > 1:
                    log.debug(f"Part size limit (~{max_size_bytes / (1024*1024):.2f}MB) reached for chunk {primary_chunk_index}, part {part_file_index}.")
                    part_split_needed = True
                    write_full_chunk = False
                    item_to_carry_over = chunk.pop()
                    items_in_primary_chunk -= 1
                    try:
                        carry_bytes = json.dumps(item_to_carry_over).encode('utf-8')
                        current_part_size_bytes -= (len(carry_bytes) + per_item_overhead)
                    except TypeError:
                         log.warning("Could not re-encode carried over item for size adjustment.")

                # Check primary limit
                if items_in_primary_chunk == count:
                    log.debug(f"Primary count limit ({count}) reached for chunk {primary_chunk_index}.")
                    primary_split_needed = True
                    part_split_needed = False # Primary split takes precedence

                # 4. Perform write and reset if any split needed
                if primary_split_needed or part_split_needed:
                    data_to_write = chunk
                    if data_to_write:
                         _write_chunk(output_prefix, primary_chunk_index, data_to_write, output_format, part_index=part_file_index, filename_format=filename_format, created_files_set=created_files_set)
                    else:
                         log.warning(f"Chunk for primary index {primary_chunk_index} part {part_file_index} was empty after size adjustment. No file written for this part.")

                    chunk = []
                    current_part_size_bytes = base_overhead

                    if item_to_carry_over:
                        log.debug("Adding carried-over item to the new chunk.")
                        chunk.append(item_to_carry_over)
                        items_in_primary_chunk += 1
                        try:
                            item_bytes = json.dumps(item_to_carry_over).encode('utf-8')
                            item_size = len(item_bytes)
                            current_part_size_bytes += item_size
                            if len(chunk) == 1:
                                current_part_size_bytes = base_overhead + item_size
                        except TypeError:
                            log.warning("Could not encode carried over item when adding to new chunk.")

                    # Update indices
                    if primary_split_needed:
                        primary_chunk_index += 1
                        part_file_index = 0
                        items_in_primary_chunk = 0
                        if item_to_carry_over: items_in_primary_chunk = 1
                        log.debug(f"Starting new primary chunk {primary_chunk_index}.")
                    elif part_split_needed:
                        part_file_index += 1
                        log.debug(f"Starting new part {part_file_index} for primary chunk {primary_chunk_index}.")

            # End of loop
            # Write any remaining items in the last chunk
            if chunk:
                # If splitting strictly by max_records, no part index needed
                final_part_index = None if split_by_max_records_only else part_file_index
                _write_chunk(output_prefix, primary_chunk_index, chunk, output_format, part_index=final_part_index, filename_format=filename_format, created_files_set=created_files_set)

            # Report final count if not perfectly divisible by interval
            if item_count_total > last_progress_report_item:
                log.info(f"  Processed {item_count_total} items total.")

            log.info(f"Splitting complete. Total items processed: {item_count_total}. Files created: Check output directory.")

    except FileNotFoundError:
        log.error(f"Error: Input file '{input_file}' not found.")
        return False # Signal failure
    except ijson.JSONError as e:
        # Attempt to extract position info from the error
        position_info = getattr(e, 'pos', None)
        line, col = getattr(e, 'lineno', None), getattr(e, 'colno', None)
        pos_str = f" near position {position_info}" if position_info is not None else ""
        line_col_str = f" around line {line}, column {col}" if line is not None and col is not None else ""
        log.error(f"Error parsing JSON{pos_str}{line_col_str}: {e}.\nCheck input file validity and path '{path}'.")
        return False # Signal failure
    except (IOError, OSError) as e: # Catch file I/O errors specifically
        log.error(f"File system error during count splitting: {e}")
        return False
    except MemoryError:
        log.error("Memory error encountered during count splitting. Input file might be too large or system resources exhausted.")
        return False
    except Exception as e: # Keep a general catch-all but log as exception
        log.exception(f"An unexpected error occurred during count splitting:") # Use log.exception to include traceback
        return False # Signal failure
    return True # Signal success


def split_by_size(input_file, output_prefix, max_size_bytes, path, output_format, max_records=None, filename_format="{prefix}_{type}_{index:04d}{part}.{ext}", created_files_set=None):
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
                    _write_chunk(output_prefix, file_index, chunk, output_format, part_index=part_file_index, filename_format=filename_format, created_files_set=created_files_set)

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
                _write_chunk(output_prefix, file_index, chunk, output_format, part_index=part_file_index, filename_format=filename_format, created_files_set=created_files_set)

            # Report final count if not perfectly divisible by interval
            if item_count_total > last_progress_report_item:
                log.info(f"  Processed {item_count_total} items total.")

            log.info(f"Splitting complete. Total items processed: {item_count_total}. Files created: Check output directory.")

    except FileNotFoundError:
        log.error(f"Error: Input file '{input_file}' not found.")
        return False # Signal failure
    except ijson.JSONError as e:
        position_info = getattr(e, 'pos', None)
        line, col = getattr(e, 'lineno', None), getattr(e, 'colno', None)
        pos_str = f" near position {position_info}" if position_info is not None else ""
        line_col_str = f" around line {line}, column {col}" if line is not None and col is not None else ""
        log.error(f"Error parsing JSON{pos_str}{line_col_str}: {e}.\nCheck input file validity and path '{path}'.")
        return False # Signal failure
    except (IOError, OSError) as e: # Catch file I/O errors specifically
        log.error(f"File system error during size splitting: {e}")
        return False
    except MemoryError:
        log.error("Memory error encountered during size splitting. Input file might be too large or system resources exhausted.")
        return False
    except Exception as e: # Keep a general catch-all but log as exception
        log.exception(f"An unexpected error occurred during size splitting:")
        return False # Signal failure
    return True # Signal success


def execute_split(args):
    """Contains the core logic to perform splitting based on parsed arguments."""
    log.info("Starting JSON splitting process...") # Add start message
    # Track files created/attempted for potential cleanup
    created_files = set()

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
        sys.exit(1) # Exit on error
    if not os.access(args.input_file, os.R_OK):
        log.error(f"Input file is not readable (check permissions): {args.input_file}")
        sys.exit(1) # Exit on error

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
    max_size_str = args.max_size # Keep original string for parsing
    if max_size_str:
        try:
            max_size_bytes = _parse_size(max_size_str)
            if max_size_bytes <= 0:
                 raise ValueError("Max size must be positive.")
        except ValueError as e:
            log.error(f"Invalid --max-size value: {e}. Use formats like 100KB, 50MB, 1GB.")
            sys.exit(1) # Exit on error

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
                 sys.exit(1) # Exit on error
        except OSError as e:
             log.error(f"Failed to create or access output directory '{output_dir}': {e}")
             sys.exit(1) # Exit on error
    elif not os.access(os.getcwd(), os.W_OK):
        # If no output dir specified (writing to cwd), check cwd writability
        log.error(f"Current working directory is not writable (check permissions): {os.getcwd()}")
        sys.exit(1) # Exit on error

    # Enforce jsonl for key splitting for now
    if args.split_by == 'key' and args.output_format == 'json':
            log.warning("Key-based splitting currently enforces JSON Lines ('jsonl') format for efficiency and reduced memory usage. Overriding --output-format to 'jsonl'.")
            args.output_format = 'jsonl'

    # --- Call the appropriate splitting function ---
    try:
        # --- Call the appropriate splitting function ---
        split_function = None
        success = True # Assume success initially

        if args.split_by == 'count':
            try:
                count_val = int(args.value)
                if count_val <= 0:
                    raise ValueError("Count must be a positive integer.")
                split_function = split_by_count
                kwargs = {
                    'count': count_val,
                    'max_records': max_records,
                    'max_size_bytes': max_size_bytes,
                    'filename_format': args.filename_format,
                    'created_files_set': created_files
                }
            except ValueError:
                 log.error(f"Invalid --value for count: '{args.value}'. Must be a positive integer.")
                 sys.exit(1)

        elif args.split_by == 'size':
            try:
                size_bytes = _parse_size(args.value)
                if size_bytes <= 0:
                    raise ValueError("Size must be positive.")
                split_function = split_by_size
                kwargs = {
                    'max_size_bytes': size_bytes,
                    'max_records': max_records,
                    'filename_format': args.filename_format,
                    'created_files_set': created_files
                }
            except ValueError as e:
                log.error(f"Invalid --value for size: {e}. Use formats like 100KB, 50MB, 1GB.")
                sys.exit(1)

        elif args.split_by == 'key':
            key_name = args.value
            if not key_name:
                log.error("--value must provide a non-empty key name for key-based splitting.")
                sys.exit(1)

            key_default_format = "{prefix}_key_{index}{part}.{ext}"
            count_size_default_format = "{prefix}_{type}_{index:04d}{part}.{ext}"
            effective_filename_format = args.filename_format
            if effective_filename_format == count_size_default_format:
                log.debug(f"Using default filename format for key splitting: '{key_default_format}'")
                effective_filename_format = key_default_format

            split_function = split_by_key
            kwargs = {
                'key_name': key_name,
                'max_records': max_records,
                'max_size_bytes': max_size_bytes,
                'on_missing_key': args.on_missing_key,
                'on_invalid_item': args.on_invalid_item,
                'filename_format': effective_filename_format,
                'created_files_set': created_files
            }

        else:
            # This case should not be reachable due to choices constraint
            log.error(f"Internal error: Splitting by '{args.split_by}' is not implemented.")
            sys.exit(1)

        # Execute the chosen split function
        result = split_function(
            input_file=args.input_file,
            output_prefix=args.output_prefix,
            path=args.path,
            output_format=args.output_format,
            **kwargs
        )
        # Check result: Split functions should return False or raise Exception on failure/policy stop
        if result is False:
             log.error("Splitting process terminated early due to errors or configuration policy (e.g., on-missing-key=error).")
             success = False

    except (ijson.JSONError, FileNotFoundError, IOError, OSError, PermissionError) as e: # More specific I/O and JSON errors
        log.error(f"Splitting failed due to input/output or JSON parsing error: {e}")
        success = False
    except ValueError as e: # Catch specific value errors (e.g., from parsing args, though some handled earlier)
        log.error(f"Splitting failed due to invalid value: {e}")
        success = False
    except MemoryError:
         log.error("Splitting failed due to insufficient memory.")
         success = False
    except Exception as e: # Catch truly unexpected errors
        log.exception(f"An unexpected error occurred during splitting execution: {e}")
        success = False

    # --- Cleanup on Failure ---
    if not success:
        log.warning("Splitting process failed. Attempting to clean up potentially partial files...")
        cleaned_count = 0
        for filename in created_files:
            try:
                if os.path.exists(filename): # Check if file actually exists before removing
                    os.remove(filename)
                    log.debug(f"  Removed partial file: {filename}")
                    cleaned_count += 1
            except OSError as rm_err:
                log.warning(f"  Could not remove partial file '{filename}': {rm_err}")
            except Exception as E:
                log.warning(f"  Unexpected error removing partial file '{filename}': {E}")
        if cleaned_count > 0:
            log.warning(f"Cleaned up {cleaned_count} potentially partial file(s).")
        else:
            log.warning("No partial files found to clean up or cleanup failed.")
        # --- End Cleanup ---

    if success:
        log.info("Splitting process completed successfully.")
    else:
        log.error("Splitting process failed or was terminated early.")
        sys.exit(1) # Ensure exit code is non-zero on failure

# --- Helper Functions for Interactive Mode ---

def _prompt_with_validation(prompt_text, required=True, validation_func=None, choices=None, default=None):
    """Generic function to prompt user with validation and choices."""
    while True:
        prompt_suffix = f" [{default}]" if default is not None else ""
        try:
            user_input = input(f"{prompt_text}{prompt_suffix}: ").strip()
            if not user_input:
                if default is not None:
                    return default # Return default if user just hits Enter
                elif required:
                    print("  Error: Input is required.")
                    continue
                else:
                    return None # Allow empty input if not required and no default

            if choices:
                if user_input.lower() not in [c.lower() for c in choices]:
                    print(f"  Error: Invalid choice. Please choose from: {', '.join(choices)}")
                    continue
                # Return the matching choice (maintaining original case if needed, though lower() is often fine)
                user_input = next(c for c in choices if c.lower() == user_input.lower())

            if validation_func:
                is_valid, error_msg_or_value = validation_func(user_input)
                if not is_valid:
                    print(f"  Error: {error_msg_or_value}")
                    continue
                # Validation function might return the processed value (e.g., parsed size)
                if error_msg_or_value is not None and error_msg_or_value != True:
                    return error_msg_or_value

            return user_input # Return the validated input
        except EOFError: # Handle Ctrl+D
            print("\nOperation cancelled.")
            sys.exit(0)

def _validate_input_file(filepath):
    if not filepath:
         return False, "Input file path cannot be empty."
    if not os.path.isfile(filepath):
        return False, f"File not found at '{filepath}'."
    if not os.access(filepath, os.R_OK):
        return False, f"File is not readable (check permissions): '{filepath}'."
    return True, None

def _validate_output_prefix(prefix):
     if not prefix:
          return False, "Output prefix cannot be empty."
     # Basic check for obviously invalid chars often disallowed, though OS varies
     # This isn't foolproof but catches common mistakes.
     invalid_chars = ':*?"<>|'
     if any(c in invalid_chars for c in prefix):
         return False, f"Output prefix contains potentially invalid characters from the set: {invalid_chars}"
     return True, None

def _validate_path(path_str):
     if not path_str:
          return False, "JSON path cannot be empty."
     # Basic check - doesn't validate ijson path syntax fully but ensures non-empty
     return True, None

def _validate_split_value(value_str, split_by):
    if not value_str:
        return False, "Split value cannot be empty."
    if split_by == 'count':
        try:
            count = int(value_str)
            if count <= 0:
                return False, "Count must be a positive integer."
            return True, count # Return the integer value
        except ValueError:
            return False, "Value must be a valid positive integer for split-by 'count'."
    elif split_by == 'size':
        try:
            size_bytes = _parse_size(value_str)
            if size_bytes <= 0:
                return False, "Size must be positive."
            return True, value_str # Return the original string for size
        except ValueError as e:
            return False, f"Invalid size format: {e}. Use numbers optionally followed by KB, MB, GB."
    elif split_by == 'key':
        # Key name just needs to be non-empty
        return True, value_str
    else:
         # Should not happen if split_by is validated first
         return False, "Invalid split_by type provided for value validation."

def _validate_optional_int(value_str):
    if not value_str: # Empty is OK, means None
        return True, None
    try:
        num = int(value_str)
        if num <= 0:
             return False, "Value must be a positive integer if provided."
        return True, num
    except ValueError:
         return False, "Value must be a valid positive integer."

def _validate_optional_size(value_str):
     if not value_str:
        return True, None
     try:
        size_bytes = _parse_size(value_str)
        if size_bytes <= 0:
            return False, "Size must be positive if provided."
        return True, value_str # Return the original string
     except ValueError as e:
        return False, f"Invalid size format: {e}. Use numbers optionally followed by KB, MB, GB."

# --- End Helper Functions ---

def run_interactive_mode():
    """Prompts the user for arguments interactively in a user-friendly way."""
    log.info("✨ Welcome to JSON Splitter Interactive Mode! ✨")
    log.info("Let's configure the splitting process step-by-step.")
    args = argparse.Namespace()

    # Set defaults first (mirroring argparse defaults)
    args.output_format = 'json'
    args.max_records = None
    args.max_size = None
    args.on_missing_key = 'group'
    args.on_invalid_item = 'warn'
    args.verbose = False
    args.filename_format = None

    try:
        print("\n--- 📝 Required Settings ---")
        # --- Required Arguments ---
        args.input_file = _prompt_with_validation(
            "📄 Enter path to the input JSON file",
            validation_func=_validate_input_file
        )
        args.output_prefix = _prompt_with_validation(
            "📂 Enter the output file prefix (e.g., output/chunk or results/data)",
            validation_func=_validate_output_prefix
        )
        args.split_by = _prompt_with_validation(
            "✂️ Split by which criterion? (Enter 'count', 'size', or 'key')",
            choices=['count', 'size', 'key']
        )

        # Provide specific examples based on the chosen split type
        value_prompt = f"🔢 Enter value for '{args.split_by}' split"
        if args.split_by == 'count':
            value_prompt += " (e.g., 10000 for 10k items per file)"
        elif args.split_by == 'size':
            value_prompt += " (e.g., 15MB, 500KB, 1GB)"
        elif args.split_by == 'key':
            value_prompt += " (e.g., category_id or user_id)"
        args.value = _prompt_with_validation(value_prompt, validation_func=lambda v: _validate_split_value(v, args.split_by))

        args.path = _prompt_with_validation(
            "🎯 Enter JSON path to the items to split (e.g., `item` for root array, `data.records.item` for nested)",
            validation_func=_validate_path
        )

        # --- Optional Arguments --- #
        print("\n--- 🤔 Optional Settings --- (Press Enter to use defaults)")
        set_optionals = _prompt_with_validation("Configure optional settings? (y/N)", required=False, choices=['y', 'n'], default='n')

        if set_optionals.lower() == 'y':
            log.info("\n🔧 Configuring optional settings...")
            args.output_format = _prompt_with_validation(
                "📦 Output format?",
                choices=['json', 'jsonl'],
                default=args.output_format,
                required=False
            )
            args.max_records = _prompt_with_validation(
                "📏 Max records per file part (secondary limit, e.g., 50000)?",
                default="None",
                validation_func=_validate_optional_int,
                required=False
            )
            args.max_size = _prompt_with_validation(
                "💾 Max size per file part (secondary limit, e.g., 50MB)?",
                default="None",
                validation_func=_validate_optional_size,
                required=False
            )

            if args.split_by == 'key':
                 log.info("\n🔑 Key Split Specific Options:")
                 args.on_missing_key = _prompt_with_validation(
                     "❓ Action for items missing the key?",
                     choices=['group', 'skip', 'error'],
                     default=args.on_missing_key,
                     required=False
                 )
                 args.on_invalid_item = _prompt_with_validation(
                     "⚠️ Action for items at path that are not objects?",
                     choices=['warn', 'skip', 'error'],
                     default=args.on_invalid_item,
                     required=False
                 )

            # Set appropriate default filename format based on split type before prompting
            default_ff = "{prefix}_key_{index}{part}.{ext}" if args.split_by == 'key' else "{prefix}_{type}_{index:04d}{part}.{ext}"
            ff_prompt = f"🏷️ Output filename format string? (Placeholders: {{prefix}}, {{type}}, {{index}}, {{part}}, {{ext}} )"
            args.filename_format = _prompt_with_validation(ff_prompt, default=default_ff, required=False)

            verbose_resp = _prompt_with_validation("🐞 Enable verbose logging? (y/N)", choices=['y', 'n'], default='n', required=False)
            args.verbose = (verbose_resp.lower() == 'y')

        # Ensure filename_format has a value (use default if not set in optionals)
        if args.filename_format is None:
             args.filename_format = "{prefix}_key_{index}{part}.{ext}" if args.split_by == 'key' else "{prefix}_{type}_{index:04d}{part}.{ext}"

        log.info("\n✅ Configuration complete. Proceeding with splitting...")
        return args

    except KeyboardInterrupt:
        print("\nOperation cancelled by user during setup.")
        sys.exit(0)
    except EOFError:
        print("\nOperation cancelled.")
        sys.exit(0)


def main():
    # Check if any command-line arguments were passed (sys.argv[0] is the script name)
    if len(sys.argv) > 1:
        # --- Standard CLI Argument Parsing ---
        parser = argparse.ArgumentParser(description="Split large JSON files using streaming.")
        parser.add_argument("input_file", help="Path to the input JSON file.")
        parser.add_argument("output_prefix", help="Prefix for the output files (e.g., 'output/chunk').")
        parser.add_argument("--split-by", required=True, choices=['count', 'size', 'key'], help="Criterion to split by ('count', 'size', or 'key').")
        parser.add_argument("--value", required=True, type=str, help="Value for the splitting criterion (e.g., number of items for 'count', size like '100MB' for 'size', the key name for 'key').")
        parser.add_argument("--path", required=True, help="JSON path to the array/objects to split (e.g., 'item' for root array, 'data.records.item'). For 'key' splitting, this should point to the objects containing the key.")
        parser.add_argument("--output-format", choices=['json', 'jsonl'], default='json', help="Output format ('json' or 'jsonl'). Default: json. Note: 'key' splitting currently forces 'jsonl'.")
        parser.add_argument("--max-records", type=int, default=None, help="Secondary constraint: Maximum records per output file.")
        parser.add_argument("--max-size", type=str, default=None, help="Secondary constraint: Maximum approximate size per output file (e.g., '50MB').")
        parser.add_argument("--on-missing-key", choices=['group', 'skip', 'error'], default='group', help="Action for items missing the specified key when splitting by key (default: group). 'group' puts them in '__missing_key__' file.")
        parser.add_argument("--on-invalid-item", choices=['warn', 'skip', 'error'], default='warn', help="Action for items at target path that are not objects when splitting by key (default: warn). 'warn' prints a message and skips.")
        parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose debug logging.")
        parser.add_argument("--filename-format", type=str,
                            default="{prefix}_{type}_{index:04d}{part}.{ext}",
                            help="Format string for output filenames. Available placeholders: "
                                 "{prefix}, {type} ('chunk' or 'key'), {index} (primary index/key value), "
                                 "{part} (_part_XXXX or empty), {ext} (json/jsonl). Example: chunk_{index}.{ext}")

        args = parser.parse_args()
        execute_split(args)
    else:
        # --- Interactive Mode --- #
        try:
            args = run_interactive_mode()
            if args:
                execute_split(args)
            else:
                log.info("Interactive setup cancelled or not yet implemented.")
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            sys.exit(0)



# Helper function to parse size strings (e.g., "100MB")
def _parse_size(size_str):
    """Parses a size string (e.g., 100KB, 5MB, 1GB, 150B, 2048) into bytes."""
    size_str_orig = size_str # Keep original for error messages
    size_str = size_str.strip().upper()
    if not size_str:
        raise ValueError("Size string cannot be empty.")

    multiplier = 1
    suffix = None
    if size_str.endswith('KB'):
        multiplier = 1024
        suffix = 'KB'
    elif size_str.endswith('MB'):
        multiplier = 1024 * 1024
        suffix = 'MB'
    elif size_str.endswith('GB'):
        multiplier = 1024 * 1024 * 1024
        suffix = 'GB'
    elif size_str.endswith('B'): # Handle explicit bytes suffix
        multiplier = 1
        suffix = 'B'
    # No else here, check if the remaining part is numeric after potentially stripping suffix

    numeric_part = size_str
    if suffix:
        numeric_part = size_str[:-len(suffix)].strip()

    if not numeric_part:
         raise ValueError(f"Missing numeric value before suffix in '{size_str_orig}'.")

    try:
        # Use float first to allow for decimal values (e.g., 1.5MB)
        # then convert to int after multiplying
        value = float(numeric_part)
        if value < 0:
             raise ValueError("Size value cannot be negative.")
        return int(value * multiplier)
    except ValueError:
         # Raise specific error if conversion fails
         raise ValueError(f"Invalid numeric value '{numeric_part}' in size string '{size_str_orig}'.")

# Helper function to write chunks consistently
def _write_chunk(output_prefix, primary_index, chunk_data, output_format, part_index=None,
                   filename_format="{prefix}_{type}_{index:04d}{part}.{ext}", created_files_set=None):
    """Writes a chunk of data to a uniquely named file using a format string.

    Args:
        output_prefix (str): The prefix for the output file.
        primary_index (int): The primary index for the file.
        chunk_data (list): The data to write to the file.
        output_format (str): The format of the output file ('json' or 'jsonl').
        part_index (int, optional): The part index for the file.
        filename_format (str, optional): The format string for the output filename.
        created_files_set (set, optional): A set to track filenames attempted for creation.
    """

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
    except ValueError as e: # Catch potential errors from format specifiers like :04d
        log.error(f"Invalid format specifier in --filename-format '{filename_format}': {e}. Using default naming.")
        output_filename = f"{output_prefix}_chunk_{primary_index:04d}{part_suffix}.{extension}"
    except Exception as e: # Keep general catch-all for other formatting issues
        log.error(f"Error formatting filename with format '{filename_format}': {e}. Using default naming.")
        # Fallback to old naming
        output_filename = f"{output_prefix}_chunk_{primary_index:04d}{part_suffix}.{extension}"

    # --- Track file before attempting to write --- #
    if created_files_set is not None:
        created_files_set.add(output_filename)
    # --- End Tracking --- #

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
                json.dump(chunk_data, outfile, indent=None) # Changed indent to None
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
    log.debug(f"Sanitizing value: '{s_value}'")
    # Remove leading/trailing whitespace FIRST
    s_value = s_value.strip()
    # Replace sequences of spaces and problematic characters with a SINGLE underscore
    # Problematic chars: \ / : * ? " < > |
    s_value = re.sub(r'[\\s\\/:*?"<>|]+', '_', s_value)
    # Remove any leading/trailing underscores that might result from replacement
    s_value = s_value.strip('_')
    # Ensure filename is not empty after sanitization
    if not s_value:
        s_value = "__empty__"
        log.debug("Sanitized value was empty, using '__empty__'")

    # Limit length to avoid issues on some filesystems (e.g., 255 bytes common limit)
    # Encode to check byte length, common limit is often byte-based
    try:
        encoded_value = s_value.encode('utf-8')
        max_len_bytes = 100 # Keep reasonably short for readability, adjust if needed
        if len(encoded_value) > max_len_bytes:
            log.debug(f"Original sanitized value '{s_value}' ({len(encoded_value)} bytes) exceeds limit {max_len_bytes}. Truncating...")
            # Find cut-off point respecting UTF-8 character boundaries
            # Iterate backwards from max_len_bytes-1 until we find the start of a valid char
            byte_index = max_len_bytes - 1
            while byte_index >= 0 and (encoded_value[byte_index] & 0xC0) == 0x80: # Check if it's a continuation byte (10xxxxxx)
                 byte_index -= 1
            # Now byte_index is at the start of the last full character within the limit, or -1
            if byte_index < 0:
                s_value = "" # Cannot truncate safely
            else:
                 # Slice up to and including the start byte of the last valid character
                 s_value = encoded_value[:byte_index+1].decode('utf-8', 'ignore')
            log.debug(f"Sanitized filename truncated to: '{s_value}' ({len(s_value.encode('utf-8'))} bytes)")
    except UnicodeEncodeError as e:
        log.warning(f"Could not encode sanitized filename '{s_value}' to UTF-8 for length check: {e}")
        s_value = s_value[:100] # Fallback to simple char length limit
    except UnicodeDecodeError as e:
        log.warning(f"Could not decode truncated bytes back to UTF-8 for sanitized filename: {e}")
        s_value = s_value[:100] # Fallback
    except Exception as e: # General catch-all for unforeseen issues during truncation
         log.warning(f"Could not properly truncate sanitized filename '{s_value}': {e}")
         s_value = s_value[:100] # Fallback to simple char length limit

    log.debug(f"Final sanitized filename part: '{s_value}'")
    return s_value

# --- Constants ---
MAX_OPEN_FILES_KEY_SPLIT = 1000 # Max files to keep open during key splitting

def split_by_key(input_file, output_prefix, key_name, path, output_format,
                   max_records=None, max_size_bytes=None,
                   on_missing_key='group', on_invalid_item='warn',
                   filename_format="{prefix}_key_{index}{part}.{ext}", # Default format specific to key
                   created_files_set=None):
    """Splits JSON objects based on the value of a specified key using streaming.

    Uses an LRU cache to limit the number of simultaneously open file handles,
    preventing excessive memory usage for high-cardinality keys.

    Args:
        input_file (str): The path to the input JSON file.
        output_prefix (str): The prefix for the output files.
        key_name (str): The name of the key to split by.
        path (str): The JSON path to the items to split.
        output_format (str): The format of the output files ('json' or 'jsonl').
        max_records (int, optional): The maximum number of records per output file.
        max_size_bytes (int, optional): The maximum approximate size per output file in bytes.
        on_missing_key (str, optional): The action to take for items missing the specified key.
        on_invalid_item (str, optional): The action to take for items at target path that are not objects.
        filename_format (str, optional): The format string for the output filenames.
        created_files_set (set, optional): A set to track filenames attempted for creation.
    """
    # Cache to hold open file handles and state per key value encountered
    # Key: sanitized_key_value
    # Value: { 'handle': file_handle, 'count': current_record_count, 'size': current_estimated_size, 'part': current_part_index }
    open_files_cache = LRUCache(maxsize=MAX_OPEN_FILES_KEY_SPLIT)
    written_keys = set() # Track keys for which a file has been created/opened at least once

    total_items_processed = 0
    success = True # Overall success flag for the function
    # Removed memory warning threshold as LRU cache addresses the core issue

    file_format_extension = 'jsonl' # Hardcoded for key splitting (simpler append)
    # Overhead estimation for JSONL
    base_overhead = 0
    per_item_overhead = 1 # Just newline
    last_progress_report_item = 0
    PROGRESS_REPORT_INTERVAL = 10000 # Report progress every N items

    try:
        log.info(f"Splitting '{input_file}' at path '{path}' by key '{key_name}' (format: {file_format_extension})...")
        log.info(f"  Using LRU cache for file handles (max open: {MAX_OPEN_FILES_KEY_SPLIT}).")
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
                # Update total items processed reliably here
                total_items_processed = item_count_total
                # ---

                try:
                    # 1. Handle non-dict items
                    if not isinstance(item, dict):
                        msg = f"Item {item_count_total} at path '{path}' is not an object/dict (type: {type(item)})."
                        if on_invalid_item == 'error':
                            log.error(msg)
                            log.critical("Exiting due to invalid item type with 'error' policy.")
                            success = False # Set failure flag
                            break # Exit loop immediately
                        elif on_invalid_item == 'skip':
                            log.debug(f"Skipping: {msg}")
                            continue # Try next item
                        else: # Default: warn
                            log.warning(f"{msg} Skipping key check for this item.")
                            continue # Try next item

                    # 2. Determine key value and handle missing/complex keys
                    key_value = item.get(key_name)
                    sanitized_value = None # Reset for each item
                    should_skip_item = False

                    if key_value is None:
                        if on_missing_key == 'error':
                            msg = f"Key '{key_name}' not found in item {item_count_total}."
                            log.error(msg)
                            log.critical("Exiting due to missing key with 'error' policy.")
                            success = False # Set failure flag
                            break # Exit loop immediately
                        elif on_missing_key == 'skip':
                            log.debug(f"Skipping item {item_count_total}: Key '{key_name}' not found.")
                            should_skip_item = True
                        else: # Default: group
                            sanitized_value = "__missing_key__"
                            log.debug(f"Item {item_count_total}: Key '{key_name}' missing, grouping as '{sanitized_value}'.")
                    elif isinstance(key_value, (dict, list)):
                        complex_type_name = type(key_value).__name__
                        sanitized_value = f"__complex_type_{_sanitize_filename(complex_type_name)}__"
                        log.warning(f"Key '{key_name}' in item {item_count_total} has complex type ({complex_type_name}). Grouping into '{sanitized_value}'.")
                    else:
                        sanitized_value = _sanitize_filename(key_value)
                        log.debug(f"Item {item_count_total}: Key '{key_name}' value '{key_value}' sanitized to '{sanitized_value}'.")

                    if should_skip_item:
                        continue # Move to next item if skip policy applied

                    # Ensure we have a sanitized value (should always be true unless skipped)
                    if sanitized_value is None:
                        log.error(f"Internal error: Sanitized value is None for item {item_count_total}. Skipping.")
                        continue # Try next item

                    # --- LRU Cache Logic ---
                    # 3. Get or Initialize State for this Key from Cache
                    state = None
                    if sanitized_value in open_files_cache:
                        # Cache hit: File is open
                        state = open_files_cache[sanitized_value] # Access updates LRU order
                        log.debug(f"Cache hit for key '{sanitized_value}'.")
                    else:
                        # Cache miss: File is not open (or never seen)
                        log.debug(f"Cache miss for key '{sanitized_value}'.")
                        # Evict if cache is full *before* creating new state/opening file
                        if len(open_files_cache) >= MAX_OPEN_FILES_KEY_SPLIT:
                            evicted_key, evicted_state = open_files_cache.popitem() # Removes LRU item
                            log.debug(f"Cache full. Evicting state for key '{evicted_key}'.")
                            try:
                                if evicted_state.get('handle') and not evicted_state['handle'].closed:
                                    log.debug(f"Closing evicted file handle for key '{evicted_key}', part {evicted_state.get('part', '?')}.")
                                    evicted_state['handle'].close()
                            except IOError as e:
                                log.warning(f"Error closing evicted file handle for key '{evicted_key}': {e}")

                        # Initialize state for the new key (will be added to cache after file open)
                        initial_part = 0
                        mode = 'w' # Default to write for a truly new key
                        if sanitized_value in written_keys:
                            mode = 'a'
                            log.debug(f"Key '{sanitized_value}' seen before (in written_keys), will use append mode.")
                        else:
                            log.debug(f"Key '{sanitized_value}' is new, will use write mode.")
                            written_keys.add(sanitized_value)

                        state = {'handle': None, 'count': 0, 'size': base_overhead, 'part': initial_part, 'mode': mode}

                    # 4. Estimate item size AND serialize (only once)
                    item_size = 0
                    item_str = None
                    try:
                        item_str = json.dumps(item)
                        if max_size_bytes:
                             item_bytes = item_str.encode('utf-8')
                             item_size = len(item_bytes)
                    except TypeError as e:
                        log.warning(f"Could not serialize item {item_count_total} (key: {sanitized_value}): {e}. Skipping size/write.")
                        item_str = None
                        item_size = 0

                    if item_str is None:
                        continue # Try next item

                    # 5. Tentatively update state (simulate adding item)
                    potential_new_count = state['count'] + 1
                    potential_new_size = state['size'] + item_size + per_item_overhead

                    # 6. Check if split is needed BEFORE adding
                    needs_new_part = False
                    split_reason = ""
                    current_handle = state.get('handle')
                    if state['count'] > 0:
                         if max_records and potential_new_count > max_records:
                             needs_new_part = True
                             split_reason = f"record limit ({max_records})"
                         elif max_size_bytes and potential_new_size > max_size_bytes:
                             needs_new_part = True
                             split_reason = f"size limit (~{max_size_bytes / (1024*1024):.2f}MB)"

                    # 7. If split needed, finalize previous part and reset state for the new part
                    if needs_new_part:
                        log.debug(f"Split needed for key '{sanitized_value}' part {state['part']} due to {split_reason}. Closing file.")
                        try:
                            if current_handle and not current_handle.closed:
                                current_handle.close()
                        except IOError as e:
                            log.warning(f"Error closing file for key '{sanitized_value}', part {state['part']}: {e}")

                        state['part'] += 1
                        state['count'] = 0
                        state['size'] = base_overhead
                        state['handle'] = None
                        state['mode'] = 'a'
                        log.debug(f"Starting new part {state['part']} for key '{sanitized_value}'.")

                    # 8. Open file if handle is None (first time or after closing/eviction)
                    if state.get('handle') is None or state['handle'].closed:
                        part_suffix = f"_part_{state['part']:04d}" if state['part'] > 0 else ""
                        format_args = {
                            'prefix': output_prefix, 'type': 'key', 'index': sanitized_value,
                            'part': part_suffix, 'ext': file_format_extension
                        }
                        try:
                            output_filename = filename_format.format(**format_args)
                            if not output_filename or '/' in os.path.basename(output_filename) or '\\' in os.path.basename(output_filename):
                                log.warning(f"Generated filename '{output_filename}' seems invalid. Using default.")
                                output_filename = f"{output_prefix}_key_{sanitized_value}{part_suffix}.{file_format_extension}"
                        except KeyError as e:
                            log.error(f"Invalid placeholder '{e}' in filename format. Using default.")
                            output_filename = f"{output_prefix}_key_{sanitized_value}{part_suffix}.{file_format_extension}"
                        except Exception as e:
                            log.error(f"Error formatting filename: {e}. Using default.")
                            output_filename = f"{output_prefix}_key_{sanitized_value}{part_suffix}.{file_format_extension}"

                        # --- Track file before attempting to open --- #
                        if created_files_set is not None:
                            created_files_set.add(output_filename)
                        # --- End Tracking --- #

                        open_mode = state.get('mode', 'a')
                        log.info(f"  Opening file ({open_mode}): {output_filename}")
                        try:
                            output_dir = os.path.dirname(output_filename)
                            if output_dir:
                                os.makedirs(output_dir, exist_ok=True)
                            new_handle = open(output_filename, open_mode, encoding='utf-8')
                            state['handle'] = new_handle
                            open_files_cache[sanitized_value] = state
                            if sanitized_value not in written_keys: written_keys.add(sanitized_value)
                        except IOError as e:
                            log.error(f"Failed to open file '{output_filename}' for key '{sanitized_value}': {e}. Skipping item.")
                            state['handle'] = None
                            if sanitized_value in open_files_cache: del open_files_cache[sanitized_value]
                            continue # Try next item

                    # 9. Write the item string to the (now hopefully open) file handle
                    current_handle = state.get('handle')
                    if current_handle and not current_handle.closed:
                        try:
                            current_handle.write(item_str + '\n')
                            # 10. Update state AFTER successful write
                            if needs_new_part: # If we just started a new part for this item
                                state['count'] = 1
                                state['size'] = base_overhead + item_size
                            else: # Otherwise, update normally using pre-calculated potential values
                                state['count'] = potential_new_count
                                state['size'] = potential_new_size
                        except IOError as e:
                            log.error(f"Failed to write to file for key '{sanitized_value}': {e}. Skipping item.")
                            try:
                                if current_handle: current_handle.close()
                            except IOError: pass
                            state['handle'] = None
                            if sanitized_value in open_files_cache: del open_files_cache[sanitized_value]
                            continue # Try next item
                    else:
                        log.error(f"Internal Error: Attempted to write for key '{sanitized_value}', but file handle was None or closed. Skipping item.")
                        continue # Try next item

                except (TypeError, ValueError) as e: # Catch errors during item processing/key extraction/sanitization
                    log.error(f"Error processing item {item_count_total} (key value attempted: '{key_value if 'key_value' in locals() else '[unknown]'}'): {e}. Skipping item.")
                    continue
                except MemoryError:
                    log.error(f"Memory error processing item {item_count_total}. Attempting to continue, but results may be incomplete.")
                    # Depending on policy, could set success = False and break here.
                    continue
                except Exception as e:
                    # Catch unexpected errors during the processing of a single item
                    log.exception(f"Unexpected error processing item {item_count_total} (key value attempted: '{key_value if 'key_value' in locals() else '[unknown]'}'). Skipping item.")
                    # Optionally: Decide if this should halt the entire process
                    # success = False
                    continue # Default: log and continue with the next item

            # End of loop

            # Report final count if not perfectly divisible by interval
            if total_items_processed > last_progress_report_item:
                 log.info(f"  Processed {total_items_processed} items total.")

            log.info("Finished processing input.")

    except FileNotFoundError:
        log.error(f"Error: Input file '{input_file}' not found.")
        success = False # Signal failure
    except ijson.JSONError as e:
        position_info = getattr(e, 'pos', None)
        line, col = getattr(e, 'lineno', None), getattr(e, 'colno', None)
        pos_str = f" near position {position_info}" if position_info is not None else ""
        line_col_str = f" around line {line}, column {col}" if line is not None and col is not None else ""
        log.error(f"Error parsing JSON{pos_str}{line_col_str}: {e}.\nCheck input file validity and path '{path}'.")
        success = False # Signal failure
    except (IOError, OSError) as e: # Catch file I/O errors specifically
        log.error(f"File system error during key splitting: {e}")
        success = False
    except MemoryError:
        log.error("Memory error encountered during key splitting setup or main loop. Input file might be too large or system resources exhausted.")
        success = False
    except Exception as e: # Keep a general catch-all but log as exception
        log.exception("An unexpected error occurred during key splitting:") # Use log.exception
        success = False # Signal failure
    finally:
        # --- Cleanup: Close all files remaining in the cache ---
        log.info("Closing remaining open files...")
        closed_count = 0
        for key, state in open_files_cache.items():
            try:
                handle = state.get('handle')
                if handle and not handle.closed:
                    log.debug(f"Closing file for key '{key}' part {state.get('part','?')}")
                    handle.close()
                    closed_count += 1
            except IOError as e:
                log.warning(f"Error closing file for key '{key}' during final cleanup: {e}")
            except Exception as e: # Catch any other error during close
                 log.warning(f"Unexpected error closing file for key '{key}': {e}")
        open_files_cache.clear() # Clear cache after closing handles
        log.info(f"Closed {closed_count} files during cleanup.")

    if success:
        log.info(f"Splitting complete. Total items processed: {total_items_processed}. Output files generated.")
    else:
        log.error(f"Splitting failed or partially failed. Total items processed before error: {total_items_processed}.")

    return success # Signal overall success/failure


if __name__ == "__main__":
    main() 