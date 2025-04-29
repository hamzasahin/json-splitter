import json
import ijson
import os
import logging
from cachetools import LRUCache

from .utils import log, parse_size, sanitize_filename, PROGRESS_REPORT_INTERVAL, ProgressTracker

MAX_OPEN_FILES_KEY_SPLIT = 1000 # Max files to keep open during key splitting

class SplitterBase:
    """Base class for all splitting strategies."""

    # PROGRESS_INTERVAL = PROGRESS_REPORT_INTERVAL # Commented out/removed old constant use

    def __init__(self, input_file, output_dir, base_name, path, output_format,
                 max_records=None, max_size=None, # Use max_size string here
                 filename_format=None, verbose=False,
                 created_files_set=None,
                 report_interval: int = 10000, # Added report_interval parameter
                 **kwargs): # Accept extra args
        self.input_file = input_file
        # self.output_prefix = output_prefix # Removed
        self.output_dir = output_dir
        self.base_name = base_name
        self.path = path if path else '' # Ensure path is not None
        self.output_format = output_format
        self.max_records = max_records
        self.max_size_str = max_size
        self.max_size_bytes = None
        if self.max_size_str:
            try:
                self.max_size_bytes = parse_size(self.max_size_str)
                if self.max_size_bytes <= 0:
                     raise ValueError("Max size must be positive.")
            except ValueError as e:
                log.error(f"Invalid --max-size value: {e}. Use formats like 100KB, 50MB, 1GB.")
                raise # Re-raise to be caught by the caller

        self.filename_format = filename_format
        self.verbose = verbose
        self.created_files_set = created_files_set if created_files_set is not None else set()
        self.log = log # Use the logger from utils
        self._report_interval = report_interval # Store report_interval

        # Set logging level based on verbose flag
        if self.verbose:
            self.log.setLevel(logging.DEBUG)
        else:
            self.log.setLevel(logging.INFO)

    def split(self):
        """Template method for splitting. Must be implemented by subclasses."""
        raise NotImplementedError()

    def _progress_report(self, item_count_total, last_report):
        """Common progress reporting. [DEPRECATED - Use ProgressTracker]"""
        # This method is now deprecated in favor of the ProgressTracker class
        # Keeping it briefly for reference during transition, can be removed later.
        report_interval = getattr(self, '_report_interval', 10000) # Get interval if available
        if report_interval > 0 and item_count_total % report_interval == 0:
            self.log.info(f"  [Legacy Report] Processed {item_count_total} items...")
            return item_count_total
        return last_report

    def _write_chunk(self, primary_index, chunk_data, part_index=None, split_type='chunk', key_value=None):
        """Writes a chunk of data to a uniquely named file using the filename format.

        Args:
            primary_index (int or str): The primary index (chunk number or sanitized key).
            chunk_data (list): The data to write.
            part_index (int, optional): The part index for secondary splits.
            split_type (str): 'chunk' for count/size, 'key' for key split.
            key_value (str, optional): The sanitized key value (used for 'key' split index).
        """
        if not chunk_data:
            self.log.warning(f"Attempted to write empty chunk for index {primary_index}, part {part_index}. Skipping.")
            return None # Indicate no file was written

        extension = 'jsonl' if self.output_format == 'jsonl' else 'json'
        part_suffix = f"_part_{part_index:04d}" if part_index is not None and part_index > 0 else ""

        # Use key_value for index if split_type is 'key', otherwise use primary_index (number)
        index_val = key_value if split_type == 'key' else primary_index

        format_args = {
            'base_name': self.base_name,
            'type': split_type,
            'index': index_val,
            'part': part_suffix,
            'ext': extension
        }

        # Determine the correct filename format string
        current_format = self.filename_format
        if not current_format: # Use default if None
             current_format = "{base_name}_key_{index}{part}.{ext}" if split_type == 'key' else "{base_name}_{type}_{index:04d}{part}.{ext}"
        # Handle potential mismatch if user didn't provide format and split_type is key
        elif split_type == 'key' and '{index:04d}' in current_format:
            self.log.debug("Defaulting key split filename format as provided format seems intended for count/size.")
            current_format = "{base_name}_key_{index}{part}.{ext}"
        # Handle potential mismatch if user didn't provide format and split_type is chunk
        elif split_type == 'chunk' and '{index}' in current_format and ':' not in current_format.split('{index}')[-1].split('}')[0]: # Check if index is used without formatting
            self.log.debug("Defaulting chunk split filename format as provided format seems intended for key.")
            current_format = "{base_name}_{type}_{index:04d}{part}.{ext}"

        try:
            # Apply formatting based on split type to get the basename
            formatted_basename = ""
            if split_type == 'chunk':
                 formatted_basename = current_format.format(**format_args)
            else: # key split - index is string
                # Ensure the format string doesn't try to apply number formatting to the key string
                temp_format = current_format.replace("{index:04d}", "{index}") # Basic safeguard
                formatted_basename = temp_format.format(**format_args)

            # Construct the full path
            output_filename = os.path.join(self.output_dir, formatted_basename)

            # Basic validation on the final path
            # Check if the generated path tries to escape the output directory (e.g., ../..)
            # This is a basic check, more robust checks exist
            abs_output_dir = os.path.abspath(self.output_dir)
            abs_output_file = os.path.abspath(output_filename)
            if not abs_output_file.startswith(abs_output_dir):
                 raise ValueError(f"Generated filename path '{output_filename}' attempts to escape the output directory '{self.output_dir}'.")

            # Check for potentially invalid characters in the basename part after formatting
            check_basename = os.path.basename(formatted_basename)
            if not check_basename or '/' in check_basename or '\\' in check_basename:
                 raise ValueError(f"Generated filename '{formatted_basename}' contains invalid path separators or is empty.")

        except (KeyError, ValueError) as e:
            self.log.error(f"Error applying filename format '{current_format}': {e}. Using fallback naming.")
            # Fallback uses base_name now
            fallback_part_suffix = f"_part_{part_index:04d}" if part_index is not None and part_index > 0 else ""
            fallback_basename = ""
            if split_type == 'key':
                fallback_basename = f"{self.base_name}_key_{index_val}{fallback_part_suffix}.{extension}"
            else:
                try: index_num = int(index_val)
                except: index_num = 0 # Fallback index
                fallback_basename = f"{self.base_name}_chunk_{index_num:04d}{fallback_part_suffix}.{extension}"
            output_filename = os.path.join(self.output_dir, fallback_basename)
            self.log.warning(f"Using fallback filename: {output_filename}")

        except Exception as e:
            self.log.error(f"Unexpected error formatting filename with '{current_format}': {e}. Using fallback naming.")
            fallback_part_suffix = f"_part_{part_index:04d}" if part_index is not None and part_index > 0 else ""
            fallback_basename = ""
            if split_type == 'key':
                 fallback_basename = f"{self.base_name}_key_{index_val}{fallback_part_suffix}.{extension}"
            else:
                try: index_num = int(index_val)
                except: index_num = 0
                fallback_basename = f"{self.base_name}_chunk_{index_num:04d}{fallback_part_suffix}.{extension}"
            output_filename = os.path.join(self.output_dir, fallback_basename)
            self.log.warning(f"Using fallback filename: {output_filename}")

        # Track file before attempting to write
        self.created_files_set.add(output_filename)

        self.log.info(f"  Writing chunk to {output_filename} ({len(chunk_data)} items)...")
        self.log.debug(f"    Format: {self.output_format}, Index: {index_val}, Part: {part_index}")

        try:
            # Ensure output directory exists (should have been validated/created by cli.py, but double-check)
            # output_dir = os.path.dirname(output_filename) # No longer needed, self.output_dir is known
            if self.output_dir:
                os.makedirs(self.output_dir, exist_ok=True)

            # Use 'w' mode; each call creates/overwrites a distinct file part
            with open(output_filename, 'w', encoding='utf-8') as outfile:
                if self.output_format == 'jsonl':
                    for item in chunk_data:
                        json.dump(item, outfile)
                        outfile.write('\n')
                else: # json
                    json.dump(chunk_data, outfile, indent=4)
            return output_filename # Return filename on success
        except IOError as e:
            self.log.error(f"Error writing to file {output_filename}: {e}")
        except TypeError as e:
            self.log.error(f"Error serializing data for {output_filename}: {e}")
        return None # Indicate failure

# --- Concrete Splitter Implementations ---

class CountSplitter(SplitterBase):
    """Splits JSON array/objects based on element count."""
    def __init__(self, count, **kwargs):
        super().__init__(**kwargs)
        self.count = count
        if self.count <= 0:
             raise ValueError("Count must be positive.")

    def split(self):
        # Determine effective splitting mode and limits
        split_by_max_records_only = False
        effective_record_limit = self.count

        if self.max_records is not None:
            self.log.info(f"--max-records ({self.max_records}) provided.")
            if self.max_size_bytes is None:
                self.log.info(f"Splitting strictly by max_records={self.max_records} per file.")
                split_by_max_records_only = True
                effective_record_limit = self.max_records
            else:
                self.log.info(f"Primary count={self.count}, secondary max_records={self.max_records}, secondary max_size set (~{self.max_size_bytes / (1024*1024):.2f}MB).")
        elif self.max_size_bytes:
            self.log.info(f"Primary count={self.count}, secondary max_size set (~{self.max_size_bytes / (1024*1024):.2f}MB).")

        tracker = None # Define tracker outside try block for finally
        items_skipped = 0 # Counter for skipped items
        success_flag = False # Indicate success/failure

        try:
            if split_by_max_records_only:
                 self.log.info(f"Splitting '{self.input_file}' at path '{self.path}' strictly by record count={effective_record_limit}...")
            else:
                self.log.info(f"Splitting '{self.input_file}' at path '{self.path}' primarily by count={self.count}...")
                if self.max_records: self.log.info(f"  Secondary limit: Max {self.max_records} records per file part.")
                if self.max_size_bytes: self.log.info(f"  Secondary limit: Max ~{self.max_size_bytes / (1024*1024):.2f} MB per file part.")

            # Initialize Progress Tracker
            tracker = ProgressTracker(logger=self.log, report_interval=self._report_interval)

            with open(self.input_file, 'rb') as f:
                items_iterator = None # Initialize to None before try block
                try:
                    # *** Attempt to create the iterator ***
                    items_iterator = ijson.items(f, self.path)
                except ijson.common.JSONError as e:
                    # *** Catch error DURING iterator setup ***
                    line, col = getattr(e, 'lineno', None), getattr(e, 'colno', None)
                    line_col_str = f" around line {line}, column {col}" if line is not None and col is not None else ""
                    self.log.error(f"Fatal JSON error setting up iterator for '{self.input_file}'{line_col_str}: {e}. Cannot process file.")
                    raise # Re-raise to be caught by the outer block and trigger failure reporting

                # Check if iterator was successfully created before proceeding
                if items_iterator is None:
                    self.log.error("Failed to initialize JSON item iterator, possibly due to an unexpected issue.")
                    # Raise a generic error as the specific JSONError should have been caught above
                    raise RuntimeError("Iterator initialization failed without specific JSONError.")

                chunk = []
                primary_chunk_index = 0
                items_in_primary_chunk = 0 # Used when NOT split_by_max_records_only
                part_file_index = 0       # Used when NOT split_by_max_records_only
                item_count_total_overall = 0 # Use a separate counter for overall enumeration
                current_part_size_bytes = 0
                base_overhead = 2 if self.output_format == 'json' else 0
                per_item_overhead = 4 if self.output_format == 'json' else 1

                # Iterate using enumerate for an overall count
                for item_count_total_overall, item_candidate in enumerate(items_iterator, 1):
                    tracker.update(item_count_total_overall) # Update tracker with overall count
                    try:
                        # Assume item_candidate is the actual item unless ijson errored fetching it
                        item = item_candidate

                        # Mode 1: Split strictly by max_records
                        if split_by_max_records_only:
                            chunk.append(item)
                            if len(chunk) == effective_record_limit:
                                self._write_chunk(primary_chunk_index, chunk, part_index=None, split_type='chunk')
                                primary_chunk_index += 1
                                chunk = []
                            continue # Continue to next item

                        # Mode 2: Split by primary count with secondary limits
                        item_size = 0
                        if self.max_size_bytes is not None or self.output_format == 'jsonl': # Calculate size only if needed
                             # Estimate item size: convert to JSON string and get bytes length
                             # Use ensure_ascii=False for potentially more accurate size of unicode chars
                             # Add per-item overhead (comma+space/newline)
                             try:
                                 # Using separators for potentially smaller size estimate
                                 item_str = json.dumps(item, ensure_ascii=False, separators=(',', ':'))
                                 item_size = len(item_str.encode('utf-8')) + per_item_overhead
                             except TypeError as te:
                                 self.log.warning(f"Could not serialize item ~#{item_count_total_overall} to estimate size: {te}. Using size 0.")
                                 item_size = 0

                        # Check secondary limits (size or records) FIRST if applicable
                        new_part_needed = False
                        # Check size limit: requires item_size > 0 and chunk not empty
                        if self.max_size_bytes is not None and len(chunk) > 0 and item_size > 0 and (current_part_size_bytes + item_size) > self.max_size_bytes:
                            self.log.debug(f"Secondary size limit ({self.max_size_str}) hit for chunk {primary_chunk_index} at ~{current_part_size_bytes + base_overhead} bytes. Starting part {part_file_index + 1}.")
                            new_part_needed = True
                        # Check record limit
                        elif self.max_records is not None and len(chunk) >= self.max_records:
                            self.log.debug(f"Secondary record limit ({self.max_records}) hit for chunk {primary_chunk_index}. Starting part {part_file_index + 1}.")
                            new_part_needed = True

                        if new_part_needed:
                            # Write existing chunk before adding new item
                            self._write_chunk(primary_chunk_index, chunk, part_index=part_file_index, split_type='chunk')
                            part_file_index += 1
                            chunk = []
                            current_part_size_bytes = base_overhead # Reset size for new part

                        # Add item to current chunk
                        chunk.append(item)
                        items_in_primary_chunk += 1
                        # Update size: Add item size and overhead if it's not the first item
                        current_part_size_bytes += item_size + (per_item_overhead if len(chunk) > 1 else 0)
                        # Correct size if it's the very first item in the chunk
                        if len(chunk) == 1:
                             current_part_size_bytes = base_overhead + item_size

                        # Check primary count limit AFTER adding item
                        if items_in_primary_chunk == self.count:
                            self._write_chunk(primary_chunk_index, chunk, part_index=part_file_index, split_type='chunk')
                            primary_chunk_index += 1
                            items_in_primary_chunk = 0
                            part_file_index = 0 # Reset part index for new primary chunk
                            chunk = []
                            current_part_size_bytes = base_overhead # Reset size for new primary chunk

                    except ijson.common.JSONError as e:
                        # *** Inner catch for errors DURING item parsing ***
                        items_skipped += 1
                        self.log.warning(f"Skipping item ~#{item_count_total_overall} due to JSON parsing/encoding error: {e}")
                        # If a partial chunk exists and we skip, should we write it?
                        # Current logic: skipping item means it's not added to chunk, loop continues.
                        continue # Skip to the next item from items_iterator

                # Write any remaining items in the last chunk
                if chunk:
                     # Handle the case where the loop finished exactly on a primary count boundary
                     # If items_in_primary_chunk is 0, it means the last chunk was already written.
                     if items_in_primary_chunk > 0 or split_by_max_records_only:
                         self._write_chunk(primary_chunk_index, chunk, part_index=part_file_index, split_type='chunk')

            success_flag = True # If we reached here without fatal errors

        except FileNotFoundError:
             self.log.error(f"Input file not found: {self.input_file}")
             success_flag = False
        except ijson.common.JSONError as e: # Catches re-raised setup error or other fatal ijson errors
             # Log with traceback regardless of verbose flag for this specific error
             self.log.exception(f"Fatal JSON error encountered during splitting of '{self.input_file}': {e}")
             success_flag = False
        except (IOError, OSError) as e:
            self.log.error(f"File system error during count splitting: {e}")
            success_flag = False
        except Exception as e:
             # Log other unexpected errors with traceback if verbose
             self.log.error(f"An unexpected error occurred during splitting: {e}", exc_info=self.verbose)
             success_flag = False
        finally:
             # Report skipped items
             if items_skipped > 0:
                 self.log.warning(f"Completed splitting, but skipped {items_skipped} items due to parsing/encoding errors during processing.")
             # Make sure finalize is called even if loop didn't run or failed early
             if tracker:
                 tracker.finalize()

        # Return success status determined during execution
        if not success_flag:
             log.error("Splitting process failed or terminated early.")
        return success_flag


class SizeSplitter(SplitterBase):
    """Splits JSON array/objects based on approximate size."""

    def __init__(self, size, **kwargs):
        # Special handling for size: parse it in the base class init
        # We expect max_size to be passed here as 'size' for consistency with CLI args
        if 'max_size' in kwargs and kwargs['max_size'] != size:
            log.warning(f"SizeSplitter initialized with both 'size' ({size}) and 'max_size' ({kwargs['max_size']}). Using 'size'.")
        kwargs['max_size'] = size # Ensure base class gets the primary value

        super().__init__(**kwargs)

        # Primary limit is the size provided
        self.primary_size_limit_bytes = self.max_size_bytes
        if self.primary_size_limit_bytes is None or self.primary_size_limit_bytes <= 0:
             # This should have been caught by base class or earlier validation
             raise ValueError("SizeSplitter requires a valid positive size argument.")

        # Secondary limit is max_records
        self.secondary_record_limit = self.max_records

        # Reset max_size_bytes in base class context as it's now the *primary* limit for this splitter
        self.max_size_bytes = None # Clear secondary size limit from base perspective

        # For clarity in SizeSplitter, refer to primary limit directly
        self.size = self.primary_size_limit_bytes


    def split(self):
        self.log.info(f"Splitting '{self.input_file}' at path '{self.path}' primarily by size={self.max_size_str} (~{self.size / (1024*1024):.2f} MB)...")
        if self.secondary_record_limit:
            self.log.info(f"  Secondary limit: Max {self.secondary_record_limit} records per file part.")

        # Initialize Progress Tracker
        tracker = None
        items_skipped = 0 # Counter for skipped items
        success_flag = False

        try:
            tracker = ProgressTracker(logger=self.log, report_interval=self._report_interval)

            with open(self.input_file, 'rb') as f:
                items_iterator = None # Initialize to None
                try:
                    # *** Attempt to create the iterator ***
                    items_iterator = ijson.items(f, self.path)
                except ijson.common.JSONError as e:
                    # *** Catch error DURING iterator setup ***
                    line, col = getattr(e, 'lineno', None), getattr(e, 'colno', None)
                    line_col_str = f" around line {line}, column {col}" if line is not None and col is not None else ""
                    self.log.error(f"Fatal JSON error setting up iterator for '{self.input_file}'{line_col_str}: {e}. Cannot process file.")
                    raise # Re-raise

                # Check if iterator was successfully created
                if items_iterator is None:
                    self.log.error("Failed to initialize JSON item iterator.")
                    raise RuntimeError("Iterator initialization failed without specific JSONError.")

                chunk = []
                chunk_index = 0
                item_count_total_overall = 0
                current_chunk_size_bytes = 0
                # Rough estimate of overhead: [] for JSON, newlines for JSONL
                base_overhead = 2 if self.output_format == 'json' else 0
                # Rough estimate per item: ',' for JSON, newline for JSONL
                per_item_overhead = 4 if self.output_format == 'json' else 1
                current_chunk_size_bytes = base_overhead # Start with overhead

                # Iterate using enumerate for an overall count
                for item_count_total_overall, item_candidate in enumerate(items_iterator, 1):
                    tracker.update(item_count_total_overall) # Update tracker
                    try:
                        # Assume item_candidate is the actual item unless ijson errored fetching it
                        item = item_candidate

                        # Calculate item size
                        item_size = 0
                        try:
                            # Serialize item to estimate size
                            # Using separators=(',', ':') for slightly smaller size, closer to file size
                            item_str = json.dumps(item, ensure_ascii=False, separators=(',', ':'))
                            item_bytes = item_str.encode('utf-8')
                            item_size = len(item_bytes)
                        except TypeError as te:
                            self.log.warning(f"Could not serialize item ~#{item_count_total_overall} to estimate size: {te}. Using size 0 for split check.")
                            item_size = 0

                        # Determine if adding this item exceeds limits (before adding)
                        # Need current chunk size + item size + potential overhead
                        potential_next_size = current_chunk_size_bytes + item_size + (per_item_overhead if chunk else 0)
                        # Check primary size limit (only if chunk > 0 to avoid splitting on first item)
                        exceeds_primary_size = len(chunk) > 0 and potential_next_size > self.size
                        # Check secondary record limit
                        exceeds_secondary_records = self.secondary_record_limit and (len(chunk) + 1) > self.secondary_record_limit

                        # Split if necessary *before* adding the current item
                        if exceeds_primary_size or exceeds_secondary_records:
                            if chunk: # Only write if there's something in the current chunk
                                reason = "size limit" if exceeds_primary_size else "record limit"
                                self.log.debug(f"Writing chunk {chunk_index} due to {reason} ({len(chunk)} items, ~{current_chunk_size_bytes / (1024*1024):.2f} MB)...")
                                self._write_chunk(chunk_index, chunk, split_type='chunk') # Use split_type='chunk'
                                chunk = []
                                current_chunk_size_bytes = base_overhead # Reset size
                                chunk_index += 1
                            else:
                                # This happens if a single item exceeds the size limit and chunk is empty
                                self.log.warning(f"Item ~#{item_count_total_overall} alone (size ~{item_size / (1024*1024):.2f} MB) may exceed the target chunk size of {self.size / (1024*1024):.2f} MB. Writing it to its own file.")
                                # The item will be added below and written immediately in the next check or at the end
                                pass

                        # Add the current item to the (potentially new) chunk
                        chunk.append(item)
                        # Update size: add item size and overhead if it's not the first item
                        current_chunk_size_bytes += item_size + (per_item_overhead if len(chunk) > 1 else 0)
                        # Correct size if it's the very first item in the chunk
                        if len(chunk) == 1:
                            current_chunk_size_bytes = base_overhead + item_size

                        # Special case: If the *first* item added *also* hits the secondary record limit (limit is 1)
                        # Or if a single large item was added and needs immediate write
                        if self.secondary_record_limit == 1 and len(chunk) == 1:
                             self.log.debug(f"Writing chunk {chunk_index} due to record limit=1.")
                             self._write_chunk(chunk_index, chunk, split_type='chunk')
                             chunk = []
                             current_chunk_size_bytes = base_overhead
                             chunk_index += 1
                        elif exceeds_primary_size and len(chunk) == 1: # Handle single large item write
                             self.log.debug(f"Writing chunk {chunk_index} containing single large item.")
                             self._write_chunk(chunk_index, chunk, split_type='chunk')
                             chunk = []
                             current_chunk_size_bytes = base_overhead
                             chunk_index += 1


                    except ijson.common.JSONError as e:
                        items_skipped += 1
                        self.log.warning(f"Skipping item ~#{item_count_total_overall} due to JSON parsing/encoding error: {e}")
                        # Logic continues, skipped item is not added to chunk.
                        continue # Skip to the next item from items_iterator

                # Write any remaining items after the loop
                if chunk:
                     self.log.debug(f"Writing final chunk {chunk_index} ({len(chunk)} items, ~{current_chunk_size_bytes / (1024*1024):.2f} MB)...")
                     self._write_chunk(chunk_index, chunk, split_type='chunk')

            success_flag = True # Reached end without fatal error

        except FileNotFoundError:
            self.log.error(f"Error: Input file '{self.input_file}' not found.")
            success_flag = False
        except ijson.common.JSONError as e: # Catches re-raised setup error or other fatal ijson errors
            # Log with traceback regardless of verbose flag for this specific error
            self.log.exception(f"Fatal JSON error encountered during splitting of '{self.input_file}': {e}")
            success_flag = False
        except (IOError, OSError) as e:
            self.log.error(f"File system error during size splitting: {e}")
            success_flag = False
        except MemoryError:
            self.log.error("Memory error during size splitting.")
            success_flag = False
        except Exception as e:
            # Log other unexpected errors with traceback if verbose
            self.log.exception(f"An unexpected error occurred during size splitting: {e}", exc_info=self.verbose)
            success_flag = False
        finally:
            if items_skipped > 0:
                self.log.warning(f"Completed splitting, but skipped {items_skipped} items due to parsing/encoding errors.")
            if tracker:
                tracker.finalize()

        # Return the success status determined in try/except blocks
        if not success_flag:
             log.error("Splitting process failed or terminated early.")
        return success_flag


class KeySplitter(SplitterBase):
    """Splits JSON objects based on the value of a specified key."""
    def __init__(self, key_name, on_missing_key='group', on_invalid_item='warn', **kwargs):
        # Key splitting forces jsonl
        output_format = kwargs.get('output_format', 'jsonl')
        if output_format == 'json':
             log.warning("Key-based splitting enforces JSON Lines ('jsonl'). Overriding format.")
        kwargs['output_format'] = 'jsonl' # Enforce jsonl

        super().__init__(**kwargs)
        self.key_name = key_name
        self.on_missing_key = on_missing_key
        self.on_invalid_item = on_invalid_item
        if not self.key_name:
            raise ValueError("Key name cannot be empty for key splitting.")

        # Key splitter specific defaults/logic
        self.output_format = 'jsonl' # Enforce again just in case
        self.file_format_extension = 'jsonl'
        # Override default filename format if not provided or unsuitable for key splitting
        if not self.filename_format or '{index:04d}' in self.filename_format:
             default_key_format = "{base_name}_key_{index}{part}.{ext}"
             if self.filename_format and self.filename_format != default_key_format:
                  self.log.debug(f"Filename format '{self.filename_format}' seems unsuitable for key splitting. Using default: '{default_key_format}'")
             self.filename_format = default_key_format

        # Validate policies early
        if self.on_missing_key not in ('group', 'skip', 'error'):
            raise ValueError("Invalid value for on_missing_key. Choose 'group', 'skip', or 'error'.")
        if self.on_invalid_item not in ('warn', 'skip', 'error'):
             raise ValueError("Invalid value for on_invalid_item. Choose 'warn', 'skip', or 'error'.")


    def split(self):
        self.log.info(f"Splitting '{self.input_file}' at path '{self.path}' by key '{self.key_name}'...")
        self.log.info(f"Output directory: {os.path.abspath(self.output_dir)}")
        self.log.info(f"Base name: {self.base_name}")
        self.log.info(f"Filename format: {self.filename_format}")
        self.log.info(f"Maximum open files cache size: {MAX_OPEN_FILES_KEY_SPLIT}")
        if self.max_records: self.log.info(f"  Secondary limit: Max {self.max_records} records per file part.")
        if self.max_size_bytes: self.log.info(f"  Secondary limit: Max ~{self.max_size_bytes / (1024*1024):.2f} MB per file part.")

        # Use cachetools LRUCache for managing file handles (Key: filepath, Value: handle)
        open_files_cache = LRUCache(maxsize=MAX_OPEN_FILES_KEY_SPLIT)
        # Track stats per key/part combination { (sanitized_key, part_index): {'count': N, 'size': M} }
        file_stats = {}
        tracker = None # Define tracker outside try block for finally

        items_processed = 0
        items_written = 0
        items_skipped_missing_key = 0
        items_skipped_invalid_type = 0
        items_skipped_serialization = 0
        items_skipped_write_error = 0
        items_skipped_parsing_error = 0 # For ijson errors
        items_grouped_missing_key = 0

        missing_key_group_key = "__missing_key__"
        complex_type_group_prefix = "__complex_type_"
        empty_sanitized_key = "__empty_key__"

        success_flag = False # Assume failure until proven otherwise

        try:
            tracker = ProgressTracker(logger=self.log, report_interval=self._report_interval)

            with open(self.input_file, 'rb') as f:
                items_iterator = None # Initialize to None
                try:
                    # *** Attempt to create the iterator ***
                    items_iterator = ijson.items(f, self.path)
                except ijson.common.JSONError as e:
                    # *** Catch error DURING iterator setup ***
                    line, col = getattr(e, 'lineno', None), getattr(e, 'colno', None)
                    line_col_str = f" around line {line}, column {col}" if line is not None and col is not None else ""
                    self.log.error(f"Fatal JSON error setting up iterator for '{self.input_file}'{line_col_str}: {e}. Cannot process file.")
                    raise # Re-raise

                # Check if iterator was successfully created
                if items_iterator is None:
                    self.log.error("Failed to initialize JSON item iterator.")
                    raise RuntimeError("Iterator initialization failed without specific JSONError.")

                for items_processed, item_candidate in enumerate(items_iterator, 1):
                    tracker.update(items_processed) # Call new tracker update

                    try:
                        # --- Attempt to retrieve and validate the item ---
                        item = item_candidate # Assume ijson yields the object directly

                        # 1. Validate item structure (should be dict/object)
                        if not isinstance(item, dict):
                            msg = f"Item ~#{items_processed} at path '{self.path}' is not a JSON object (type: {type(item).__name__})."
                            if self.on_invalid_item == 'error':
                                self.log.error(msg + " Stopping due to on_invalid_item='error'.")
                                raise TypeError(msg) # Raise to break loop via outer except
                            elif self.on_invalid_item == 'skip':
                                self.log.debug(f"Skipping: {msg}")
                                items_skipped_invalid_type += 1; continue
                            else: # warn
                                self.log.warning(f"{msg} Skipping key check for this item.")
                                items_skipped_invalid_type += 1; continue

                        # --- Key Extraction and Sanitization ---
                        key_value_original = item.get(self.key_name, None)
                        sanitized_key = None

                        if key_value_original is None: # Key is missing
                            if self.on_missing_key == 'error':
                                msg = f"Key '{self.key_name}' not found in item ~#{items_processed}."
                                self.log.error(msg + " Stopping due to on_missing_key='error'.")
                                raise KeyError(msg) # Raise to break loop
                            elif self.on_missing_key == 'skip':
                                self.log.debug(f"Skipping item ~#{items_processed}: Key '{self.key_name}' missing.")
                                items_skipped_missing_key += 1; continue
                            else: # group
                                sanitized_key = missing_key_group_key
                                items_grouped_missing_key += 1 # Count grouped items
                        elif isinstance(key_value_original, (dict, list)): # Key value is complex
                            complex_type = type(key_value_original).__name__
                            sanitized_key = f"{complex_type_group_prefix}{sanitize_filename(complex_type)}"
                            self.log.warning(f"Key '{self.key_name}' in item ~#{items_processed} is complex ({complex_type}). Grouping as '{sanitized_key}'.")
                        else: # Key value is simple, sanitize it
                            sanitized_key = sanitize_filename(str(key_value_original))
                            if not sanitized_key: # Handle empty result after sanitization
                                 self.log.warning(f"Key value '{key_value_original}' in item ~#{items_processed} resulted in empty sanitized filename. Grouping as '{empty_sanitized_key}'.")
                                 sanitized_key = empty_sanitized_key

                        if sanitized_key is None: # Should not happen if logic is correct
                            self.log.error(f"Internal logic error: Sanitized key is None for item ~#{items_processed}. Skipping.")
                            continue

                        # --- Serialize Item (needed for size checks and writing) --- #
                        item_size = 0
                        item_str = None
                        try:
                            # Use ensure_ascii=False for potentially better unicode handling & size estimate
                            item_str = json.dumps(item, ensure_ascii=False)
                            # Estimate size only if size limit is active
                            if self.max_size_bytes:
                                item_bytes = item_str.encode('utf-8')
                                item_size = len(item_bytes) + 1 # +1 for newline in jsonl
                        except TypeError as e:
                            self.log.warning(f"Could not serialize item ~#{items_processed} (key: {sanitized_key}): {e}. Skipping.")
                            items_skipped_serialization += 1
                            continue

                        # --- Determine Target File Part & Check Secondary Limits --- #
                        # Find the current part index for this key by checking stats
                        current_part_index = 0
                        while True: # Loop to find the correct part index
                            target_key_part_tuple = (sanitized_key, current_part_index)
                            current_stats = file_stats.get(target_key_part_tuple, {'count': 0, 'size': 0})
                            record_count = current_stats['count']
                            approx_size_bytes = current_stats['size']

                            # Check secondary limits (only if part already has items)
                            secondary_limit_hit = False
                            if record_count > 0:
                                if self.max_records is not None and record_count >= self.max_records:
                                    secondary_limit_hit = True
                                    split_reason = f"record limit ({self.max_records})"
                                elif self.max_size_bytes is not None and (approx_size_bytes + item_size) > self.max_size_bytes:
                                    secondary_limit_hit = True
                                    split_reason = f"size limit (~{self.max_size_bytes / (1024*1024):.2f}MB)"

                            if secondary_limit_hit:
                                 self.log.debug(f"Secondary {split_reason} hit for key '{sanitized_key}', part {current_part_index}. Moving to part {current_part_index + 1}.")
                                 # Close the previous part's handle if it's in the cache
                                 self._close_cached_file(sanitized_key, current_part_index, open_files_cache)
                                 current_part_index += 1
                                 continue # Restart check for the new part index
                            else:
                                # This is the correct part index to write to
                                break # Exit the while loop

                        # --- Get File Handle for Current Part --- #
                        target_key_part_tuple = (sanitized_key, current_part_index)
                        outfile, file_path = self._get_or_open_file(
                            sanitized_key,
                            current_part_index,
                            open_files_cache
                            # No need to pass file_stats here
                        )

                        if outfile is None or outfile.closed:
                             self.log.error(f"Failed to get valid file handle for key '{sanitized_key}', part {current_part_index} (path: {file_path}). Skipping item ~#{items_processed}.")
                             items_skipped_write_error += 1
                             continue

                        # --- Write Item --- #
                        try:
                            outfile.write(item_str + '\\n')
                            items_written += 1
                            # Update stats AFTER successful write
                            current_stats = file_stats.get(target_key_part_tuple, {'count': 0, 'size': 0})
                            current_stats['count'] += 1
                            current_stats['size'] += item_size
                            file_stats[target_key_part_tuple] = current_stats # Store updated stats
                        except (IOError, OSError) as e:
                            self.log.error(f"Failed to write item ~#{items_processed} to file '{file_path}' for key '{sanitized_key}': {e}.")
                            self._close_cached_file(sanitized_key, current_part_index, open_files_cache, force_pop=True) # Close and remove from cache
                            items_skipped_write_error += 1
                            continue # Skip this item

                    except ijson.common.JSONError as e:
                        # This catches errors during the yielding of 'item_candidate' itself
                        items_skipped_parsing_error += 1
                        self.log.warning(f"Skipping item ~#{items_processed} due to JSON parsing/encoding error during item retrieval: {e}")
                        # Item could not be retrieved, so no further processing needed for it.
                        continue # Skip to the next item from items_iterator
                    except (KeyError, TypeError, ValueError) as e: # Catch policy errors raised above
                         self.log.error(f"Terminating splitting due to policy error: {e}")
                         raise # Re-raise to be caught by outer handler and set success_flag=False
                    except Exception as e: # Catch unexpected errors during item processing
                         self.log.exception(f"Unexpected error processing item ~#{items_processed} (key: '{key_value_original if 'key_value_original' in locals() else 'unknown'}'):")
                         # Depending on severity, might want to skip or raise
                         continue # Skip this item for now


            # End of main processing loop (inside try block)
            self.log.info("Finished processing input file stream.")
            success_flag = True # If we reach here, processing completed without fatal policy errors

        except FileNotFoundError:
            self.log.error(f"Error: Input file '{self.input_file}' not found.")
            success_flag = False
        except ijson.common.JSONError as e: # Catches re-raised setup error or other fatal ijson errors
            # Log with traceback regardless of verbose flag for this specific error
            self.log.exception(f"Fatal JSON error encountered during splitting of '{self.input_file}': {e}")
            success_flag = False
        except (IOError, OSError) as e:
            self.log.error(f"File system error during key splitting: {e}")
            success_flag = False
        except (KeyError, TypeError, ValueError) as e: # Catch policy errors raised in the loop
            # Error already logged when raised
            success_flag = False
        except MemoryError:
            self.log.error("Memory error during key splitting setup or loop.")
            success_flag = False
        except Exception as e:
            self.log.exception(f"An unexpected error occurred during key splitting: {e}")
            success_flag = False
        finally:
            # This block *always* executes, ensuring files are closed
            self.log.info("Closing remaining open files...")
            closed_count = 0
            # Safely iterate and close handles from the cache
            cached_files = list(open_files_cache.keys()) # Get keys first
            for file_path in cached_files:
                handle = open_files_cache.pop(file_path, None) # Remove from cache
                if handle and not handle.closed:
                    try:
                        handle.close()
                        closed_count += 1
                    except Exception as e:
                         self.log.warning(f"Error closing file {file_path}: {e}")
            self.log.info(f"Closed {closed_count} file handles during cleanup.")

            # Final progress report
            if tracker:
                 tracker.finalize()

            # Report final counts
            if items_processed > 0:
                 self.log.info(f"--- Summary ---")
                 self.log.info(f"  Total items processed from stream: {items_processed}")
                 self.log.info(f"  Items written to files: {items_written}")
                 if items_skipped_missing_key: self.log.warning(f"  Items skipped (missing key): {items_skipped_missing_key}")
                 if items_grouped_missing_key: self.log.info(f"  Items grouped (missing key): {items_grouped_missing_key}")
                 if items_skipped_invalid_type: self.log.warning(f"  Items skipped (invalid type): {items_skipped_invalid_type}")
                 if items_skipped_serialization: self.log.warning(f"  Items skipped (serialization error): {items_skipped_serialization}")
                 if items_skipped_parsing_error: self.log.warning(f"  Items skipped (JSON parsing/encoding error): {items_skipped_parsing_error}")
                 if items_skipped_write_error: self.log.warning(f"  Items skipped (file write error): {items_skipped_write_error}")
                 total_skipped = items_skipped_missing_key + items_skipped_invalid_type + items_skipped_serialization + items_skipped_parsing_error + items_skipped_write_error
                 if total_skipped + items_written + items_grouped_missing_key != items_processed:
                      # This check might be slightly off if grouping isn't counted as written/skipped exactly
                      # self.log.debug("Item count discrepancy detected.")
                      pass
            else:
                 self.log.info("No items processed from the stream.")

        # Return the success status determined in try/except blocks
        if not success_flag:
             log.error("Splitting process failed or terminated early.")
        return success_flag


    def _get_or_open_file(self, sanitized_key, part_index, file_cache):
        """Gets file handle from cache or opens a new one.
           Handles filename formatting.
           Returns (file_handle, full_file_path) or (None, None) on error.
        """
        # Generate the full filename using the format string
        part_suffix = f"_part_{part_index:04d}" if part_index > 0 else ""
        format_args = {
            'base_name': self.base_name,
            'type': 'key',
            'index': sanitized_key,
            'part': part_suffix,
            'ext': self.file_format_extension # Should be jsonl
        }

        formatted_basename = ""
        full_file_path = None
        try:
            # Assume self.filename_format is correctly set in __init__
            current_format = self.filename_format
            # Basic safeguard against number formatting for string index
            temp_format = current_format.replace("{index:04d}", "{index}")
            formatted_basename = temp_format.format(**format_args)

            # Construct the full path
            full_file_path = os.path.join(self.output_dir, formatted_basename)

            # Add basic validation checks similar to _write_chunk
            abs_output_dir = os.path.abspath(self.output_dir)
            abs_output_file = os.path.abspath(full_file_path)
            if not abs_output_file.startswith(abs_output_dir):
                 raise ValueError(f"Generated filename path '{full_file_path}' attempts to escape the output directory '{self.output_dir}'.")
            check_basename = os.path.basename(formatted_basename)
            if not check_basename or '/' in check_basename or '\\\\' in check_basename:
                 raise ValueError(f"Generated filename '{formatted_basename}' contains invalid path separators or is empty.")

        except (KeyError, ValueError) as e:
            # Use a more specific fallback name
            fallback_basename = f"{self.base_name}_key_{sanitized_key}{part_suffix}.{self.file_format_extension}"
            self.log.error(f"Error applying filename format '{self.filename_format}' for key '{sanitized_key}': {e}. Using fallback: {fallback_basename}")
            full_file_path = os.path.join(self.output_dir, fallback_basename)
        except Exception as e:
             # Generic fallback
             fallback_basename = f"{self.base_name}_key_{sanitized_key}{part_suffix}.{self.file_format_extension}"
             self.log.error(f"Unexpected error formatting filename for key '{sanitized_key}': {e}. Using fallback: {fallback_basename}")
             full_file_path = os.path.join(self.output_dir, fallback_basename)

        if full_file_path is None: # Should not happen if fallback works
            self.log.error(f"Could not determine filename for key '{sanitized_key}', part {part_index}. Cannot open file.")
            return None, None

        # Check cache using the full file path as the key
        if full_file_path in file_cache:
            cached_handle = file_cache[full_file_path]
            if not cached_handle.closed:
                # self.log.debug(f"Cache hit for {full_file_path}")
                return cached_handle, full_file_path
            else:
                self.log.warning(f"Found closed handle in cache for {full_file_path}. Will reopen.")
                # Remove the closed handle before reopening
                file_cache.pop(full_file_path, None)

        # Not in cache or handle was closed, open file (append mode)
        self.log.debug(f"Cache miss or closed handle. Opening {full_file_path} (Append Mode)")
        try:
            # Ensure directory exists
            if self.output_dir:
                os.makedirs(self.output_dir, exist_ok=True)

            # Check if this specific file needs to be tracked (first time seeing it)
            if full_file_path not in self.created_files_set:
                 self.created_files_set.add(full_file_path)
                 self.log.info(f"  Creating/Appending output file: {full_file_path}")

            # Open in append mode, utf-8 encoding
            file_handle = open(full_file_path, 'a', encoding='utf-8')

            # Add new handle to cache
            # Check if adding will cause eviction (LRU behavior) - we don't explicitly handle closing evicted handles here
            # Relies on the finally block and explicit closing during part splits.
            file_cache[full_file_path] = file_handle

            return file_handle, full_file_path

        except (IOError, OSError) as e:
            self.log.error(f"Could not open file {full_file_path} for append: {e}")
            return None, None
        except Exception as e:
            self.log.exception(f"Unexpected error opening file {full_file_path}: {e}")
            return None, None

    def _close_cached_file(self, sanitized_key, part_index, file_cache, force_pop=False):
         """Helper to close a specific file handle in the cache if found."""
         # Reconstruct the potential file path to check the cache
         # (This duplicates filename generation, maybe refactor later)
         part_suffix = f"_part_{part_index:04d}" if part_index > 0 else ""
         format_args = {
             'base_name': self.base_name, 'type': 'key', 'index': sanitized_key,
             'part': part_suffix, 'ext': self.file_format_extension
         }
         file_path = None
         try:
              temp_format = self.filename_format.replace("{index:04d}", "{index}")
              formatted_basename = temp_format.format(**format_args)
              file_path = os.path.join(self.output_dir, formatted_basename)
         except Exception:
              # Try fallback name if formatting failed
              fallback_basename = f"{self.base_name}_key_{sanitized_key}{part_suffix}.{self.file_format_extension}"
              file_path = os.path.join(self.output_dir, fallback_basename)

         if file_path and file_path in file_cache:
              handle_to_close = file_cache[file_path] # Get handle before potential pop
              if force_pop:
                   handle_to_close = file_cache.pop(file_path, None) # Remove if forcing

              if handle_to_close and not handle_to_close.closed:
                   try:
                        self.log.debug(f"Closing cached handle for {file_path}")
                        handle_to_close.close()
                   except Exception as e:
                        self.log.warning(f"Error closing cached file {file_path}: {e}")
         # else: File path not determined or not in cache


    def _write_chunk(self, primary_index, chunk_data, part_index=None, split_type='chunk', key_value=None):
        """Writes a chunk of data to a uniquely named file using the filename format.

        Args:
            primary_index (int or str): The primary index (chunk number or sanitized key).
            chunk_data (list): The data to write.
            part_index (int, optional): The part index for secondary splits.
            split_type (str): 'chunk' for count/size, 'key' for key split.
            key_value (str, optional): The sanitized key value (used for 'key' split index).
        """
        if not chunk_data:
            self.log.warning(f"Attempted to write empty chunk for index {primary_index}, part {part_index}. Skipping.")
            return None # Indicate no file was written

        extension = 'jsonl' if self.output_format == 'jsonl' else 'json'
        part_suffix = f"_part_{part_index:04d}" if part_index is not None and part_index > 0 else ""

        # Use key_value for index if split_type is 'key', otherwise use primary_index (number)
        index_val = key_value if split_type == 'key' else primary_index

        format_args = {
            'base_name': self.base_name,
            'type': split_type,
            'index': index_val,
            'part': part_suffix,
            'ext': extension
        }

        # Determine the correct filename format string
        current_format = self.filename_format
        if not current_format: # Use default if None
             current_format = "{base_name}_key_{index}{part}.{ext}" if split_type == 'key' else "{base_name}_{type}_{index:04d}{part}.{ext}"
        # Handle potential mismatch if user didn't provide format and split_type is key
        elif split_type == 'key' and '{index:04d}' in current_format:
            self.log.debug("Defaulting key split filename format as provided format seems intended for count/size.")
            current_format = "{base_name}_key_{index}{part}.{ext}"
        # Handle potential mismatch if user didn't provide format and split_type is chunk
        elif split_type == 'chunk' and '{index}' in current_format and ':' not in current_format.split('{index}')[-1].split('}')[0]: # Check if index is used without formatting
            self.log.debug("Defaulting chunk split filename format as provided format seems intended for key.")
            current_format = "{base_name}_{type}_{index:04d}{part}.{ext}"

        try:
            # Apply formatting based on split type to get the basename
            formatted_basename = ""
            if split_type == 'chunk':
                 formatted_basename = current_format.format(**format_args)
            else: # key split - index is string
                # Ensure the format string doesn't try to apply number formatting to the key string
                temp_format = current_format.replace("{index:04d}", "{index}") # Basic safeguard
                formatted_basename = temp_format.format(**format_args)

            # Construct the full path
            output_filename = os.path.join(self.output_dir, formatted_basename)

            # Basic validation on the final path
            # Check if the generated path tries to escape the output directory (e.g., ../..)
            # This is a basic check, more robust checks exist
            abs_output_dir = os.path.abspath(self.output_dir)
            abs_output_file = os.path.abspath(output_filename)
            if not abs_output_file.startswith(abs_output_dir):
                 raise ValueError(f"Generated filename path '{output_filename}' attempts to escape the output directory '{self.output_dir}'.")

            # Check for potentially invalid characters in the basename part after formatting
            check_basename = os.path.basename(formatted_basename)
            if not check_basename or '/' in check_basename or '\\' in check_basename:
                 raise ValueError(f"Generated filename '{formatted_basename}' contains invalid path separators or is empty.")

        except (KeyError, ValueError) as e:
            self.log.error(f"Error applying filename format '{current_format}': {e}. Using fallback naming.")
            # Fallback uses base_name now
            fallback_part_suffix = f"_part_{part_index:04d}" if part_index is not None and part_index > 0 else ""
            fallback_basename = ""
            if split_type == 'key':
                fallback_basename = f"{self.base_name}_key_{index_val}{fallback_part_suffix}.{extension}"
            else:
                try: index_num = int(index_val)
                except: index_num = 0 # Fallback index
                fallback_basename = f"{self.base_name}_chunk_{index_num:04d}{fallback_part_suffix}.{extension}"
            output_filename = os.path.join(self.output_dir, fallback_basename)
            self.log.warning(f"Using fallback filename: {output_filename}")

        except Exception as e:
            self.log.error(f"Unexpected error formatting filename with '{current_format}': {e}. Using fallback naming.")
            fallback_part_suffix = f"_part_{part_index:04d}" if part_index is not None and part_index > 0 else ""
            fallback_basename = ""
            if split_type == 'key':
                 fallback_basename = f"{self.base_name}_key_{index_val}{fallback_part_suffix}.{extension}"
            else:
                try: index_num = int(index_val)
                except: index_num = 0
                fallback_basename = f"{self.base_name}_chunk_{index_num:04d}{fallback_part_suffix}.{extension}"
            output_filename = os.path.join(self.output_dir, fallback_basename)
            self.log.warning(f"Using fallback filename: {output_filename}")

        # Track file before attempting to write
        self.created_files_set.add(output_filename)

        self.log.info(f"  Writing chunk to {output_filename} ({len(chunk_data)} items)...")
        self.log.debug(f"    Format: {self.output_format}, Index: {index_val}, Part: {part_index}")

        try:
            # Ensure output directory exists (should have been validated/created by cli.py, but double-check)
            # output_dir = os.path.dirname(output_filename) # No longer needed, self.output_dir is known
            if self.output_dir:
                os.makedirs(self.output_dir, exist_ok=True)

            # Use 'w' mode; each call creates/overwrites a distinct file part
            with open(output_filename, 'w', encoding='utf-8') as outfile:
                if self.output_format == 'jsonl':
                    for item in chunk_data:
                        json.dump(item, outfile)
                        outfile.write('\n')
                else: # json
                    json.dump(chunk_data, outfile, indent=4)
            return output_filename # Return filename on success
        except IOError as e:
            self.log.error(f"Error writing to file {output_filename}: {e}")
        except TypeError as e:
            self.log.error(f"Error serializing data for {output_filename}: {e}")
        return None # Indicate failure 