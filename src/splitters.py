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

    def __init__(self, input_file, output_prefix, path, output_format,
                 max_records=None, max_size=None, # Use max_size string here
                 filename_format=None, verbose=False,
                 created_files_set=None,
                 report_interval: int = 10000, # Added report_interval parameter
                 **kwargs): # Accept extra args
        self.input_file = input_file
        self.output_prefix = output_prefix
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
            'prefix': self.output_prefix,
            'type': split_type,
            'index': index_val,
            'part': part_suffix,
            'ext': extension
        }

        # Determine the correct filename format string
        current_format = self.filename_format
        if not current_format: # Use default if None
             current_format = "{prefix}_key_{index}{part}.{ext}" if split_type == 'key' else "{prefix}_{type}_{index:04d}{part}.{ext}"
        # Handle potential mismatch if user didn't provide format and split_type is key
        elif split_type == 'key' and '{index:04d}' in current_format:
            self.log.debug("Defaulting key split filename format as provided format seems intended for count/size.")
            current_format = "{prefix}_key_{index}{part}.{ext}"
        # Handle potential mismatch if user didn't provide format and split_type is chunk
        elif split_type == 'chunk' and '{index}' in current_format and ':' not in current_format.split('{index}')[-1].split('}')[0]: # Check if index is used without formatting
            self.log.debug("Defaulting chunk split filename format as provided format seems intended for key.")
            current_format = "{prefix}_{type}_{index:04d}{part}.{ext}"

        try:
            # Apply formatting based on split type
            if split_type == 'chunk':
                 output_filename = current_format.format(**format_args)
            else: # key split - index is string
                # Ensure the format string doesn't try to apply number formatting to the key string
                temp_format = current_format.replace("{index:04d}", "{index}") # Basic safeguard
                output_filename = temp_format.format(**format_args)

            # Basic validation
            basename = os.path.basename(output_filename)
            if not basename or '/' in basename or '\\' in basename:
                raise ValueError(f"Generated filename '{output_filename}' seems invalid (contains path separators).")

        except (KeyError, ValueError) as e:
            self.log.error(f"Error applying filename format '{current_format}': {e}. Using fallback naming.")
            fallback_part_suffix = f"_part_{part_index:04d}" if part_index is not None and part_index > 0 else ""
            if split_type == 'key':
                output_filename = f"{self.output_prefix}_key_{index_val}{fallback_part_suffix}.{extension}"
            else:
                # Ensure index_val is treated as an int for formatting
                try: index_num = int(index_val) 
                except: index_num = 0 # Fallback index
                output_filename = f"{self.output_prefix}_chunk_{index_num:04d}{fallback_part_suffix}.{extension}"
        except Exception as e:
            self.log.error(f"Unexpected error formatting filename with '{current_format}': {e}. Using fallback naming.")
            fallback_part_suffix = f"_part_{part_index:04d}" if part_index is not None and part_index > 0 else ""
            if split_type == 'key':
                 output_filename = f"{self.output_prefix}_key_{index_val}{fallback_part_suffix}.{extension}"
            else:
                try: index_num = int(index_val) 
                except: index_num = 0
                output_filename = f"{self.output_prefix}_chunk_{index_num:04d}{fallback_part_suffix}.{extension}"

        # Track file before attempting to write
        self.created_files_set.add(output_filename)

        self.log.info(f"  Writing chunk to {output_filename} ({len(chunk_data)} items)...")
        self.log.debug(f"    Format: {self.output_format}, Index: {index_val}, Part: {part_index}")

        try:
            # Ensure output directory exists
            output_dir = os.path.dirname(output_filename)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)

            # Use 'w' mode; each call creates/overwrites a distinct file part
            with open(output_filename, 'w', encoding='utf-8') as outfile:
                if self.output_format == 'jsonl':
                    for item in chunk_data:
                        json.dump(item, outfile)
                        outfile.write('\n')
                else: # json
                    json.dump(chunk_data, outfile, indent=None)
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
                items_iterator = ijson.items(f, self.path)
                chunk = []
                primary_chunk_index = 0
                items_in_primary_chunk = 0 # Used when NOT split_by_max_records_only
                part_file_index = 0       # Used when NOT split_by_max_records_only
                item_count_total = 0
                current_part_size_bytes = 0
                base_overhead = 2 if self.output_format == 'json' else 0
                per_item_overhead = 4 if self.output_format == 'json' else 1
                # last_progress_report_item = 0 # Removed legacy tracker var

                for item_count_total, item in enumerate(items_iterator, 1):
                    # last_progress_report_item = self._progress_report(item_count_total, last_progress_report_item) # Removed legacy call
                    tracker.update(item_count_total) # Call new tracker update

                    # Mode 1: Split strictly by max_records
                    if split_by_max_records_only:
                        chunk.append(item)
                        if len(chunk) == effective_record_limit:
                            self._write_chunk(primary_chunk_index, chunk, part_index=None, split_type='chunk')
                            primary_chunk_index += 1
                            chunk = []
                        continue

                    # Mode 2: Split by primary count with secondary limits
                    item_size = 0
                    if self.max_size_bytes:
                        try:
                            item_str = json.dumps(item)
                            item_bytes = item_str.encode('utf-8')
                            item_size = len(item_bytes)
                        except TypeError as e:
                            self.log.warning(f"Could not serialize item {item_count_total} to estimate size: {e}. Skipping size check.")
                            item_size = 0

                    # Add item to chunk
                    chunk.append(item)
                    items_in_primary_chunk += 1
                    current_part_size_bytes += item_size + (per_item_overhead if len(chunk) > 1 else 0)
                    if len(chunk) == 1:
                        current_part_size_bytes = base_overhead + item_size # Correct size for first item

                    # Determine if split is needed
                    part_split_needed = False
                    primary_split_needed = False
                    item_to_carry_over = None

                    # Check secondary limits
                    if self.max_records and len(chunk) == self.max_records:
                        self.log.debug(f"Part record limit ({self.max_records}) reached for chunk {primary_chunk_index}, part {part_file_index}.")
                        part_split_needed = True
                    elif self.max_size_bytes and current_part_size_bytes > self.max_size_bytes and len(chunk) > 1:
                        self.log.debug(f"Part size limit (~{self.max_size_bytes / (1024*1024):.2f}MB) reached for chunk {primary_chunk_index}, part {part_file_index}.")
                        part_split_needed = True
                        item_to_carry_over = chunk.pop()
                        items_in_primary_chunk -= 1
                        try:
                            carry_bytes = json.dumps(item_to_carry_over).encode('utf-8')
                            current_part_size_bytes -= (len(carry_bytes) + per_item_overhead)
                        except TypeError:
                            self.log.warning("Could not re-encode carried over item for size adjustment.")

                    # Check primary limit
                    if items_in_primary_chunk == self.count:
                        self.log.debug(f"Primary count limit ({self.count}) reached for chunk {primary_chunk_index}.")
                        primary_split_needed = True
                        part_split_needed = False # Primary takes precedence

                    # Perform splits if needed
                    if part_split_needed or primary_split_needed:
                        data_to_write = chunk if not item_to_carry_over else chunk[:-1] # Don't write carried-over item yet
                        if part_split_needed and not primary_split_needed:
                            self.log.debug(f"Writing part {part_file_index} for chunk {primary_chunk_index} due to secondary limit.")
                        elif primary_split_needed:
                            self.log.debug(f"Writing final part {part_file_index} for chunk {primary_chunk_index} due to primary limit.")

                        if data_to_write:
                            self._write_chunk(primary_chunk_index, data_to_write, part_index=part_file_index, split_type='chunk')
                        else:
                            self.log.warning(f"Skipping write for chunk {primary_chunk_index} part {part_file_index} as there is no data to write (likely due to carry-over). ")

                        # Reset for next part/chunk
                        chunk = []
                        current_part_size_bytes = base_overhead # Start with base overhead
                        part_file_index += 1 # Increment part index after writing

                        if item_to_carry_over:
                            chunk.append(item_to_carry_over)
                            items_in_primary_chunk += 1 # Re-add count for carried over
                            # Recalculate size for the carried-over item
                            try:
                                item_str = json.dumps(item_to_carry_over)
                                item_bytes = item_str.encode('utf-8')
                                item_size = len(item_bytes)
                            except TypeError: item_size = 0 # Fallback
                            current_part_size_bytes += item_size
                            item_to_carry_over = None # Clear carried item

                        if primary_split_needed:
                            primary_chunk_index += 1
                            items_in_primary_chunk = 0
                            part_file_index = 0 # Reset part index for new primary chunk
                            # Reset chunk and size again if it was just populated by carry-over
                            if chunk: # If carry-over happened
                                 chunk = []
                                 current_part_size_bytes = base_overhead
                                 items_in_primary_chunk = 0

                # Write any remaining data after the loop
                if chunk:
                    if split_by_max_records_only:
                         self._write_chunk(primary_chunk_index, chunk, part_index=None, split_type='chunk')
                    else:
                        # Use the current primary_chunk_index and part_file_index for the last file
                         self._write_chunk(primary_chunk_index, chunk, part_index=part_file_index, split_type='chunk')

            tracker.finalize() # Call finalize after loop
            return True # Indicate success

        except FileNotFoundError:
            self.log.error(f"Error: Input file '{self.input_file}' not found.")
            return False
        except ijson.JSONError as e:
            line, col = getattr(e, 'lineno', None), getattr(e, 'colno', None)
            line_col_str = f" around line {line}, column {col}" if line is not None and col is not None else ""
            self.log.error(f"Error parsing JSON{line_col_str}: {e}.")
            return False
        except (IOError, OSError) as e:
            self.log.error(f"File system error during count splitting: {e}")
            return False
        except MemoryError:
            self.log.error("Memory error during count splitting.")
            return False
        except Exception as e:
            self.log.exception("An unexpected error occurred during count splitting:")
            return False


class SizeSplitter(SplitterBase):
    """Splits JSON array/objects based on approximate size."""

    def __init__(self, size, **kwargs):
        # Special handling for size: parse it in the base class init
        # We expect max_size to be passed here as 'size' for consistency with CLI args
        if 'max_size' in kwargs:
            log.warning("Both 'size' and 'max_size' provided to SizeSplitter; using 'size'.")
            kwargs['max_size'] = size # Ensure base class gets the primary value
        else:
             kwargs['max_size'] = size # Pass size as max_size to base

        super().__init__(**kwargs)

        # Primary limit is the size provided
        self.primary_size_limit_bytes = self.max_size_bytes
        if self.primary_size_limit_bytes is None:
             # This should have been caught by base class or earlier validation
             raise ValueError("SizeSplitter requires a valid size argument.")

        # Secondary limit is max_records
        self.secondary_record_limit = self.max_records

        # Reset max_size_bytes in base class context as it's now the *primary* limit for this splitter
        # We don't have a tertiary size limit :)
        self.max_size_bytes = None # Clear secondary size limit from base perspective

        # For clarity in SizeSplitter, refer to primary limit directly
        self.size = self.primary_size_limit_bytes

    def split(self):
        self.log.info(f"Splitting '{self.input_file}' at path '{self.path}' primarily by size={self.max_size_str} (~{self.size / (1024*1024):.2f} MB)...")
        if self.secondary_record_limit:
            self.log.info(f"  Secondary limit: Max {self.secondary_record_limit} records per file part.")

        # Initialize Progress Tracker
        tracker = ProgressTracker(logger=self.log, report_interval=self._report_interval)

        try:
            with open(self.input_file, 'rb') as f:
                items_iterator = ijson.items(f, self.path)
                chunk = []
                chunk_index = 0
                item_count_total = 0
                current_chunk_size_bytes = 0
                # Rough estimate of overhead: [] for JSON, newlines for JSONL
                base_overhead = 2 if self.output_format == 'json' else 0
                # Rough estimate per item: ',' for JSON, newline for JSONL
                per_item_overhead = 4 if self.output_format == 'json' else 1
                # last_progress_report_item = 0 # Removed legacy tracker var

                for item_count_total, item in enumerate(items_iterator, 1):
                    # last_progress_report_item = self._progress_report(item_count_total, last_progress_report_item) # Removed legacy call
                    tracker.update(item_count_total) # Call new tracker update

                    # Calculate item size
                    item_size = 0
                    try:
                        # Serialize item to estimate size
                        # Using separators=(',', ':') for slightly smaller size, closer to file size
                        item_str = json.dumps(item, separators=(',', ':'))
                        item_bytes = item_str.encode('utf-8')
                        item_size = len(item_bytes)
                    except TypeError as e:
                        self.log.warning(f"Could not serialize item {item_count_total} to estimate size: {e}. Skipping size check for split.")
                        # Treat as 0 size for splitting logic, but still add to chunk
                        item_size = 0

                    # Determine if adding this item exceeds limits
                    potential_next_size = current_chunk_size_bytes + item_size + (per_item_overhead if chunk else 0)
                    exceeds_primary_size = potential_next_size > self.size and len(chunk) > 0
                    exceeds_secondary_records = self.secondary_record_limit and (len(chunk) + 1) > self.secondary_record_limit

                    # Split if necessary *before* adding the current item
                    if exceeds_primary_size or exceeds_secondary_records:
                        if chunk: # Only write if there's something in the current chunk
                            reason = "size limit" if exceeds_primary_size else "record limit"
                            self.log.debug(f"Writing chunk {chunk_index} due to {reason} ({len(chunk)} items, ~{current_chunk_size_bytes / (1024*1024):.2f} MB)...")
                            self._write_chunk(chunk_index, chunk, split_type='chunk')
                            chunk = []
                            current_chunk_size_bytes = base_overhead # Reset size
                            chunk_index += 1
                        else:
                            # This happens if a single item exceeds the size limit
                            self.log.warning(f"Item {item_count_total} alone (size ~{item_size / (1024*1024):.2f} MB) may exceed the target chunk size of {self.size / (1024*1024):.2f} MB. Writing it to its own file.")
                            # We will add it below and potentially write it immediately if it also hits record limit
                            pass

                    # Add the current item to the (potentially new) chunk
                    chunk.append(item)
                    # Update size: add item size and overhead if it's not the first item
                    current_chunk_size_bytes += item_size + (per_item_overhead if len(chunk) > 1 else 0)
                    # Correct size if it's the very first item in the chunk
                    if len(chunk) == 1:
                        current_chunk_size_bytes = base_overhead + item_size

                    # Special case: If the *first* item added also hits the secondary record limit (limit is 1)
                    if len(chunk) == 1 and self.secondary_record_limit == 1:
                         self.log.debug(f"Writing chunk {chunk_index} due to record limit=1.")
                         self._write_chunk(chunk_index, chunk, split_type='chunk')
                         chunk = []
                         current_chunk_size_bytes = base_overhead
                         chunk_index += 1


                # Write any remaining items after the loop
                if chunk:
                     self.log.debug(f"Writing final chunk {chunk_index} ({len(chunk)} items, ~{current_chunk_size_bytes / (1024*1024):.2f} MB)...")
                     self._write_chunk(chunk_index, chunk, split_type='chunk')

            tracker.finalize() # Call finalize after loop
            return True # Indicate success

        except ijson.JSONError as e:
            self.log.error(f"Invalid JSON encountered in '{self.input_file}' at path '{self.path}': {e}")
            return False
        except (IOError, OSError) as e:
            self.log.error(f"File system error during size splitting: {e}")
            return False
        except MemoryError:
            self.log.error("Memory error during size splitting.")
            return False
        except Exception as e:
            self.log.exception("An unexpected error occurred during size splitting:")
            return False


class KeySplitter(SplitterBase):
    """Splits JSON objects based on the value of a specified key."""
    def __init__(self, key_name, on_missing_key='group', on_invalid_item='warn', **kwargs):
        # Key splitting forces jsonl
        output_format = kwargs.get('output_format', 'jsonl')
        if output_format == 'json':
             log.warning("Key-based splitting enforces JSON Lines ('jsonl'). Overriding format.")
             kwargs['output_format'] = 'jsonl'

        super().__init__(**kwargs)
        self.key_name = key_name
        self.on_missing_key = on_missing_key
        self.on_invalid_item = on_invalid_item
        if not self.key_name:
            raise ValueError("Key name cannot be empty for key splitting.")

        # Key splitter specific defaults/logic
        self.output_format = 'jsonl' # Enforce
        self.file_format_extension = 'jsonl'
        # Override default filename format if not provided or unsuitable
        if not self.filename_format or '{index:04d}' in self.filename_format:
             default_key_format = "{prefix}_key_{index}{part}.{ext}"
             if self.filename_format and self.filename_format != default_key_format:
                  self.log.debug(f"Using default filename format for key splitting: '{default_key_format}'")
             self.filename_format = default_key_format

    def split(self):
        self.log.info(f"Splitting '{self.input_file}' at path '{self.path}' by key '{self.key_name}'...")
        self.log.info(f"  Output format forced to: {self.output_format}")

        # File cache: Maps sanitized key value -> open file handle
        file_cache = LRUCache(maxsize=MAX_OPEN_FILES_KEY_SPLIT)
        # Track parts per key: Maps sanitized key value -> current part index
        key_part_indices = {}
        # Track records/size per *open file*: Maps filename -> (record_count, approx_bytes)
        file_stats = {}

        # Initialize Progress Tracker
        tracker = ProgressTracker(logger=self.log, report_interval=self._report_interval)

        items_processed = 0
        items_written = 0
        items_skipped_missing_key = 0
        items_skipped_invalid = 0
        missing_key_file_handle = None
        missing_key_filename = None
        missing_key_part_index = 0
        missing_key_stats = {'count': 0, 'bytes': 0}
        success_flag = True # Assume success initially
        # last_progress_report_item = 0 # Removed legacy var

        try:
            with open(self.input_file, 'rb') as f:
                items_iterator = ijson.items(f, self.path)

                for items_processed, item in enumerate(items_iterator, 1):
                    # last_progress_report_item = self._progress_report(items_processed, last_progress_report_item) # Removed legacy call
                    tracker.update(items_processed) # Call new tracker update

                    # Validate item type (must be dict-like for key access)
                    if not isinstance(item, dict):
                        msg = f"Item {items_processed} at path '{self.path}' is not an object (type: {type(item)})."
                        if self.on_invalid_item == 'error':
                            self.log.error(msg)
                            # Set failure flag and break loop on error
                            success_flag = False
                            break
                        elif self.on_invalid_item == 'skip':
                            self.log.debug(f"Skipping: {msg}"); continue
                        else: # warn
                            self.log.warning(f"{msg} Skipping key check."); continue

                    key_value_original = "[unknown]" # For logging
                    try:
                        key_value_original = item.get(self.key_name)
                        sanitized_value = None
                        should_skip_item = False

                        if key_value_original is None:
                            if self.on_missing_key == 'error':
                                self.log.error(f"Key '{self.key_name}' not found in item {items_processed}.")
                                # Set failure flag and break loop on error
                                success_flag = False
                                break
                            elif self.on_missing_key == 'skip':
                                self.log.debug(f"Skipping item {items_processed}: Key '{self.key_name}' missing.")
                                should_skip_item = True
                            else: # group
                                sanitized_value = "__missing_key__"
                                self.log.debug(f"Item {items_processed}: Key missing, grouping as '{sanitized_value}'.")
                        elif isinstance(key_value_original, (dict, list)):
                            complex_type = type(key_value_original).__name__
                            sanitized_value = f"__complex_type_{sanitize_filename(complex_type)}__"
                            self.log.warning(f"Key '{self.key_name}' in item {items_processed} is complex ({complex_type}). Grouping as '{sanitized_value}'.")
                        else:
                            sanitized_value = sanitize_filename(key_value_original)
                            self.log.debug(f"Item {items_processed}: Key '{key_value_original}' sanitized to '{sanitized_value}'.")

                        if should_skip_item: continue
                        if sanitized_value is None: # Should not happen normally
                             self.log.error(f"Internal error: Sanitized value is None for item {items_processed}. Skipping.")
                             continue

                        # LRU Cache Logic
                        state = None
                        if sanitized_value in file_cache:
                            state = file_cache[sanitized_value]
                            self.log.debug(f"Cache hit for key '{sanitized_value}'.")
                        else:
                            self.log.debug(f"Cache miss for key '{sanitized_value}'.")
                            if len(file_cache) >= MAX_OPEN_FILES_KEY_SPLIT:
                                evicted_key, evicted_state = file_cache.popitem()
                                self.log.debug(f"Cache full. Evicting state for key '{evicted_key}'.")
                                try:
                                    handle = evicted_state.get('handle')
                                    if handle and not handle.closed:
                                        self.log.debug(f"Closing evicted file handle for key '{evicted_key}', part {evicted_state.get('part', '?')}.")
                                        handle.close()
                                except IOError as e:
                                    self.log.warning(f"Error closing evicted file for key '{evicted_key}': {e}")

                            mode = 'a' if sanitized_value in key_part_indices else 'w'
                            self.log.debug(f"Key '{sanitized_value}' mode: '{mode}'.")
                            state = {'handle': None, 'count': 0, 'size': 0, 'part': 0, 'mode': mode}
                            # Don't add to key_part_indices until open succeeds
                            # Don't add state to file_stats until open succeeds

                        # Serialize item
                        item_size = 0
                        item_str = None
                        try:
                            item_str = json.dumps(item)
                            if self.max_size_bytes:
                                item_bytes = item_str.encode('utf-8')
                                item_size = len(item_bytes)
                        except TypeError as e:
                            self.log.warning(f"Could not serialize item {items_processed} (key: {sanitized_value}): {e}. Skipping.")
                            continue

                        # Check if split is needed BEFORE adding
                        potential_new_count = state['count'] + 1
                        potential_new_size = state['size'] + item_size + 1
                        needs_new_part = False
                        split_reason = ""

                        if state['count'] > 0: # Only split if the part already has items
                            if self.max_records and potential_new_count > self.max_records:
                                needs_new_part = True
                                split_reason = f"record limit ({self.max_records})"
                            elif self.max_size_bytes and potential_new_size > self.max_size_bytes:
                                needs_new_part = True
                                split_reason = f"size limit (~{self.max_size_bytes / (1024*1024):.2f}MB)"

                        current_handle = state.get('handle')
                        if needs_new_part:
                            self.log.debug(f"Split needed for key '{sanitized_value}' part {state['part']} due to {split_reason}. Closing file.")
                            try:
                                if current_handle and not current_handle.closed:
                                    current_handle.close()
                            except IOError as e:
                                self.log.warning(f"Error closing file for key '{sanitized_value}', part {state['part']}: {e}")
                            state['part'] += 1
                            state['count'] = 0
                            state['size'] = 0
                            state['handle'] = None # Mark handle as needing reopening
                            state['mode'] = 'a' # Subsequent parts always append
                            self.log.debug(f"Starting new part {state['part']} for key '{sanitized_value}'.")

                        # Open file if needed
                        if state.get('handle') is None or state['handle'].closed:
                            # --- Refactored: Let _write_chunk handle filename generation and opening ---
                            # Determine mode for the first write to this key/part combination
                            open_mode = state.get('mode', 'a') # Should be 'w' for part 0 of a new key

                            # Attempt to open the file via _write_chunk logic indirectly
                            # We need a mechanism to open/get the handle without writing the item yet.
                            # OR: Simplify - just open directly here, remove complex _write_chunk call for filename

                            part_suffix = f"_part_{state['part']:04d}" if state['part'] > 0 else ""
                            format_args = {
                                'prefix': self.output_prefix, 'type': 'key',
                                'index': sanitized_value, 'part': part_suffix,
                                'ext': self.file_format_extension
                            }
                            try:
                                # Use the filename format resolution logic from _write_chunk
                                current_format = self.filename_format
                                if not current_format: # Use default if None
                                    current_format = "{prefix}_key_{index}{part}.{ext}"
                                elif '{index:04d}' in current_format: # Basic check for wrong format type
                                    current_format = "{prefix}_key_{index}{part}.{ext}"
                                # Apply formatting (handle potential :04d for keys)
                                temp_format = current_format.replace("{index:04d}", "{index}")
                                output_filename = temp_format.format(**format_args)

                                basename = os.path.basename(output_filename)
                                if not basename or '/' in basename or '\\' in basename:
                                    raise ValueError(f"Generated filename '{output_filename}' invalid.")

                            except (KeyError, ValueError) as e:
                                self.log.error(f"Error applying filename format '{self.filename_format}': {e}. Using fallback.")
                                fallback_part_suffix = f"_part_{state['part']:04d}" if state['part'] > 0 else ""
                                output_filename = f"{self.output_prefix}_key_{sanitized_value}{fallback_part_suffix}.{self.file_format_extension}"
                            except Exception as e:
                                 self.log.error(f"Unexpected error formatting filename: {e}. Using fallback.")
                                 fallback_part_suffix = f"_part_{state['part']:04d}" if state['part'] > 0 else ""
                                 output_filename = f"{self.output_prefix}_key_{sanitized_value}{fallback_part_suffix}.{self.file_format_extension}"

                            # Track file before attempting to open
                            self.created_files_set.add(output_filename)

                            self.log.info(f"  Opening file ({open_mode}): {output_filename}")
                            try:
                                output_dir = os.path.dirname(output_filename)
                                if output_dir:
                                    os.makedirs(output_dir, exist_ok=True)

                                new_handle = open(output_filename, open_mode, encoding='utf-8')
                                state['handle'] = new_handle
                                file_cache[sanitized_value] = state # Add/update cache *after* successful open
                                if sanitized_value not in key_part_indices:
                                    key_part_indices[sanitized_value] = 0
                            except IOError as e:
                                self.log.error(f"Failed to open file '{output_filename}' for key '{sanitized_value}': {e}. Skipping item.")
                                state['handle'] = None # Ensure handle is None on failure
                                if sanitized_value in file_cache: del file_cache[sanitized_value]
                                continue # Skip to next item
                            except Exception as e: # Catch other potential errors like permission issues
                                 self.log.exception(f"Failed to open file '{output_filename}' for key '{sanitized_value}':")
                                 state['handle'] = None
                                 if sanitized_value in file_cache: del file_cache[sanitized_value]
                                 continue

                        # Write item
                        current_handle = state.get('handle') # Re-get handle
                        if current_handle and not current_handle.closed:
                            try:
                                current_handle.write(item_str + '\n')
                                # Update state AFTER write
                                if needs_new_part: # Just started a new part for this item
                                    state['count'] = 1
                                    state['size'] = item_size
                                else:
                                    state['count'] = potential_new_count
                                    state['size'] = potential_new_size
                            except IOError as e:
                                self.log.error(f"Failed to write to file for key '{sanitized_value}': {e}. Closing handle.")
                                try:
                                    if current_handle: current_handle.close()
                                except IOError: pass
                                state['handle'] = None
                                if sanitized_value in file_cache: del file_cache[sanitized_value]
                                continue # Skip this item
                        else:
                            self.log.error(f"Internal Error: Handle invalid for key '{sanitized_value}' before write. Skipping.")
                            continue

                    except (TypeError, ValueError) as e:
                        self.log.error(f"Error processing item {items_processed} (key value: '{key_value_original}'): {e}. Skipping.")
                        continue
                    except MemoryError:
                        self.log.error(f"Memory error processing item {items_processed}. Attempting to continue.")
                        continue
                    except Exception as e:
                        self.log.exception(f"Unexpected error processing item {items_processed} (key: '{key_value_original}'):")
                        continue

            # End of main processing loop (inside try block)
            self.log.info("Finished processing input file stream.")

            # Final log messages and return should happen *before* exception handlers
            if items_written > 0:
                 self.log.info(f"Key splitting finished successfully.")
                 self.log.info(f"  Total items read from path: {items_processed}")
                 self.log.info(f"  Items written to files: {items_written}")
                 # Add counts for skipped items
            else:
                 # Check if items were processed but none written (e.g., all skipped/errors)
                 if items_processed > 0:
                     self.log.warning(f"Key splitting finished, but no items were written.")
                     self.log.info(f"  Total items read from path: {items_processed}")
                 else:
                     self.log.info(f"Key splitting finished. No items found at the specified path.")

            tracker.finalize() # Call finalize before returning success
            # success_flag = True # Moved initialization before try block

        except FileNotFoundError:
            self.log.error(f"Error: Input file '{self.input_file}' not found.")
            success_flag = False # Set failure flag
        except ijson.JSONError as e:
            line, col = getattr(e, 'lineno', None), getattr(e, 'colno', None)
            line_col_str = f" around line {line}, column {col}" if line is not None and col is not None else ""
            self.log.error(f"Error parsing JSON{line_col_str}: {e}.")
            success_flag = False
        except (IOError, OSError) as e:
            self.log.error(f"File system error during key splitting: {e}")
            success_flag = False
        except MemoryError:
            self.log.error("Memory error during key splitting setup or loop.")
            success_flag = False
        except Exception as e:
            self.log.exception("An unexpected error occurred during key splitting:")
            success_flag = False
        finally:
            # This block *always* executes, ensuring files are closed
            self.log.info("Closing remaining open files...")
            closed_count = 0
            keys_to_clear = list(file_cache.keys())
            for key in keys_to_clear: # Iterate over keys to allow cache modification
                 state = file_cache.pop(key, None) # Remove from cache
                 if state:
                     try:
                         handle = state.get('handle')
                         if handle and not handle.closed:
                             self.log.debug(f"Closing file for key '{key}' part {state.get('part','?')}")
                             handle.close()
                             closed_count += 1
                     except IOError as e:
                         self.log.warning(f"Error closing file for key '{key}': {e}")
                     except Exception as e:
                          self.log.warning(f"Unexpected error closing file for key '{key}': {e}")
            file_cache.clear()
            self.log.info(f"Closed {closed_count} files during cleanup.")

        # Return the success status determined in try/except blocks
        if not success_flag:
             log.error("Splitting process failed or terminated early.")
        return success_flag

    def _get_or_open_file(self, sanitized_key, part_index, file_cache, file_stats):
        """Gets existing handle from cache or opens a new file part."""
        # Check cache first (using a combined key? No, handle is enough)
        # Caller should manage removing from cache if closed.

        # Construct filename
        # This duplicates logic from split() - needs refactoring maybe
        part_suffix = f"_part_{part_index:04d}" if part_index > 0 else ""
        format_args = {
            'prefix': self.output_prefix, 'type': 'key',
            'index': sanitized_key, 'part': part_suffix,
            'ext': self.output_format # Should be jsonl
        }
        try:
            # Use the filename format resolution logic
            current_format = self.filename_format
            if not current_format: # Use default if None
                current_format = "{prefix}_key_{index}{part}.{ext}"
            elif '{index:04d}' in current_format: # Basic check for wrong format type
                current_format = "{prefix}_key_{index}{part}.{ext}"
            # Apply formatting (handle potential :04d for keys)
            temp_format = current_format.replace("{index:04d}", "{index}")
            output_filename = temp_format.format(**format_args)

            basename = os.path.basename(output_filename)
            if not basename or '/' in basename or '\\' in basename:
                raise ValueError(f"Generated filename '{output_filename}' invalid.")

        except (KeyError, ValueError) as e:
            self.log.error(f"Error applying filename format '{self.filename_format}': {e}. Using fallback.")
            fallback_part_suffix = f"_part_{part_index:04d}" if part_index > 0 else ""
            output_filename = f"{self.output_prefix}_key_{sanitized_key}{fallback_part_suffix}.{self.output_format}"
        except Exception as e:
                self.log.error(f"Unexpected error formatting filename: {e}. Using fallback.")
                fallback_part_suffix = f"_part_{part_index:04d}" if part_index > 0 else ""
                output_filename = f"{self.output_prefix}_key_{sanitized_key}{fallback_part_suffix}.{self.output_format}"


        # Track file before attempting to open
        self.created_files_set.add(output_filename)

        open_mode = 'w' if part_index == 0 else 'a'
        self.log.info(f"  Opening file ({open_mode}): {output_filename}")
        try:
            output_dir = os.path.dirname(output_filename)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)

            handle = open(output_filename, open_mode, encoding='utf-8')

            # Initialize stats for the *new* file handle
            if output_filename not in file_stats:
                 file_stats[output_filename] = {'count': 0, 'bytes': 0}

            # Add handle to cache (caller should remove if needed)
            # Note: We might overwrite an existing entry if LRU didn't evict?
            # Consider if cache key should be (sanitized_key, part_index) ?
            # Sticking with sanitized_key for now, assuming caller handles part logic.
            file_cache[sanitized_key] = handle

            return handle, output_filename

        except IOError as e:
            self.log.error(f"Failed to open file '{output_filename}' for key '{sanitized_key}': {e}. Skipping item.")
        except Exception as e:
            self.log.exception(f"Failed to open file '{output_filename}' for key '{sanitized_key}':")

        return None, None # Indicate failure 