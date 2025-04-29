import json
import logging
import ijson
import re
import os
from abc import ABC, abstractmethod
from datetime import datetime
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError

# Assuming utils.py will contain parse_size
from .utils import parse_size 

# Placeholder for parse_size until utils.py is implemented
# def parse_size(size_str):
#     """Parses a size string (e.g., '10MB', '200KB') into bytes."""
#     size_str = str(size_str).strip().upper()
#     match = re.match(r'^(\d+(\.\d+)?)\s*([KMGT]?B)?$', size_str)
#     if not match:
#         raise ValueError(f"Invalid size format: {size_str}")
#
#     value = float(match.group(1))
#     unit = match.group(3)
#
#     factors = {'B': 1, 'KB': 1024, 'MB': 1024**2, 'GB': 1024**3, 'TB': 1024**4}
#     
#     if unit is None: # Assume bytes if no unit
#         unit = 'B'
#     elif unit not in factors: # Handle units like K, M, G, T without B
#          unit += 'B'
#          if unit not in factors:
#              raise ValueError(f"Invalid size unit in: {size_str}")
#
#     return int(value * factors[unit])


class SplitterBase(ABC):
    """Base class for different splitting strategies."""
    DEFAULT_FILENAME_FORMAT = "{base_name}_{type}_{index:04d}{part}.{ext}"

    def __init__(self, 
                 blob_service_client: BlobServiceClient,
                 input_blob_path: str,
                 output_container: str,
                 output_blob_prefix: str = "",
                 base_name: str = "chunk", 
                 path: str = "", 
                 output_format: str = "json",
                 max_records: int | None = None, 
                 max_size: str | None = None,
                 filename_format: str | None = None, 
                 verbose: bool = False,
                 report_interval: int = 10000,
                 **kwargs): # Accept extra args for subclasses
        
        self.blob_service_client = blob_service_client
        self.input_blob_path = input_blob_path
        self.output_container = output_container
        self.output_blob_prefix = output_blob_prefix.strip('/') if output_blob_prefix else ""
        self.base_name = base_name
        self.path = path if path else "item" # Default to 'item' if path is empty, as ijson requires a prefix
        self.output_format = output_format.lower()
        self.max_records_secondary = max_records # Secondary limit
        self.max_size_secondary_bytes = parse_size(max_size) if max_size else None # Secondary limit
        self.filename_format = filename_format or self.DEFAULT_FILENAME_FORMAT
        self.verbose = verbose
        self.report_interval = report_interval
        
        self.total_items_processed = 0
        self.output_blobs_created = []
        self.start_time = None

        if self.output_format not in ['json', 'jsonl']:
            raise ValueError("output_format must be 'json' or 'jsonl'")
            
        if '.' not in self.path and self.path != 'item':
             logging.warning(f"JSON path '{self.path}' might be too broad. Consider a more specific path like 'item' or 'data.records.item'.")

        try:
            # Validate input blob exists early
            container_name, blob_name = self.input_blob_path.split('/', 1)
            input_blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            input_blob_client.get_blob_properties()
            logging.info(f"Input blob validated: {self.input_blob_path}")
        except (ValueError, IndexError):
             raise ValueError(f"Invalid input_blob_path format: {self.input_blob_path}. Expected 'container/blobname'.")
        except ResourceNotFoundError:
             raise FileNotFoundError(f"Input blob not found: {self.input_blob_path}")
        except Exception as e:
             logging.error(f"Error accessing input blob {self.input_blob_path}: {e}")
             raise

    def _log_progress(self, item_count, force=False):
        """Logs progress periodically."""
        if self.verbose and (item_count % self.report_interval == 0 or force):
            elapsed_time = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
            rate = item_count / elapsed_time if elapsed_time > 0 else 0
            logging.info(f"Processed {item_count} items... ({rate:.2f} items/sec)")

    def _write_chunk(self, primary_index, chunk_data, part_index=None, split_type='chunk', key_value=None):
        """Writes a chunk of data to a blob in the output container."""
        if not chunk_data:
            logging.warning("Attempted to write an empty chunk. Skipping.")
            return None

        ext = 'jsonl' if self.output_format == 'jsonl' else 'json'
        part_str = f"_part{part_index:03d}" if part_index is not None else ""
        key_str = f"_{key_value}" if key_value is not None else "" # Use sanitized key directly

        # Prepare format arguments
        format_args = {
            'base_name': self.base_name,
            'type': split_type,
            'index': primary_index,
            'part': part_str,
            'key': key_str, # Keep original key if needed, but use sanitized for filename
            'ext': ext
        }

        try:
            formatted_basename = self.filename_format.format(**format_args)
            # Basic sanitization for blob names
            formatted_basename = re.sub(r'[<>:"/\\|?*%\']', '_', formatted_basename) 
            # Ensure filename doesn't exceed Azure limits (1024 chars), reserving space for prefix
            max_len = 1024 - len(self.output_blob_prefix) -1 # -1 for the '/'
            if len(formatted_basename) > max_len:
                 # Truncate intelligently if possible, keeping extension
                 name, dot, extension = formatted_basename.rpartition('.')
                 name = name[:max_len - len(dot) - len(extension)]
                 formatted_basename = f"{name}{dot}{extension}"
                 logging.warning(f"Output blob name truncated due to length limit: {formatted_basename}")

        except KeyError as e:
            logging.error(f"Invalid key '{e}' in filename_format string: '{self.filename_format}'. Using default format.")
            self.filename_format = self.DEFAULT_FILENAME_FORMAT
            formatted_basename = self.filename_format.format(**format_args)
            formatted_basename = re.sub(r'[<>:"/\\|?*%\']', '_', formatted_basename) # Sanitize again


        # Construct full blob path with prefix
        output_blob_path = f"{self.output_blob_prefix}/{formatted_basename}" if self.output_blob_prefix else formatted_basename
        
        logging.info(f"Writing chunk to {self.output_container}/{output_blob_path} ({len(chunk_data)} items)")

        try:
            # Get blob client for the output
            output_blob_client = self.blob_service_client.get_blob_client(
                container=self.output_container,
                blob=output_blob_path
            )
            
            # Serialize the data
            content_type = "application/json"
            if self.output_format == 'jsonl':
                # Ensure each item is a valid JSON object before joining
                valid_items = []
                for item in chunk_data:
                    try:
                        valid_items.append(json.dumps(item))
                    except TypeError as te:
                         logging.warning(f"Skipping non-serializable item during JSONL creation: {te}. Item: {str(item)[:100]}...") # Log only snippet
                serialized_data = "\n".join(valid_items).encode('utf-8')
                content_type = "application/x-jsonlines" 
            else:  # json
                try:
                    serialized_data = json.dumps(chunk_data, indent=4).encode('utf-8')
                except TypeError as te:
                    logging.error(f"Failed to serialize chunk to JSON: {te}. Chunk data sample: {str(chunk_data)[:200]}...")
                    # Decide how to handle - skip chunk? raise error? For now, log and skip write.
                    return None

            # Define content settings
            content_settings = ContentSettings(content_type=content_type)

            # Upload the blob
            output_blob_client.upload_blob(serialized_data, overwrite=True, content_settings=content_settings)
            self.output_blobs_created.append(output_blob_path)
            return output_blob_path

        except Exception as e:
            logging.exception(f"Error writing chunk to blob {self.output_container}/{output_blob_path}: {e}")
            return None # Indicate failure

    def _stream_items(self):
        """Generator to stream items from the input blob using ijson."""
        container_name, blob_name = self.input_blob_path.split('/', 1)
        input_blob_client = self.blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name
        )
        
        logging.info(f"Starting download stream for {self.input_blob_path}...")
        try:
            with input_blob_client.download_blob() as blob_stream:
                 logging.info(f"Streaming JSON items from path '{self.path}'...")
                 # Use content_as_bytes with use_float=True for potential performance gain with ijson C backend
                 items_iterator = ijson.items(blob_stream.content_as_bytes(), self.path, use_float=True) 
                 for item in items_iterator:
                     yield item
        except ijson.JSONError as je:
             logging.error(f"Invalid JSON structure in blob {self.input_blob_path} at path '{self.path}': {je}")
             raise # Re-raise as the file is fundamentally flawed for processing
        except Exception as e:
             logging.exception(f"Error streaming blob content from {self.input_blob_path}: {e}")
             raise # Re-raise critical error

    @abstractmethod
    def split(self):
        """Abstract method to be implemented by subclasses."""
        pass

    def run(self):
        """Runs the splitting process and logs results."""
        self.start_time = datetime.now()
        logging.info(f"Starting split process for {self.input_blob_path}...")
        logging.info(f"Strategy: {type(self).__name__}, Output Container: {self.output_container}, Prefix: '{self.output_blob_prefix}'")
        
        success = False
        try:
            self.split()
            success = True # Assume success if no exceptions were raised
        except Exception as e:
            logging.exception(f"Splitting failed for {self.input_blob_path}: {e}")
            success = False # Explicitly mark as failed
        finally:
            end_time = datetime.now()
            duration = (end_time - self.start_time).total_seconds()
            
            self._log_progress(self.total_items_processed, force=True) # Log final count

            logging.info(f"Split process finished in {duration:.2f} seconds.")
            logging.info(f"Total items processed: {self.total_items_processed}")
            logging.info(f"Output blobs created ({len(self.output_blobs_created)}): {self.output_container}/{self.output_blob_prefix if self.output_blob_prefix else ''}")
            
            if not success:
                 logging.error(f"Splitting process for {self.input_blob_path} encountered errors.")

        return success


class CountSplitter(SplitterBase):
    """Splits JSON array items based on a maximum count per blob."""
    def __init__(self, count_per_blob: int, **kwargs):
        super().__init__(**kwargs)
        if not isinstance(count_per_blob, int) or count_per_blob <= 0:
            raise ValueError("count_per_blob must be a positive integer.")
        self.count_per_blob = count_per_blob
        logging.info(f"CountSplitter initialized: Max {self.count_per_blob} items per blob.")

    def split(self):
        chunk_data = []
        chunk_index = 1 # Start chunk numbering at 1
        self.total_items_processed = 0

        for item in self._stream_items():
            self.total_items_processed += 1
            chunk_data.append(item)
            
            # Check primary condition (count)
            if len(chunk_data) >= self.count_per_blob:
                self._write_chunk(chunk_index, chunk_data, split_type='count')
                chunk_data = []
                chunk_index += 1
            
            # Check secondary conditions (max_records/max_size) - applied after primary check
            elif self.max_records_secondary and len(chunk_data) >= self.max_records_secondary:
                 logging.info(f"Secondary limit hit: max_records ({self.max_records_secondary}). Writing chunk {chunk_index}.")
                 self._write_chunk(chunk_index, chunk_data, split_type='count')
                 chunk_data = []
                 chunk_index += 1
            elif self.max_size_secondary_bytes:
                 # Estimate size - can be expensive, do it less often or only when near limit?
                 # Simple estimate for now:
                 current_size = sum(len(json.dumps(i).encode('utf-8')) for i in chunk_data)
                 if current_size >= self.max_size_secondary_bytes:
                     logging.info(f"Secondary limit hit: max_size ({self.max_size_secondary_bytes} bytes). Writing chunk {chunk_index}.")
                     self._write_chunk(chunk_index, chunk_data, split_type='count')
                     chunk_data = []
                     chunk_index += 1

            self._log_progress(self.total_items_processed)

        # Write any remaining data
        if chunk_data:
            self._write_chunk(chunk_index, chunk_data, split_type='count')


class SizeSplitter(SplitterBase):
    """Splits JSON array items based on a maximum blob size."""
    def __init__(self, size_per_blob: str, **kwargs):
        super().__init__(**kwargs)
        self.max_size_bytes = parse_size(size_per_blob)
        if self.max_size_bytes <= 0:
             raise ValueError("size_per_blob must result in a positive byte value.")
        logging.info(f"SizeSplitter initialized: Max {self.max_size_bytes} bytes per blob.")


    def split(self):
        chunk_data = []
        current_chunk_size = 0
        chunk_index = 1
        self.total_items_processed = 0

        # Estimate overhead (JSON list brackets, commas, newlines for jsonl)
        # This is rough, actual size depends on content.
        json_overhead = 2 # Account for [ and ]
        jsonl_overhead = 0 

        for item in self._stream_items():
            self.total_items_processed += 1
            
            # Estimate item size
            try:
                 item_str = json.dumps(item)
                 item_size = len(item_str.encode('utf-8'))
            except TypeError:
                 logging.warning(f"Skipping non-serializable item at index {self.total_items_processed}.")
                 continue # Skip this item

            # Add comma/newline size if not the first item in chunk
            separator_size = 0
            if chunk_data:
                separator_size = 1 # Comma for JSON, newline for JSONL

            # Check if adding this item exceeds the limit
            if chunk_data and (current_chunk_size + item_size + separator_size + (json_overhead if self.output_format=='json' else jsonl_overhead)) > self.max_size_bytes:
                # Write the current chunk before adding the new item
                self._write_chunk(chunk_index, chunk_data, split_type='size')
                chunk_data = []
                current_chunk_size = 0
                chunk_index += 1
            
            # Add item to chunk
            chunk_data.append(item)
            current_chunk_size += item_size + separator_size
            
             # Check secondary conditions (max_records) - applied after adding item
            if self.max_records_secondary and len(chunk_data) >= self.max_records_secondary:
                 logging.info(f"Secondary limit hit: max_records ({self.max_records_secondary}). Writing chunk {chunk_index}.")
                 self._write_chunk(chunk_index, chunk_data, split_type='size')
                 chunk_data = []
                 current_chunk_size = 0
                 chunk_index += 1
            # No need to check secondary size limit here as it's the primary one

            self._log_progress(self.total_items_processed)

        # Write any remaining data
        if chunk_data:
            self._write_chunk(chunk_index, chunk_data, split_type='size')


class KeySplitter(SplitterBase):
    """Splits JSON array items based on the value of a specified key."""
    # Simple LRU cache might be too basic if memory is tight and keys are many.
    # For now, use a dict as per the plan, but add warnings/limits.
    MAX_BUFFERED_KEYS = 10000 # Add a safety limit

    def __init__(self, 
                 split_key: str, 
                 on_missing_key: str = "group", # group, skip, error
                 on_invalid_item: str = "warn", # warn, skip, error
                 **kwargs):
        super().__init__(**kwargs)
        if not split_key:
            raise ValueError("split_key cannot be empty.")
        self.split_key = split_key
        self.on_missing_key = on_missing_key.lower()
        self.on_invalid_item = on_invalid_item.lower()
        
        if self.on_missing_key not in ['group', 'skip', 'error']:
            raise ValueError("on_missing_key must be 'group', 'skip', or 'error'")
        if self.on_invalid_item not in ['warn', 'skip', 'error']:
            raise ValueError("on_invalid_item must be 'warn', 'skip', or 'error'")

        logging.info(f"KeySplitter initialized: Splitting by key '{self.split_key}'.")
        logging.info(f"Missing key behavior: {self.on_missing_key}. Invalid item behavior: {self.on_invalid_item}")
        
        # Track buffer memory roughly - this is very approximate
        self.estimated_total_buffer_size = 0
        # Add a config for max total buffer size later if needed e.g., 500MB
        self.MAX_TOTAL_BUFFER_BYTES = 500 * 1024 * 1024 


    def _get_key_value(self, item, item_index):
        """Extracts the key value from an item."""
        if not isinstance(item, dict):
            if self.on_invalid_item == 'error':
                raise TypeError(f"Item at index {item_index} is not a dictionary (found {type(item)}), cannot extract key '{self.split_key}'.")
            elif self.on_invalid_item == 'skip':
                logging.warning(f"Skipping item at index {item_index}: Not a dictionary (found {type(item)}).")
                return None
            else: # warn
                logging.warning(f"Item at index {item_index} is not a dictionary (found {type(item)}). Treating as missing key.")
                return '__MISSING_KEY__' # Use placeholder

        key_value = item.get(self.split_key)

        if key_value is None:
            if self.on_missing_key == 'error':
                raise KeyError(f"Key '{self.split_key}' not found in item at index {item_index}.")
            elif self.on_missing_key == 'skip':
                logging.warning(f"Skipping item at index {item_index}: Key '{self.split_key}' not found.")
                return None
            else: # group
                return '__MISSING_KEY__'
        
        # Sanitize key for use in filenames (simple version)
        sanitized_key = str(key_value)
        sanitized_key = re.sub(r'\s+', '_', sanitized_key) # Replace whitespace
        sanitized_key = re.sub(r'[^\w\-.]', '', sanitized_key) # Remove invalid chars
        sanitized_key = sanitized_key or "__EMPTY_KEY__" # Handle empty strings after sanitization
        
        # Truncate sanitized key if excessively long for filename part
        max_key_len = 50 
        if len(sanitized_key) > max_key_len:
            sanitized_key = sanitized_key[:max_key_len]
            logging.warning(f"Sanitized key truncated to '{sanitized_key}' due to length.")

        return sanitized_key

    def _flush_buffer(self, buffers, key_to_flush):
        """Writes a specific key's buffer to a blob and resets it."""
        if key_to_flush in buffers and buffers[key_to_flush]['items']:
            buffer = buffers[key_to_flush]
            logging.debug(f"Flushing buffer for key '{key_to_flush}' (Part {buffer['part']}, {len(buffer['items'])} items, {buffer['size']} bytes)")
            self._write_chunk(
                buffer['part'], # Use part number as the primary index for key splitting filenames
                buffer['items'], 
                split_type='key', 
                key_value=key_to_flush # Pass sanitized key for filename generation
            )
            # Update buffer state
            buffer['part'] += 1
            buffer['items'] = []
            self.estimated_total_buffer_size -= buffer['size'] # Decrement estimated size
            buffer['size'] = 0
        else:
             logging.warning(f"Attempted to flush non-existent or empty buffer for key '{key_to_flush}'.")


    def split(self):
        # Memory buffers instead of file handles
        buffers = {}  # sanitized_key -> {'items': [], 'size': 0, 'part': 0}
        self.total_items_processed = 0
        self.estimated_total_buffer_size = 0

        for item in self._stream_items():
            self.total_items_processed += 1
            
            sanitized_key = self._get_key_value(item, self.total_items_processed)
            if sanitized_key is None: # Skip item based on configuration
                continue 

            # Get or create buffer for this key
            if sanitized_key not in buffers:
                 # Safety check: Limit number of distinct keys buffered simultaneously
                 if len(buffers) >= self.MAX_BUFFERED_KEYS:
                     # Strategy: Flush the oldest/largest buffer? For now, error out.
                     logging.error(f"Maximum number of buffered keys ({self.MAX_BUFFERED_KEYS}) reached. Consider increasing limit or checking data cardinality.")
                     # Optionally, implement an eviction strategy here (e.g., flush largest buffer)
                     # Example: Find largest buffer and flush it
                     # largest_key = max(buffers, key=lambda k: buffers[k]['size'])
                     # self._flush_buffer(buffers, largest_key)
                     # Fallback: Raise error or log and potentially drop data
                     raise MemoryError(f"Exceeded maximum buffered keys limit ({self.MAX_BUFFERED_KEYS}).")

                 buffers[sanitized_key] = {'items': [], 'size': 0, 'part': 1} # Start part index at 1
            
            buffer = buffers[sanitized_key]
            
            # Add item to buffer
            buffer['items'].append(item)
            
            # Update estimated size (approximate)
            try:
                item_str = json.dumps(item)
                item_size = len(item_str.encode('utf-8')) + (1 if self.output_format == 'jsonl' else 0) # +1 for potential newline
                buffer['size'] += item_size
                self.estimated_total_buffer_size += item_size
            except TypeError:
                 logging.warning(f"Could not estimate size for non-serializable item with key '{sanitized_key}'. Buffer size may be inaccurate.")
                 # We added the item, so it should still be flushed later, but size check might fail.


            # Check if buffer needs to be flushed based on primary limits (count/size if specified)
            # Note: The plan used max_records/max_size as flush triggers here. These are the *secondary* limits in the base class.
            # Let's clarify: KeySplitter *itself* doesn't have a primary count/size limit, it groups by key.
            # Flushing should happen based on secondary limits OR memory pressure.
            should_flush = False
            if self.max_records_secondary and len(buffer['items']) >= self.max_records_secondary:
                logging.debug(f"Key '{sanitized_key}' buffer hit secondary record limit ({self.max_records_secondary}).")
                should_flush = True
            elif self.max_size_secondary_bytes and buffer['size'] >= self.max_size_secondary_bytes:
                logging.debug(f"Key '{sanitized_key}' buffer hit secondary size limit ({self.max_size_secondary_bytes} bytes).")
                should_flush = True
                
            if should_flush:
                self._flush_buffer(buffers, sanitized_key)

            # Check for overall memory pressure - very basic check
            if self.estimated_total_buffer_size > self.MAX_TOTAL_BUFFER_BYTES:
                 logging.warning(f"Estimated total buffer size ({self.estimated_total_buffer_size} bytes) exceeds limit ({self.MAX_TOTAL_BUFFER_BYTES} bytes). Flushing largest buffer.")
                 # Find largest buffer and flush it
                 if buffers: # Ensure buffers is not empty
                    largest_key = max(buffers, key=lambda k: buffers.get(k, {'size': -1})['size']) # Safe get
                    self._flush_buffer(buffers, largest_key)
                 else:
                     logging.error("Memory pressure detected but no buffers found to flush.")


            self._log_progress(self.total_items_processed)
    
        # Flush any remaining buffers
        logging.info(f"Flushing remaining {len(buffers)} key buffers...")
        # Iterate over keys to avoid modifying dict during iteration if flush fails
        keys_to_flush = list(buffers.keys()) 
        for key in keys_to_flush:
             if buffers[key]['items']: # Check again in case it was flushed by memory pressure logic
                self._flush_buffer(buffers, key)
        logging.info("Finished flushing remaining buffers.") 