import logging
import azure.functions as func
import os
import json
from urllib.parse import urlparse
import datetime

from azure.storage.blob import BlobServiceClient

# Relative imports for Azure Functions Python v2 model
from .splitters import CountSplitter, SizeSplitter, KeySplitter
from .utils import parse_size

# Configure logging for the function
# Logging settings in host.json will also apply
logger = logging.getLogger('JsonSplitterFunction')

def main(event: func.EventGridEvent):
    """
    Azure Function entry point triggered by Event Grid.
    Processes JSON file uploads to the input container.
    """
    start_time = datetime.datetime.now()
    logger.info(f"Python EventGrid trigger function processed event.")
    logger.info(f"Subject: {event.subject}")
    logger.info(f"Event Type: {event.event_type}")
    logger.info(f"ID: {event.id}")

    try:
        # Get event data as JSON
        event_data = event.get_json()
        logger.debug(f"Event Data: {json.dumps(event_data)}")
        
        # Extract the blob URL from the event data
        # The 'url' field is standard for Microsoft.Storage.BlobCreated events
        input_url = event_data.get('url')
        
        if not input_url:
             # Fallback/Alternative if URL is missing (less common for BlobCreated)
            blob_name = event.subject.split('/')[-1] # Often the blob name is in the subject
            # Need container name - potentially parse from subject or rely on config?
            # This path is less reliable, let's prioritize the 'url'.
            raise ValueError("Could not determine source blob URL from event data.")

        # Parse the URL to get container and blob name
        parsed_url = urlparse(input_url)
        if not parsed_url.netloc or not parsed_url.path:
            raise ValueError(f"Invalid blob URL format: {input_url}")
            
        # Path usually starts with '/', split and take parts
        path_parts = parsed_url.path.strip('/').split('/', 1)
        if len(path_parts) != 2:
             raise ValueError(f"Could not parse container/blob from URL path: {parsed_url.path}")
        input_container_name, input_blob_name = path_parts
        input_blob_path = f"{input_container_name}/{input_blob_name}" # Format: container/blob
        logger.info(f"Identified input blob: {input_blob_path}")

        # --- Configuration Loading --- 
        logger.info("Loading configuration from environment variables...")
        connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        if not connection_string:
             # Critical configuration missing
             logger.error("AZURE_STORAGE_CONNECTION_STRING environment variable not set.")
             raise ValueError("Storage connection string is missing.")
             
        split_strategy = os.environ.get("SPLIT_STRATEGY")
        split_value = os.environ.get("SPLIT_VALUE")
        json_path = os.environ.get("JSON_PATH", "") # Defaults to 'item' in splitter if empty
        output_container = os.environ.get("OUTPUT_CONTAINER_NAME")
        output_prefix = os.environ.get("OUTPUT_BLOB_PREFIX", "")
        base_name = os.environ.get("BASE_NAME", "chunk")
        output_format = os.environ.get("OUTPUT_FORMAT", "json")
        
        # Validate essential configuration
        if not split_strategy or not split_value or not output_container:
             missing = [k for k,v in {'SPLIT_STRATEGY':split_strategy, 'SPLIT_VALUE':split_value, 'OUTPUT_CONTAINER_NAME':output_container}.items() if not v]
             logger.error(f"Missing required environment variables: {missing}")
             raise ValueError(f"Missing required configuration: {missing}")

        # Optional parameters (with type conversion and validation)
        max_records = None
        try:
            max_records_str = os.environ.get("MAX_RECORDS_PER_PART")
            if max_records_str:
                max_records = int(max_records_str)
                if max_records <= 0:
                     logger.warning(f"MAX_RECORDS_PER_PART must be positive, got {max_records}. Ignoring.")
                     max_records = None
        except ValueError:
             logger.warning(f"Invalid value for MAX_RECORDS_PER_PART: '{max_records_str}'. Must be integer. Ignoring.")

        max_size = os.environ.get("MAX_SIZE_PER_PART") # Kept as string, parsed by splitter
        if max_size: # Basic validation if provided
             try:
                 parse_size(max_size)
             except ValueError as e:
                 logger.warning(f"Invalid value for MAX_SIZE_PER_PART: '{max_size}' ({e}). Ignoring.")
                 max_size = None

        filename_format = os.environ.get("FILENAME_FORMAT") # Defaults in splitter if None/Empty
        
        report_interval = 10000
        try:
             report_interval_str = os.environ.get("REPORT_INTERVAL", "10000")
             report_interval = int(report_interval_str)
             if report_interval <= 0:
                  logger.warning(f"REPORT_INTERVAL must be positive, defaulting to 10000.")
                  report_interval = 10000
        except ValueError:
             logger.warning(f"Invalid REPORT_INTERVAL '{report_interval_str}'. Defaulting to 10000.")
             report_interval = 10000

        verbose = os.environ.get("VERBOSE", "false").lower() == "true"
        
        # Key splitter specific options
        on_missing_key = os.environ.get("ON_MISSING_KEY", "group")
        on_invalid_item = os.environ.get("ON_INVALID_ITEM", "warn")
        
        logger.info(f"Configuration loaded: Strategy='{split_strategy}', Value='{split_value}', Path='{json_path or '(root/items)'}', Output='{output_container}/{output_prefix}'")
        
        # --- Initialize Blob Service Client --- 
        try:
             blob_service_client = BlobServiceClient.from_connection_string(connection_string)
             logger.info("BlobServiceClient initialized.")
        except ValueError as e:
            logger.error(f"Invalid storage connection string format: {e}")
            raise # Critical error, cannot proceed
        except Exception as e:
             logger.error(f"Failed to initialize BlobServiceClient: {e}")
             raise # Critical error

        # --- Initialize and Run Splitter --- 
        common_args = {
            "blob_service_client": blob_service_client,
            "input_blob_path": input_blob_path,
            "output_container": output_container,
            "output_blob_prefix": output_prefix,
            "base_name": base_name,
            "path": json_path,
            "output_format": output_format,
            "max_records": max_records,
            "max_size": max_size,
            "filename_format": filename_format,
            "verbose": verbose,
            "report_interval": report_interval
        }
        
        splitter = None
        split_strategy = split_strategy.lower()

        if split_strategy == "count":
            try:
                count_value = int(split_value)
                splitter = CountSplitter(count_value, **common_args)
            except ValueError:
                logger.error(f"Invalid count value: '{split_value}'. Must be an integer for 'count' strategy.")
                raise ValueError(f"Invalid count value: {split_value}")
                
        elif split_strategy == "size":
            try:
                 # Validate size format here before passing
                 parse_size(split_value) 
                 splitter = SizeSplitter(split_value, **common_args)
            except ValueError as e:
                 logger.error(f"Invalid size value: '{split_value}' ({e}). Must be a valid size format (e.g., 10MB, 500KB) for 'size' strategy.")
                 raise ValueError(f"Invalid size value: {split_value}")
            
        elif split_strategy == "key":
            if not split_value:
                 logger.error("Split value (the key name) cannot be empty for 'key' strategy.")
                 raise ValueError("Split key name cannot be empty.")
            splitter = KeySplitter(
                split_value, # The key name
                on_missing_key=on_missing_key,
                on_invalid_item=on_invalid_item,
                **common_args
            )
        else:
            logger.error(f"Invalid split strategy: '{split_strategy}'. Must be 'count', 'size', or 'key'.")
            raise ValueError(f"Invalid split strategy: {split_strategy}")
        
        # Execute the splitting operation using the run method
        logger.info(f"Executing splitter: {type(splitter).__name__}")
        success = splitter.run()
        
        if success:
            logger.info(f"Successfully completed splitting blob {input_blob_path}")
        else:
            # Errors should have been logged within splitter.run() or during setup
            logger.error(f"Splitting process for blob {input_blob_path} failed or encountered errors. Check previous logs.")
            # Raise an exception to indicate failure to the Azure Functions runtime
            raise RuntimeError(f"Splitting process failed for {input_blob_path}")
            
    except Exception as e:
        # Catch any unexpected errors during setup or execution
        logger.exception(f"Unhandled exception processing event for blob related to subject '{event.subject}': {str(e)}")
        # Re-raise the exception to ensure the function execution is marked as failed
        raise
    finally:
         end_time = datetime.datetime.now()
         duration = (end_time - start_time).total_seconds()
         logger.info(f"Function execution finished in {duration:.2f} seconds.") 