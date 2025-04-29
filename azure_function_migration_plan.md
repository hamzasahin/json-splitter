# Azure Function Migration Plan for JSON Splitter

## 1. Overview

This document outlines a comprehensive plan to migrate the JSON Splitter tool from a local Python application to an Azure Function triggered by Event Grid events when new JSON files are uploaded to Azure Blob Storage. The function will process large JSON files in a memory-efficient manner and split them into smaller, more manageable chunks following the same strategies as the original tool (by count, size, or key).

## 2. Architecture

### 2.1. Architecture Diagram

```
┌─────────────────┐     ┌───────────────┐     ┌────────────────────┐     ┌─────────────────┐
│                 │     │               │     │                    │     │                 │
│  Input Blob     │────▶│  Event Grid   │────▶│  Azure Function    │────▶│  Output Blob    │
│  Container      │     │  System Topic │     │  (Premium Plan)    │     │  Container      │
│                 │     │               │     │                    │     │                 │
└─────────────────┘     └───────────────┘     └────────────────────┘     └─────────────────┘
```

### 2.2. Component Details

1. **Azure Blob Storage (Input Container)**
   - Stores the original large JSON files
   - Acts as the event source

2. **Event Grid System Topic**
   - Monitors the input container for `Microsoft.Storage.BlobCreated` events
   - Filters events by file extension (`.json`)
   - Routes matching events to the Azure Function

3. **Azure Function (Python)**
   - Runs in a Premium or Dedicated App Service Plan (for extended runtime and memory)
   - Uses an Event Grid trigger to respond to blob creation events
   - Processes the JSON file using the refactored splitter logic
   - Outputs the split files to the output container

4. **Azure Blob Storage (Output Container)**
   - Stores the resulting split JSON/JSONL files
   - Files are organized using the same naming conventions as the original tool

## 3. Azure Resources Required

### 3.1. Storage Account
   - **Name**: `jsonsplitterstorage` (or appropriate name following naming conventions)
   - **Performance Tier**: Standard
   - **Replication**: LRS (or GRS based on redundancy requirements)
   - **Access Tier**: Hot
   - **Containers**:
     - `input`: For source JSON files
     - `output`: For split result files

### 3.2. Event Grid System Topic
   - **Source**: Storage account
   - **Event Types**: `Microsoft.Storage.BlobCreated`
   - **Filters**:
     - Subject ends with: `.json`
     - Container name: `input`

### 3.3. Function App
   - **Runtime Stack**: Python 3.8 or higher
   - **Hosting Plan**: Premium (EP1 or higher) or Dedicated App Service Plan
     - Justification: Need for extended execution time (up to 60 minutes) and increased memory (at least 4GB)
   - **Region**: Same as Storage Account (to minimize latency and data transfer costs)
   - **Application Insights**: Enabled (for monitoring and debugging)

### 3.4. Key Vault (Optional)
   - For storing sensitive connection strings and credentials
   - Function can access via Managed Identity

## 4. Code Refactoring Strategy

### 4.1. Project Structure

```
JsonSplitterFunction/
├── __init__.py             # Main entry point with Event Grid trigger
├── function.json           # Function definition and bindings
├── splitters.py            # Refactored splitter classes
├── utils.py                # Utility functions (size parsing, logging, etc.)
├── host.json               # Host configuration
├── local.settings.json     # Local settings (for development)
└── requirements.txt        # Dependencies
```

### 4.2. Dependencies (requirements.txt)

```
azure-functions>=1.10.0
azure-storage-blob>=12.10.0
ijson>=3.1.4
cachetools>=5.0.0
```

### 4.3. Detailed Code Refactoring

#### 4.3.1. SplitterBase Class Modifications

Convert the base class to work with blob storage:

```python
class SplitterBase:
    def __init__(self, 
                 blob_service_client,  # Replace input_file with service client
                 input_blob_path,      # Path to the input blob
                 output_container,     # Output container name
                 output_blob_prefix,   # Optional prefix (e.g., "processed/")
                 base_name, 
                 path, 
                 output_format,
                 max_records=None, 
                 max_size=None,
                 filename_format=None, 
                 verbose=False,
                 report_interval=10000,
                 **kwargs):
        
        self.blob_service_client = blob_service_client
        self.input_blob_path = input_blob_path
        self.output_container = output_container
        self.output_blob_prefix = output_blob_prefix or ""
        # Keep other parameters similar to original
        # ...
        
    def _write_chunk(self, primary_index, chunk_data, part_index=None, split_type='chunk', key_value=None):
        """Writes a chunk of data to a blob in the output container"""
        # Generate blob name based on format string (similar to file naming logic)
        # ...
        
        # Construct full blob path with prefix
        output_blob_path = f"{self.output_blob_prefix}{formatted_basename}"
        
        # Get blob client for the output
        output_blob_client = self.blob_service_client.get_blob_client(
            container=self.output_container,
            blob=output_blob_path
        )
        
        # Serialize the data
        if self.output_format == 'jsonl':
            serialized_data = "\n".join([json.dumps(item) for item in chunk_data])
        else:  # json
            serialized_data = json.dumps(chunk_data, indent=4)
            
        # Upload the blob
        output_blob_client.upload_blob(serialized_data, overwrite=True)
        
        return output_blob_path  # Return the path to the created blob
```

#### 4.3.2. CountSplitter and SizeSplitter Modifications

The main changes in these classes involve:

1. Reading from blob storage instead of a local file:

```python
def split(self):
    # ...
    try:
        # Get blob client for input
        container_name, blob_name = self.input_blob_path.split('/', 1)
        input_blob_client = self.blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name
        )
        
        # Download the blob as a stream
        with input_blob_client.download_blob() as blob_stream:
            # Use ijson to stream-parse the JSON
            items_iterator = ijson.items(blob_stream.content_as_stream(), self.path)
            
            # Rest of the original processing loop remains largely the same
            # ...
```

2. The chunk writing changes to use the refactored `_write_chunk` method.

#### 4.3.3. KeySplitter Modifications (Memory Buffer Approach)

The key splitter changes are more substantial, as we need to replace the file handle-based approach with an in-memory buffer:

```python
def split(self):
    # Memory buffers instead of file handles
    buffers = {}  # key -> {'items': [], 'size': 0, 'part': 0}
    
    # Get blob client for input
    container_name, blob_name = self.input_blob_path.split('/', 1)
    input_blob_client = self.blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name
    )
    
    with input_blob_client.download_blob() as blob_stream:
        items_iterator = ijson.items(blob_stream.content_as_stream(), self.path)
        
        for item_count, item in enumerate(items_iterator, 1):
            # Process item, determine key value
            # ...
            
            # Get or create buffer for this key
            if sanitized_key not in buffers:
                buffers[sanitized_key] = {'items': [], 'size': 0, 'part': 0}
            
            buffer = buffers[sanitized_key]
            
            # Add item to buffer
            buffer['items'].append(item)
            
            # Update estimated size
            item_str = json.dumps(item)
            item_size = len(item_str.encode('utf-8')) + 1  # +1 for newline
            buffer['size'] += item_size
            
            # Check if buffer needs to be flushed
            should_flush = False
            if self.max_records and len(buffer['items']) >= self.max_records:
                should_flush = True
            elif self.max_size_bytes and buffer['size'] > self.max_size_bytes:
                should_flush = True
                
            if should_flush:
                # Write current buffer to blob
                self._write_chunk(
                    buffer['part'], 
                    buffer['items'], 
                    split_type='key', 
                    key_value=sanitized_key
                )
                
                # Update buffer state
                buffer['part'] += 1
                buffer['items'] = []
                buffer['size'] = 0
    
    # Flush any remaining buffers
    for key, buffer in buffers.items():
        if buffer['items']:
            self._write_chunk(
                buffer['part'],
                buffer['items'],
                split_type='key',
                key_value=key
            )
```

#### 4.3.4. Azure Function Main Entry Point

```python
import logging
import azure.functions as func
import os
import json
from azure.storage.blob import BlobServiceClient

from .splitters import CountSplitter, SizeSplitter, KeySplitter
from .utils import parse_size

def main(event: func.EventGridEvent):
    """
    Azure Function entry point triggered by Event Grid.
    Processes JSON file uploads to the input container.
    """
    try:
        # Get event data
        event_data = event.get_json()
        logging.info(f"Received event: {event.subject}")
        
        # Extract the blob URL from the event
        input_url = event_data.get('url')
        if not input_url:
            # Alternative approach if URL not directly available
            storage_account = event_data.get('storageAccount')
            container_name = event_data.get('containerName')
            blob_name = event_data.get('blobName')
            
            if not all([storage_account, container_name, blob_name]):
                raise ValueError("Could not determine source blob details from event")
                
            input_blob_path = f"{container_name}/{blob_name}"
        else:
            # Parse URL to get container/blob path
            # Format: https://{storage_account}.blob.core.windows.net/{container}/{blob}
            url_parts = input_url.split('.blob.core.windows.net/', 1)
            if len(url_parts) != 2:
                raise ValueError(f"Invalid blob URL format: {input_url}")
            input_blob_path = url_parts[1]  # container/blob
        
        # Get configuration from environment variables
        connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        split_strategy = os.environ["SPLIT_STRATEGY"]
        split_value = os.environ["SPLIT_VALUE"]
        json_path = os.environ.get("JSON_PATH", "")
        output_container = os.environ["OUTPUT_CONTAINER_NAME"]
        output_prefix = os.environ.get("OUTPUT_BLOB_PREFIX", "")
        base_name = os.environ.get("BASE_NAME", "chunk")
        output_format = os.environ.get("OUTPUT_FORMAT", "json")
        
        # Optional parameters
        max_records = os.environ.get("MAX_RECORDS_PER_PART")
        if max_records:
            max_records = int(max_records)
            
        max_size = os.environ.get("MAX_SIZE_PER_PART")
        filename_format = os.environ.get("FILENAME_FORMAT")
        report_interval = int(os.environ.get("REPORT_INTERVAL", "10000"))
        verbose = os.environ.get("VERBOSE", "false").lower() == "true"
        
        # Key splitter specific options
        on_missing_key = os.environ.get("ON_MISSING_KEY", "group")
        on_invalid_item = os.environ.get("ON_INVALID_ITEM", "warn")
        
        # Initialize blob service client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Initialize the appropriate splitter
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
        
        if split_strategy == "count":
            try:
                count_value = int(split_value)
                splitter = CountSplitter(count_value, **common_args)
            except ValueError:
                raise ValueError(f"Invalid count value: {split_value}. Must be an integer.")
                
        elif split_strategy == "size":
            splitter = SizeSplitter(split_value, **common_args)
            
        elif split_strategy == "key":
            splitter = KeySplitter(
                split_value, 
                on_missing_key=on_missing_key,
                on_invalid_item=on_invalid_item,
                **common_args
            )
        else:
            raise ValueError(f"Invalid split strategy: {split_strategy}. Must be 'count', 'size', or 'key'.")
        
        # Execute the splitting operation
        success = splitter.split()
        
        if success:
            logging.info(f"Successfully split blob {input_blob_path}")
        else:
            logging.error(f"Failed to split blob {input_blob_path}")
            
    except Exception as e:
        logging.exception(f"Error processing blob: {str(e)}")
        raise
```

### 4.4. Function.json

```json
{
  "scriptFile": "__init__.py",
  "bindings": [
    {
      "type": "eventGridTrigger",
      "name": "event",
      "direction": "in"
    }
  ]
}
```

## 5. Configuration Management

### 5.1. Application Settings (Environment Variables)

| Setting Name | Description | Example Value |
|--------------|-------------|---------------|
| AZURE_STORAGE_CONNECTION_STRING | Connection string for the Storage Account | `DefaultEndpointsProtocol=https;AccountName=...` |
| SPLIT_STRATEGY | The splitting strategy to use | `count`, `size`, or `key` |
| SPLIT_VALUE | The primary value for the chosen strategy | `10000`, `50MB`, or `customer_id` |
| JSON_PATH | Dot-notation path to the array in the JSON | `data.records.item` |
| OUTPUT_CONTAINER_NAME | Output container name | `output` |
| OUTPUT_BLOB_PREFIX | Optional prefix for output blobs | `processed/` |
| BASE_NAME | Base name for output files | `chunk` |
| OUTPUT_FORMAT | Output format | `json` or `jsonl` |
| MAX_RECORDS_PER_PART | Optional secondary limit by record count | `5000` |
| MAX_SIZE_PER_PART | Optional secondary limit by size | `10MB` |
| FILENAME_FORMAT | Custom filename format | `{base_name}_{type}_{index:04d}{part}.{ext}` |
| ON_MISSING_KEY | (Key splitting) Behavior when key missing | `group`, `skip`, or `error` |
| ON_INVALID_ITEM | (Key splitting) Behavior for invalid items | `warn`, `skip`, or `error` |
| REPORT_INTERVAL | Progress reporting interval | `10000` |
| VERBOSE | Enable verbose logging | `true` or `false` |

### 5.2. Host.json Configuration

```json
{
  "version": "2.0",
  "logging": {
    "applicationInsights": {
      "samplingSettings": {
        "isEnabled": true,
        "excludedTypes": "Request"
      }
    },
    "logLevel": {
      "default": "Information",
      "Function": "Information"
    }
  },
  "functionTimeout": "01:00:00",
  "extensions": {
    "eventGrid": {}
  }
}
```

## 6. Implementation Steps

### 6.1. Local Development Setup

1. **Install Prerequisites**
   - Azure Functions Core Tools v4.x
   - Python 3.8 or higher
   - Visual Studio Code with Azure Functions extension
   - Azure CLI

2. **Create Local Function Project**
   ```bash
   func init JsonSplitterFunction --python
   cd JsonSplitterFunction
   func new --template "Event Grid trigger" --name JsonSplitterFunction
   ```

3. **Copy Modified Code**
   - Create/modify `__init__.py`, `splitters.py`, and `utils.py` as described
   - Update `requirements.txt` with dependencies
   - Configure `local.settings.json` for local testing

4. **Local Testing with Azurite**
   - Start Azurite for local storage emulation
   - Upload test JSON files
   - Trigger function manually with sample Event Grid payload

### 6.2. Azure Resource Provisioning

1. **Create Azure Resources (Azure CLI or Portal)**
   ```bash
   # Create Resource Group
   az group create --name json-splitter-rg --location eastus

   # Create Storage Account
   az storage account create --name jsonsplitterstorage --resource-group json-splitter-rg --location eastus --sku Standard_LRS

   # Create Storage Containers
   az storage container create --name input --account-name jsonsplitterstorage
   az storage container create --name output --account-name jsonsplitterstorage

   # Create Premium Function App
   az functionapp plan create --resource-group json-splitter-rg --name json-splitter-plan --sku EP1 --location eastus
   
   az functionapp create --resource-group json-splitter-rg --name json-splitter-func --storage-account jsonsplitterstorage --plan json-splitter-plan --runtime python --runtime-version 3.9 --functions-version 4
   ```

2. **Configure Event Grid**
   ```bash
   # Create System Topic
   az eventgrid system-topic create --name json-splitter-topic --resource-group json-splitter-rg --source-resource-id /subscriptions/{subscription-id}/resourceGroups/json-splitter-rg/providers/Microsoft.Storage/storageAccounts/jsonsplitterstorage --topic-type Microsoft.Storage.StorageAccounts

   # Create Event Subscription
   az eventgrid event-subscription create --name json-splitter-sub --source-resource-id /subscriptions/{subscription-id}/resourceGroups/json-splitter-rg/providers/Microsoft.Storage/storageAccounts/jsonsplitterstorage --subject-begins-with /blobServices/default/containers/input --subject-ends-with .json --endpoint /subscriptions/{subscription-id}/resourceGroups/json-splitter-rg/providers/Microsoft.Web/sites/json-splitter-func/functions/JsonSplitterFunction --endpoint-type azurefunction
   ```

3. **Configure Application Settings**
   ```bash
   # Get storage connection string
   CONNECTION_STRING=$(az storage account show-connection-string --name jsonsplitterstorage --resource-group json-splitter-rg --output tsv)

   # Set Function App settings
   az functionapp config appsettings set --name json-splitter-func --resource-group json-splitter-rg --settings "AZURE_STORAGE_CONNECTION_STRING=$CONNECTION_STRING" "SPLIT_STRATEGY=count" "SPLIT_VALUE=10000" "JSON_PATH=item" "OUTPUT_CONTAINER_NAME=output" "BASE_NAME=chunk" "OUTPUT_FORMAT=json" "REPORT_INTERVAL=10000"
   ```

### 6.3. Deployment

1. **Deploy Function using VS Code**
   - Use Azure Functions extension
   - Right-click project folder → Deploy to Function App → Select target

2. **Alternatively, Deploy using Azure Functions Core Tools**
   ```bash
   func azure functionapp publish json-splitter-func
   ```

### 6.4. Verify Deployment

1. **Check Function Status**
   ```bash
   az functionapp show --name json-splitter-func --resource-group json-splitter-rg
   ```

2. **Upload Test JSON File**
   ```bash
   az storage blob upload --account-name jsonsplitterstorage --container-name input --name test.json --file /path/to/test.json
   ```

3. **Check Function Execution Logs**
   - Use Azure Portal → Function App → Functions → JsonSplitterFunction → Monitor
   - Or use Application Insights

4. **Verify Output Blobs**
   ```bash
   az storage blob list --account-name jsonsplitterstorage --container-name output --output table
   ```

## 7. Testing Strategy

### 7.1. Unit Testing

1. **Mock Blob Storage**
   - Use `azure-storage-blob` testing fixtures or mocks
   - Test splitting logic with in-memory streams

2. **Test Each Splitter Class**
   - Test `CountSplitter` with various count values
   - Test `SizeSplitter` with different size specifications
   - Test `KeySplitter` with different key scenarios

### 7.2. Integration Testing

1. **Local End-to-End Testing**
   - Use Azurite storage emulator
   - Run function locally
   - Upload test files to local emulator
   - Verify output blobs

2. **Azure Testing**
   - Start with small files (< 1MB)
   - Progress to medium files (10-100MB)
   - Test with large files (1GB+)
   - Validate output correctness
   - Monitor execution time and memory usage

### 7.3. Stress Testing

1. **Test with Very Large Files**
   - Test the upper limits (5GB+)
   - Validate memory usage stays reasonable

2. **Test with High Cardinality Key Splitting**
   - Files with thousands of unique key values
   - Validate memory buffering strategy

3. **Concurrent Execution**
   - Upload multiple files simultaneously
   - Verify function scale-out behavior

## 8. Performance Tuning

### 8.1. Memory Optimization

1. **Buffer Size Management**
   - Periodically check total memory usage during execution
   - Consider implementing a more aggressive buffer flush strategy if memory pressure is detected
   - Add configuration option for max buffer memory

2. **Key Splitting Enhancement**
   - For extremely high cardinality keys, implement a tiered buffering strategy:
     - Tier 1: Active keys in memory (most recently used)
     - Tier 2: Flush less active keys more aggressively
   - Monitor memory usage and adjust thresholds dynamically

### 8.2. Speed Optimization

1. **Parallel Processing Options**
   - For very large files, consider using Durable Functions to split the work:
     - Orchestrator function to coordinate
     - Activity functions to process chunks in parallel
   - Implement this as a "Phase 2" enhancement if initial implementation shows performance issues

2. **Blob Storage Performance**
   - Use block blobs with appropriate block size for outputs
   - Consider using Premium Storage for high-throughput scenarios

### 8.3. Cost Optimization

1. **Storage Transaction Reduction**
   - Batch small writes where possible 
   - Use appropriate buffer sizes to minimize storage operations

2. **Execution Time Management**
   - Monitor function execution time
   - Optimize execution path for common use cases

## 9. Monitoring and Maintenance

### 9.1. Application Insights

1. **Key Metrics to Monitor**
   - Function execution time
   - Memory usage
   - Blob storage operations
   - Error rates

2. **Custom Telemetry**
   - Add custom telemetry for split operations:
     - Number of items processed
     - Number of output blobs created
     - Processing rate (items/second)

### 9.2. Alerts

1. **Set Up Alerts for**
   - Function execution failures
   - Execution timeouts
   - High memory usage (>80% of limit)
   - Error rate exceeding threshold

### 9.3. Logging Strategy

1. **Structured Logging**
   - Use consistent log formats
   - Include operation IDs for correlation
   - Log key metrics at appropriate intervals

2. **Log Retention**
   - Configure appropriate retention periods in Application Insights
   - Export critical logs to longer-term storage if needed

## 10. Future Enhancements

1. **Durable Functions Migration**
   - For extremely large files or better resiliency
   - Break the processing into multiple stages that can restart

2. **Azure Data Factory Integration**
   - Trigger the process as part of a larger data pipeline
   - Coordinate with upstream and downstream processes

3. **UI for Configuration**
   - Create a simple web UI to submit splitting jobs
   - Allow monitoring of running jobs

4. **Multiple Input Sources**
   - Support for Azure Data Lake Storage
   - Support for files via HTTP upload

## 11. Security Considerations

1. **Data Protection**
   - Ensure appropriate access controls on storage containers
   - Consider using Private Endpoints for the storage account

2. **Secrets Management**
   - Move sensitive settings to Azure Key Vault
   - Use managed identities for secure access

3. **Network Security**
   - Consider VNet integration for the Function App
   - Restrict storage account access to the function's subnet

## 12. Conclusion

This plan outlines a comprehensive approach to migrating the JSON Splitter tool to Azure Functions. By leveraging Event Grid triggers and Azure Blob Storage, we create a serverless solution that maintains the core functionality of the original tool while gaining the benefits of cloud scalability and event-driven architecture.

The proposed implementation balances performance, memory efficiency, and cloud resource optimization, with particular attention to handling very large files in a constrained serverless environment. Multiple testing strategies ensure reliability, while monitoring and alerting capabilities provide operational visibility.
