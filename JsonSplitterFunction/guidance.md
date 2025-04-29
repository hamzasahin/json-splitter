# Using the JSON Splitter Azure Function

This guide outlines the steps to deploy and use the JSON Splitter Azure Function.

## Prerequisites

1.  **Azure Account:** You need an active Azure subscription.
2.  **Azure CLI:** Install the Azure Command-Line Interface ([Install Guide](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)). Log in using `az login`.
3.  **Azure Functions Core Tools:** Install v4.x ([Install Guide](https://docs.microsoft.com/en-us/azure/azure-functions/functions-run-local?tabs=v4%2Cwindows%2Cnode%2Cportal%2Cbash)).
4.  **Python:** Python 3.8 or higher.
5.  **Code:** This `JsonSplitterFunction` project folder.

## 1. Azure Resource Provisioning

These resources need to exist in your Azure subscription. Refer to `azure_function_migration_plan.md` (Section 6.2) for detailed CLI commands. **Replace placeholders like `{subscription-id}` and use appropriate names/locations.**

*   **Resource Group:** A container for all related resources.
    ```bash
    # Example
    az group create --name json-splitter-rg --location eastus
    ```
*   **Storage Account:** Stores input and output blobs.
    ```bash
    # Example (Use a globally unique name)
    az storage account create --name jsonsplitterstorageUNIQUE --resource-group json-splitter-rg --location eastus --sku Standard_LRS
    ```
*   **Storage Containers:** Create `input` and `output` containers within the storage account.
    ```bash
    # Example
    az storage container create --name input --account-name jsonsplitterstorageUNIQUE
    az storage container create --name output --account-name jsonsplitterstorageUNIQUE
    ```
*   **Function App:** Hosts the function code. **Choose a Premium (EP1+) or Dedicated plan** for potentially long-running splits.
    ```bash
    # Example (Premium Plan)
    az functionapp plan create --resource-group json-splitter-rg --name json-splitter-plan --sku EP1 --location eastus --is-linux # Linux recommended
    az functionapp create --resource-group json-splitter-rg --name json-splitter-funcUNIQUE --storage-account jsonsplitterstorageUNIQUE --plan json-splitter-plan --runtime python --runtime-version 3.9 --functions-version 4 --os-type Linux
    ```
*   **Event Grid System Topic & Subscription:** Connects blob creation events to the function.
    ```bash
    # Example (Replace {subscription-id}, resource group, account, function names)
    STORAGE_ID=$(az storage account show --name jsonsplitterstorageUNIQUE --resource-group json-splitter-rg --query id --output tsv)
    FUNCTION_ID=$(az functionapp function show --resource-group json-splitter-rg --name json-splitter-funcUNIQUE --function-name JsonSplitterFunction --query id --output tsv)
    
    # Create System Topic (if not automatically created by subscription)
    # az eventgrid system-topic create --name json-splitter-topic --resource-group json-splitter-rg --source $STORAGE_ID --location global --topic-type Microsoft.Storage.StorageAccounts

    # Create Event Subscription linking Storage -> Function
    az eventgrid event-subscription create \
        --name json-splitter-sub \
        --source-resource-id $STORAGE_ID \
        --endpoint $FUNCTION_ID \
        --endpoint-type azurefunction \
        --included-event-types Microsoft.Storage.BlobCreated \
        --subject-begins-with /blobServices/default/containers/input/ \
        --subject-ends-with .json
    ```

## 2. Configure Application Settings

The function reads its configuration (split strategy, paths, etc.) from Application Settings.

1.  **Get Storage Connection String:**
    ```bash
    CONNECTION_STRING=$(az storage account show-connection-string --name jsonsplitterstorageUNIQUE --resource-group json-splitter-rg --query connectionString --output tsv)
    ```
2.  **Set Function App Settings:** Update `--name` and `--resource-group`. Configure `SPLIT_STRATEGY`, `SPLIT_VALUE`, etc., as needed (referencing Section 5.1 of the plan).
    ```bash
    az functionapp config appsettings set --name json-splitter-funcUNIQUE --resource-group json-splitter-rg --settings \
        "AzureWebJobsStorage=$CONNECTION_STRING" \
        "AZURE_STORAGE_CONNECTION_STRING=$CONNECTION_STRING" \
        "SPLIT_STRATEGY=count" \
        "SPLIT_VALUE=10000" \
        "JSON_PATH=items" \
        "OUTPUT_CONTAINER_NAME=output" \
        "OUTPUT_BLOB_PREFIX=processed/" \
        "BASE_NAME=split_data" \
        "OUTPUT_FORMAT=jsonl" \
        "MAX_RECORDS_PER_PART=" \
        "MAX_SIZE_PER_PART=50MB" \
        "FILENAME_FORMAT=" \
        "ON_MISSING_KEY=group" \
        "ON_INVALID_ITEM=warn" \
        "REPORT_INTERVAL=5000" \
        "VERBOSE=true"
    ```
    *   **Note:** `AzureWebJobsStorage` is also required by the Functions runtime itself.
    *   You can also configure these settings in the Azure Portal under your Function App -> Configuration -> Application settings.

## 3. Deploy the Function Code

Navigate to the root of your project (`/Users/user/json-splitter`) in your terminal.

1.  **Install Dependencies Locally (Optional but good practice):**
    ```bash
    python -m venv .venv
    source .venv/bin/activate # Or .\.venv\Scripts\activate on Windows
    pip install -r JsonSplitterFunction/requirements.txt
    ```
2.  **Deploy using Azure Functions Core Tools:**
    ```bash
    # Make sure you are in the /Users/user/json-splitter directory
    # The tool detects the JsonSplitterFunction subfolder
    func azure functionapp publish json-splitter-funcUNIQUE
    ```
    *   Alternatively, use the Azure Functions extension in VS Code.

## 4. Trigger the Function

Simply upload a JSON file (with a `.json` extension) to the `input` container in your `jsonsplitterstorageUNIQUE` Azure Storage Account.

*   You can use the Azure Portal, Azure Storage Explorer, or Azure CLI:
    ```bash
    # Example upload
    az storage blob upload --account-name jsonsplitterstorageUNIQUE --container-name input --name myLargeFile.json --file /path/to/your/local/myLargeFile.json --auth-mode login
    ```
*   Event Grid will detect the `BlobCreated` event (filtered by `.json` and the `input` container) and trigger your deployed `JsonSplitterFunction`.

## 5. Monitor Execution

1.  **Azure Portal:**
    *   Navigate to your Function App (`json-splitter-funcUNIQUE`).
    *   Go to **Functions** -> **JsonSplitterFunction** -> **Monitor**.
    *   You can view invocation traces, logs, and successes/errors.
2.  **Application Insights:**
    *   If enabled during Function App creation (recommended), navigate to the associated Application Insights resource.
    *   Explore Logs (using Kusto Query Language - KQL), Live Metrics, Failures, and Performance.
    *   Example KQL query for traces:
        ```kql
        traces
        | where cloud_RoleName == "json-splitter-funcUNIQUE" // Function App name
        | order by timestamp desc 
        | limit 100
        ```

## 6. Verify Output

Check the `output` container (or the specified `OUTPUT_BLOB_PREFIX` within it) in your `jsonsplitterstorageUNIQUE` storage account. You should find the split JSON or JSONL files named according to the configured format.

*   Using Azure CLI:
    ```bash
    # List blobs in the output container (adjust prefix if used)
    az storage blob list --account-name jsonsplitterstorageUNIQUE --container-name output --output table --auth-mode login
    
    # List blobs within a prefix
    az storage blob list --account-name jsonsplitterstorageUNIQUE --container-name output --prefix "processed/" --output table --auth-mode login
    ```
