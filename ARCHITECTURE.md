# JSON Splitter Architecture Overview

This document outlines the architecture and workflow of the `json_splitter.py` script.

## 1. Purpose

The primary goal of `json_splitter.py` is to split a large JSON file, specifically targeting an array of objects within it, into multiple smaller output files based on user-defined criteria. It is designed to handle large files efficiently by using a streaming approach, thus avoiding loading the entire JSON structure into memory.

## 2. Core Components

-   **`main()`**: Entry point of the script. Determines whether to run in interactive mode or parse command-line arguments. Calls `run_interactive_mode()` or `execute_split()`.
-   **`run_interactive_mode()`**: Provides a guided, user-friendly way to collect necessary arguments through prompts.
-   **`execute_split(args)`**: The central orchestrator.
    -   Performs initial validation of arguments (input file existence, output directory writability, value parsing).
    -   Sets up logging level.
    -   Determines the correct splitting function (`split_by_count`, `split_by_size`, or `split_by_key`) based on `args.split_by`.
    -   Prepares a `kwargs` dictionary containing specific arguments for the chosen split function.
    -   Initializes a `created_files` set to track output files for cleanup.
    -   Calls the chosen split function, passing common arguments and the specific `kwargs` (including the `created_files_set`).
    -   Handles exceptions during the splitting process.
    -   If splitting fails, attempts to clean up (delete) any files listed in `created_files_set`.
    -   Logs success or failure and exits with an appropriate status code.
-   **Splitting Functions:**
    -   **`split_by_count(...)`**: Splits the input JSON array into chunks containing a specified number of items (`--value`). Supports secondary limits (`--max-records`, `--max-size`).
    -   **`split_by_size(...)`**: Splits the input JSON array into chunks where each output file is approximately a specified size (`--value`). Size is estimated by serializing items. Supports a secondary limit (`--max-records`).
    -   **`split_by_key(...)`**: Splits the input JSON array based on the value of a specified key (`--value`) found within each object. Objects with the same key value go into the same output file (or file parts if secondary limits are met). Uses an LRU cache (`open_files_cache`) to manage open file handles efficiently for high-cardinality keys. Handles missing keys and non-object items based on `--on-missing-key` and `--on-invalid-item` policies. Enforces `jsonl` output.
-   **Helper Functions:**
    -   **`_parse_size(size_str)`**: Parses human-readable size strings (e.g., "100MB", "2GB") into bytes.
    -   **`_write_chunk(...)`**: Handles the actual writing of a data chunk to an output file. Formats the filename based on `filename_format` and writes data either as a JSON array or JSON Lines (`jsonl`). Adds the filename to `created_files_set` before writing.
    -   **`_sanitize_filename(value)`**: Cleans a key value (or any string) to make it suitable for use in a filename, removing problematic characters and handling length limits.
    -   **`_prompt_with_validation(...)` & other `_validate_*` functions**: Used by the interactive mode to get and validate user input.

## 3. Workflow

1.  **Initialization**: Script starts via `main()`.
2.  **Mode Selection**: Determines CLI or Interactive mode based on `sys.argv`.
3.  **Argument Acquisition**:
    -   *CLI Mode*: `argparse` parses command-line arguments.
    -   *Interactive Mode*: `run_interactive_mode()` prompts the user for required and optional settings.
4.  **Execution (`execute_split`)**:
    -   Input arguments are validated.
    -   Logging is configured.
    -   Output directory is checked/created.
    -   The appropriate splitting function (`split_by_X`) is selected.
    -   `created_files` set is initialized.
    -   The selected splitting function is called.
5.  **Streaming & Processing (within `split_by_X` functions)**:
    -   The input JSON file is opened.
    -   `ijson.items()` creates an iterator to stream items from the specified `--path` without loading the whole file.
    -   The script iterates through items one by one.
    -   Based on the splitting mode (`count`, `size`, `key`) and secondary constraints (`max-records`, `max-size`), items are collected into chunks or assigned to key-specific files.
    -   Size estimation (if needed) involves `json.dumps()` per item.
    -   Key splitting uses an LRU cache for file handles.
6.  **Writing (`_write_chunk` / `split_by_key` direct write)**:
    -   When a chunk is complete (count/size limit reached) or an item needs writing (key mode), the target filename is generated.
    -   The filename is added to `created_files_set`.
    -   The file is opened (or retrieved from cache in key mode).
    -   Data is written in the specified `--output-format` (`json` or `jsonl`).
    -   Files might be closed and reopened (especially in key mode due to the LRU cache).
7.  **Completion/Error Handling**:
    -   After iterating through all items, any remaining data in buffers/chunks is written.
    -   The splitting function returns `True` on success or `False` on handled errors/policy stops.
    -   `execute_split` catches the return value or any exceptions.
    -   **On Failure**: Logs the error. Iterates through `created_files_set` and attempts to delete each file. Exits with status 1.
    -   **On Success**: Logs completion message. Exits with status 0.

## 4. Key Technologies & Concepts

-   **Streaming**: Uses the `ijson` library to iterate over JSON items without loading the entire file into memory, crucial for large files.
-   **Memory Management (Key Splitting)**: Employs an `LRUCache` from the `cachetools` library to limit the number of simultaneously open file handles when splitting by key, preventing resource exhaustion with many unique keys.
-   **Error Handling**: Uses specific `try...except` blocks (`IOError`, `JSONError`, `ValueError`, `MemoryError`, etc.) for robustness.
-   **File Cleanup**: Tracks attempted output filenames and tries to remove them if the script fails, preventing partial files.
-   **Modularity**: Splits logic into distinct functions for argument parsing, execution orchestration, different splitting strategies, and helper tasks.
-   **Configuration**: Offers both command-line arguments (`argparse`) and an interactive prompt mode for flexibility.
