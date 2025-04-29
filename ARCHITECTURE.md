# JSON Splitter Architecture Overview

This document outlines the architecture and workflow of the `json_splitter.py` script.

## 1. Purpose

The primary goal of `json_splitter.py` is to split a large JSON file, specifically targeting an array of objects within it, into multiple smaller output files based on user-defined criteria. It is designed to handle large files efficiently by using a streaming approach, thus avoiding loading the entire JSON structure into memory.

## 2. Core Components

-   **`main.py` (`main()`)**: Entry point of the script. Determines whether to run in interactive mode or parse command-line arguments. Calls `run_interactive_mode()` or `execute_split()` from `cli.py`.
-   **`cli.py` (`run_interactive_mode()`)**: Provides a guided, user-friendly way to collect necessary arguments through prompts.
-   **`cli.py` (`execute_split(args)`)**: The central orchestrator.
    -   Performs initial validation of arguments (using functions from `utils.py` where applicable, like input file existence, output directory writability, value parsing).
    -   Sets up logging level based on verbosity.
    -   Determines the correct splitter class (`CountSplitter`, `SizeSplitter`, or `KeySplitter` from `splitters.py`) based on `args.split_by`.
    -   Prepares arguments for the chosen splitter's constructor.
    -   Initializes a `created_files` set to track output files for cleanup.
    -   Instantiates the chosen splitter class, passing common arguments and strategy-specific ones (like `count`, `size`, `key_name`, `on_missing_key`, etc.). The `created_files_set` is passed to the splitter instance.
    -   Calls the `split()` method on the splitter instance.
    -   Handles exceptions during the splitting process.
    -   If splitting fails, attempts to clean up (delete) any files listed in the `created_files_set` (which the splitter instance should have populated).
    -   Logs success or failure and exits with an appropriate status code.
-   **`splitters.py` (Splitter Classes)**:
    -   **`SplitterBase`**: Abstract base class providing common initialization (parsing `max_size`, setting up logging, storing common args like `output_dir`, `base_name`), the `_write_chunk` method, and the `split()` method interface.
    -   **`CountSplitter`**: Splits the input JSON array into chunks containing a specified number of items (`count`). Uses `ProgressTracker`. Supports secondary limits (`max_records`, `max_size`).
    -   **`SizeSplitter`**: Splits the input JSON array into chunks where each output file is approximately a specified size (`size`). Size is estimated by serializing items. Uses `ProgressTracker`. Supports a secondary limit (`max_records`).
    -   **`KeySplitter`**: Splits the input JSON array based on the value of a specified key (`key_name`) found within each object. Objects with the same key value go into the same output file (or file parts if secondary limits are met). Uses an LRU cache (`open_files_cache`, managed by `_get_or_open_file`) from `cachetools` with a fixed size (`MAX_OPEN_FILES_KEY_SPLIT`) to manage open file handles efficiently for high-cardinality keys. Uses `ProgressTracker`. Handles missing keys and non-object items based on `--on-missing-key` and `--on-invalid-item` policies. Enforces `jsonl` output.
-   **`utils.py` (Helper Functions & Classes)**:
    -   **`parse_size(size_str)`**: Parses human-readable size strings (e.g., "100MB", "2GB") into bytes.
    -   **`sanitize_filename(value)`**: Cleans a key value (or any string) to make it suitable for use in a filename, removing problematic characters and handling length limits.
    -   **`validate_inputs(...)`**: Central function for validating core arguments (file paths, split strategy, values). Used implicitly or explicitly by `execute_split` or the splitters.
    -   **`ProgressTracker`**: Class used by splitters to track the number of items processed and log progress messages periodically based on a configurable interval (`--report-interval`).
    -   **Logging Setup (`log`)**: Basic configuration for the application's logger.
-   **`splitters.py` (`_write_chunk(...)`)**: Helper method within `SplitterBase` (used by `CountSplitter` and `SizeSplitter`) that handles the actual writing of a data chunk to an output file. Constructs the full path using `os.path.join(output_dir, formatted_basename)`. Formats the basename based on `filename_format` and `base_name`. Writes data either as a pretty-printed JSON array (`indent=4`) or JSON Lines (`jsonl`). Adds the filename to the instance's `created_files_set` before writing.
-   **`cli.py` (`_prompt_with_validation(...)` & other `_validate_*` functions)**: Used by the interactive mode to get and validate user input.

## 3. Workflow

1.  **Initialization**: Script starts via `main.py` (`main()`), which calls `cli.main()`.
2.  **Argument Parser Setup**: `argparse.ArgumentParser` is configured with all options, including `--config`, `--output-dir`, `--base-name`.
3.  **Config File Loading (Optional)**:
    -   `parse_known_args` checks for `--config`.
    -   If present, the specified YAML file is loaded using `yaml.safe_load`.
    -   Loaded values are applied as defaults using `parser.set_defaults()`.
4.  **Final Argument Parsing**: `parser.parse_args()` parses the remaining command-line arguments, overriding any defaults set by the config file.
5.  **Mode Selection**: Determines CLI or Interactive mode based on `sys.argv` length and presence of `--config`.
6.  **Argument Acquisition**:
    -   *CLI Mode*: Arguments are taken directly from the parsed `args`.
    -   *Interactive Mode*: `run_interactive_mode()` prompts the user for required and optional settings, shows a summary, and asks for confirmation.
7.  **Execution (`execute_split`)**:
    -   Input arguments are validated (input file readability, output directory writability via `_validate_output_dir`).
    -   Logging is configured.
    -   Output directory is checked/created if needed.
    -   The appropriate `Splitter` class is selected.
    -   `created_files` set is initialized.
    -   The splitter instance is created (receiving `output_dir`, `base_name`, `created_files` set, etc.).
    -   The `split()` method of the instance is called.
8.  **Streaming & Processing (within `split()` methods of splitter classes)**:
    -   The input JSON file is opened.
    -   `ijson.items()` creates an iterator to stream items from the specified `--path` (dot-notation, `item` or empty for root) without loading the whole file.
    -   A `ProgressTracker` instance is initialized with the desired `--report-interval`.
    -   The script iterates through items one by one, updating the `ProgressTracker`.
    -   Based on the splitting mode (`count`, `size`, `key`) and secondary constraints (`max-records`, `max-size`), items are collected into chunks or assigned to key-specific files.
    -   Size estimation (if needed) involves `json.dumps()` per item.
    -   Key splitting uses an LRU cache for file handles via `_get_or_open_file`, using the full file path as the cache key.
9.  **Writing (`_write_chunk` or `split_by_key` direct write)**:
    -   When a chunk is complete (count/size limit reached) or an item needs writing (key mode), the target *basename* is generated using the filename format string and `base_name`.
    -   The full output path is constructed using `os.path.join(output_dir, formatted_basename)`.
    -   The filename is added to the splitter instance's `created_files_set`.
    -   The file is opened (or retrieved from cache in key mode).
    -   Data is written in the specified `--output-format` (pretty `json` or `jsonl`, with `key` mode forcing `jsonl`).
    -   Files might be closed and reopened (especially in key mode due to the LRU cache).
10. **Completion/Error Handling**:
    -   After iterating through all items, any remaining data in buffers/chunks is written.
    -   The `split()` method returns `True` on success or `False` on handled errors/policy stops.
    -   `execute_split` catches the return value or any exceptions raised during splitting.
    -   **On Failure**: Logs the error. Iterates through the `created_files_set` held by the (potentially partially successful) splitter instance and attempts to delete each file. Exits with status 1.
    -   **On Success**: Logs completion message, including final stats from the `ProgressTracker`. Exits with status 0.

## 4. Key Technologies & Concepts

-   **Streaming**: Uses the `ijson` library to iterate over JSON items without loading the entire file into memory, crucial for large files.
-   **Memory Management (Key Splitting)**: Employs an `LRUCache` from the `cachetools` library with a fixed size (`MAX_OPEN_FILES_KEY_SPLIT` constant in `splitters.py`) to limit the number of simultaneously open file handles when splitting by key, preventing resource exhaustion with many unique keys. Uses the full file path as the cache key.
-   **Progress Reporting**: Uses a `ProgressTracker` class (`utils.py`) to periodically log processing progress based on the number of items handled and a configurable interval (`--report-interval`).
-   **Error Handling**: Uses specific `try...except` blocks (`IOError`, `ijson.JSONError`, `yaml.YAMLError`, `ValueError`, `MemoryError`, etc.) for robustness.
-   **File Cleanup**: Tracks attempted output filenames within the splitter instance and `execute_split` tries to remove them if the script fails, preventing partial files.
-   **Modularity**: Splits logic into distinct modules (`cli.py`, `splitters.py`, `utils.py`) and classes/functions for argument parsing, config loading, execution orchestration, different splitting strategies, and helper tasks.
-   **Configuration**: Offers command-line arguments (`argparse`), an interactive prompt mode, and YAML configuration file loading (`--config`) via `PyYAML`. Defines clear precedence (CLI > Config File > Defaults).
-   **Output Formatting**: Provides pretty-printed JSON (`indent=4`) for standard JSON output and compact JSON Lines for `jsonl` output.
