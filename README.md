# JSON Splitter

A robust and memory-efficient Python command-line tool for splitting large or complex JSON files into smaller, manageable chunks based on various criteria. Designed to handle multi-gigabyte files without loading the entire dataset into memory.

## Problem Solved

Working with extremely large JSON files (multiple gigabytes) can be challenging:
- Loading the entire file into memory often leads to crashes or requires excessive RAM.
- Processing or importing large files into other systems can be slow or infeasible.

JSON Splitter addresses this by using a streaming approach (`ijson` library) to read the input file incrementally and split it into smaller files based on your defined rules, keeping memory usage low and consistent.

## Features

- **Streaming Processing:** Handles arbitrarily large JSON files with minimal memory usage thanks to the `ijson` library.
- **Multiple Splitting Strategies:**
    - **By Count (`--split-by count`):** Split into files containing N items each.
    - **By Approximate Size (`--split-by size`):** Split into files of approximately N megabytes/kilobytes/etc.
    *   **By Key Value (`--split-by key`):** Split items into separate files based on the value of a specified key (outputs as JSON Lines).
- **Nested Data Handling:** Process arrays or objects located deep within the JSON structure using the `--path` argument (e.g., `data.records.item`).
- **Secondary Splitting Constraints:** Apply secondary limits (`--max-records`, `--max-size`) to further subdivide chunks created by the primary strategy.
- **Configurable Error Handling:** Choose how to handle items missing a specified key (`--on-missing-key`) or items at the target path that are not valid objects (`--on-invalid-item`) when splitting by key.
- **Flexible Output Formats:** Output chunks as standard JSON arrays (`json`) or JSON Lines (`jsonl`). (*Note: Key-based splitting currently enforces `jsonl`*).
- **Customizable Filenames:** Define your own output filename patterns using the `--filename-format` option.
- **Verbose Logging:** Enable detailed debug messages using the `--verbose` (`-v`) flag for troubleshooting.

## Installation

1.  **Prerequisites:** Python 3.7+
2.  **Clone the repository (or download the source):**
    ```bash
    git clone <repository_url> # Replace with the actual repo URL
    cd json-splitter
    ```
3.  **Install dependencies:**
    It's recommended to use a virtual environment:
    ```bash
    python -m venv venv
    source venv/bin/activate # On Windows use `venv\Scripts\activate`
    pip install -r requirements.txt
    ```
    This will install the `ijson` library.

## Usage

The script is run from the command line.

### Basic Command Structure

```bash
python src/json_splitter.py <input_file> <output_prefix> --split-by <strategy> --value <split_value> --path <json_path> [options]
```

### Arguments

-   **`input_file`**: (Required) Path to the large input JSON file.
-   **`output_prefix`**: (Required) Prefix for the output files. Can include a directory path (e.g., `output/data_chunk`). The directory will be created if it doesn't exist.
-   **`--split-by`**: (Required) The primary splitting strategy. Choices:
    -   `count`: Split by item count.
    -   `size`: Split by approximate file size.
    -   `key`: Split by the value of a specific key.
-   **`--value`**: (Required) The value associated with `--split-by`:
    -   For `count`: The number of items per chunk (e.g., `10000`).
    -   For `size`: The approximate target size with units (e.g., `15MB`, `500KB`, `1GB`). Case-insensitive. Bytes assumed if no unit.
    -   For `key`: The name of the key to use for grouping (e.g., `category_id`).
-   **`--path`**: (Required) The `ijson` path to the array or objects you want to split. Use `item` for a root-level array. Use dots (`.`) to access nested elements (e.g., `results.users.item`).
-   **`--output-format`**: (Optional) Output format. Choices: `json` (default), `jsonl`. Note: `--split-by key` currently enforces `jsonl`.
-   **`--max-records`**: (Optional) Secondary constraint: Maximum number of records per output file part.
-   **`--max-size`**: (Optional) Secondary constraint: Maximum approximate size (e.g., `50MB`) per output file part.
-   **`--on-missing-key`**: (Optional, for `--split-by key` only) Action for items missing the specified key. Choices: `group` (default - puts in `__missing_key__` file), `skip`, `error`.
-   **`--on-invalid-item`**: (Optional, for `--split-by key` only) Action for items at target `--path` that are not dictionary objects. Choices: `warn` (default - logs and skips), `skip`, `error`.
-   **`--filename-format`**: (Optional) A format string for output filenames. Default varies by split type. Available placeholders: `{prefix}`, `{type}` ('chunk' or 'key'), `{index}` (numeric index or sanitized key value), `{part}` (e.g., `_part_0001` or empty), `{ext}` ('json' or 'jsonl'). Example: `--filename-format "output_{type}_{index}.{ext}"`
-   **`-v`, `--verbose`**: (Optional) Enable detailed debug logging output.

### Examples

**1. Split by Size (15MB chunks):**
Split `large_data.json` where the array is at the root (`item`) into files roughly 15MB each, placed in the `output_chunks` directory.

```bash
python src/json_splitter.py large_data.json output_chunks/data --split-by size --value 15MB --path item
```
*Output files might look like: `output_chunks/data_chunk_0000.json`, `output_chunks/data_chunk_0001.json`, ...*

**2. Split by Count (10,000 items per chunk):**
Split `events.json` where the array is at `log_data.events.item` into JSON Lines files of 10,000 records each.

```bash
python src/json_splitter.py events.json output/events --split-by count --value 10000 --path log_data.events.item --output-format jsonl
```
*Output files might look like: `output/events_chunk_0000.jsonl`, `output/events_chunk_0001.jsonl`, ...*

**3. Split by Key (`user_id`):**
Split `user_activity.json` (array at `activities.item`) into separate JSON Lines files for each unique `user_id`. Handle missing keys by grouping them.

```bash
python src/json_splitter.py user_activity.json user_files/activity --split-by key --value user_id --path activities.item --on-missing-key group
```
*Output files might look like: `user_files/activity_key_user123.jsonl`, `user_files/activity_key_user456.jsonl`, `user_files/activity_key___missing_key__.jsonl`, ...*

**4. Split by Size with Secondary Record Limit:**
Split `products.json` (array at `catalog.item`) into ~100MB chunks, but ensure no single output file part exceeds 50,000 records.

```bash
python src/json_splitter.py products.json output/products --split-by size --value 100MB --path catalog.item --max-records 50000
```
*Output files might look like: `output/products_chunk_0000.json`, `output/products_chunk_0000_part_0001.json`, `output/products_chunk_0001.json`, ... (Parts appear if the record limit is hit before the size limit)*

**5. Custom Filenames:**
Split by count, naming files `chunk_1.json`, `chunk_2.json`, etc.

```bash
python src/json_splitter.py input.json output/file --split-by count --value 500 --path item --filename-format "{prefix}_{index}.{ext}"
```
*Output files: `output/file_1.json`, `output/file_2.json`, ... (Note: default format includes zero-padding)*


## Input Data Requirements

-   The script **requires valid JSON** input. It uses a streaming parser that will fail if the JSON syntax is incorrect.
-   If your files contain invalid characters (e.g., unescaped control characters, syntax errors), you **must pre-process** them to fix these issues *before* using this splitter. This tool does *not* clean or repair invalid JSON content.

## Output Files

-   Files are created based on the `<output_prefix>` and the chosen splitting strategy.
-   The `--filename-format` argument allows full control over naming.
-   Default naming conventions:
    -   `count`/`size`: `{prefix}_chunk_{index:04d}[_part_{part:04d}].{ext}`
    -   `key`: `{prefix}_key_{sanitized_key_value}[_part_{part:04d}].jsonl`
    -   `{index:04d}` means the numeric index is zero-padded to 4 digits.
    -   `[_part_{part:04d}]` is added only if a secondary constraint causes a split within a primary chunk/key file.
    -   `{sanitized_key_value}` is the value of the key, processed to remove characters invalid for filenames.

## Performance and Memory Usage

-   Memory usage is generally low and constant due to the streaming nature of `ijson`.
-   Splitting by `size` or `key` involves serializing items internally to estimate size, which adds some CPU overhead compared to splitting purely by `count`.
-   Splitting by `key` holds state for each unique key encountered. If your dataset has an extremely high number of unique key values (millions), memory usage could increase. A warning is issued if the count exceeds 1000 unique keys.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request. (Consider adding more specific guidelines if desired).

## License

This project is licensed under the MIT License. See the LICENSE file for details. (You may need to add a LICENSE file with the MIT license text). 