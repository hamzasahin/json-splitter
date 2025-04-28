# JSON Splitter ✨

A memory-efficient Python tool to split large JSON files into smaller chunks by count, size, or key value. Ideal for multi-gigabyte files.

## Key Features

-   **Streaming:** Handles huge files with low RAM usage (using `ijson`).
-   **Splitting Methods:** By item `count`, approximate file `size` (e.g., `15MB`), or unique `key` value.
-   **Nested Data:** Targets specific arrays/objects within JSON using `--path` (e.g., `data.records.item`).
-   **Flexible Output:** JSON or JSON Lines (`jsonl`) format (*key splitting forces `jsonl`*).
-   **Secondary Limits:** Fine-tune chunks with `--max-records` or `--max-size`.
-   **Custom Filenames:** Control output names with `--filename-format`.
-   **Interactive Mode:** Guides you through options if run without arguments.

## Installation

1.  **Prerequisites:** Python 3.7+
2.  **Clone:**
    ```bash
    git clone https://github.com/hamzasahin/json-splitter.git # Or your repo URL
    cd json-splitter
    ```
3.  **Install Dependencies (virtual env recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate # Windows: venv\Scripts\activate
    pip install -r requirements.txt
    ```

## Usage

Run from the command line or use interactive mode by running without arguments.

### Command Line

```bash
python src/json_splitter.py <input_file> <output_prefix> --split-by <strategy> --value <split_value> --path <json_path> [options]
```

**Core Arguments:**

-   `input_file`: Path to your large JSON file.
-   `output_prefix`: Path and filename prefix for output chunks (e.g., `output/data`). Directory created if needed.
-   `--split-by`: Strategy (`count`, `size`, `key`).
-   `--value`: Value for the strategy (e.g., `10000`, `15MB`, `category_id`).
-   `--path`: `ijson` path to the data to split (e.g., `item`, `results.users.item`).

**Common Options:**

-   `--output-format`: `json` (default) or `jsonl`.
-   `--max-records <N>`: Max items per output file part.
-   `--max-size <size>`: Max size (e.g., `50MB`) per output file part.
-   `--filename-format <fmt>`: Customize output filenames (see details below).
-   `-v`, `--verbose`: Enable debug logs.

*(For `--split-by key` specific options like `--on-missing-key`, see `--help`)*

### Interactive Mode

Simply run the script without any arguments to be guided through the setup:

```bash
python src/json_splitter.py
```

### Examples

**Split by Size (Most Common):**
Split `large_data.json` (array at root `item`) into ~15MB files in `output_chunks/`.

```bash
python src/json_splitter.py large_data.json output_chunks/data --split-by size --value 15MB --path item
```
*Output: `output_chunks/data_chunk_0000.json`, ...*

**Split by Count:**
Split `events.json` (array at `log.events.item`) into JSONL files of 10k items each.

```bash
python src/json_splitter.py events.json output/events --split-by count --value 10000 --path log.events.item --output-format jsonl
```
*Output: `output/events_chunk_0000.jsonl`, ...*

**Split by Key:**
Split `activity.json` (array at `data.item`) by `user_id` into separate JSONL files.

```bash
python src/json_splitter.py activity.json user_files/act --split-by key --value user_id --path data.item
```
*Output: `user_files/act_key_user123.jsonl`, ...*

## Important Notes

-   **Valid JSON Input Required:** The script needs valid JSON. Pre-process files to fix syntax errors or invalid characters first.
-   **Filename Formatting:** Use placeholders like `{prefix}`, `{type}`, `{index}` (number or key value), `{part}`, `{ext}` with `--filename-format`.
-   **Memory (Key Splitting):** Using `--split-by key` on data with millions of unique keys *can* increase memory usage. A warning is shown if >1000 unique keys are detected.

## Contributing

Issues and pull requests are welcome!

## License

MIT License (Please add a LICENSE file to the repository). 