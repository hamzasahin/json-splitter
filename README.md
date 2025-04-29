# JSON Splitter ✨

A simple, memory-efficient Python tool to split large JSON files into smaller, manageable chunks.

## 🤔 What does it do?

It takes a large JSON file (even multiple gigabytes!) and splits the data inside (specifically, an array of objects) into multiple smaller files. You can choose *how* to split it:

-   By a specific **number of items** per file (`count`).
-   By an approximate **file size** for each file (`size`, e.g., `10MB`).
-   By the **value of a specific key** within the data (`key`, e.g., grouping all items with the same `user_id` together).

## ✅ Why use it?

-   **Handles Huge Files:** Designed for files too large to fit into memory, using efficient streaming via `ijson`.
-   **Flexible Splitting:** Choose the method that best suits your needs.
-   **Works with Nested Data:** Can target data deep within the JSON structure using a simple dot-notation path (e.g., `data.records.item`).
-   **Memory Efficient Key Splitting:** Uses an LRU (Least Recently Used) cache to limit the number of simultaneously open files when splitting by key, preventing issues with resource limits.
-   **Easy to Use:** Run via command-line or a helpful interactive mode.

## 🚀 Getting Started

### Prerequisites

-   Python 3.7 or newer

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/hamzasahin/json-splitter.git # Or your repo URL
    cd json-splitter
    ```

2.  **Set up a virtual environment (Recommended):**
    *This keeps dependencies isolated.*
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use: venv\\Scripts\\activate
    ```

3.  **Install dependencies:**
    *(Ensure `cachetools` is included if it wasn't already)*
    ```bash
    pip install -r requirements.txt
    ```

## 💻 How to Use

You have three ways to run the splitter:

### 1. Interactive Mode (Easy Start)

If you're unsure about the options, just run the script without any arguments from the project root directory:

```bash
python -m src.main
```

### 2. Command-Line Interface (CLI)

For scripting or direct control, use the command line from the project root directory:

```bash
python -m src.main <input_file> --split-by <strategy> --value <split_value> --path <json_path> [options]
```

**Core Arguments:**

| Argument        | Description                                                                 |
| :-------------- | :-------------------------------------------------------------------------- |
| `input_file`    | Path to your large input JSON file.                                         |
| `--split-by`    | How to split: `count`, `size`, or `key`.                                    |
| `--value`       | The value for the split strategy (e.g., `10000`, `50MB`, `product_id`).      |
| `--path`        | Dot-notation path to the array to split (e.g., `item`, `data.records.item`). Use `item` or leave empty for root array. |

**Common Options:**

| Option                | Description                                                                     |
| :-------------------- | :------------------------------------------------------------------------------ |
| `--config <file>`     | Path to a YAML configuration file (see *Configuration File* below).             |
| `--output-dir <dir>`  | Directory to save output files (default: current directory `.`).                  |
| `--base-name <name>`  | Base name for output files (default: `chunk`).                                  |
| `--output-format`     | `json` (default, pretty-printed) or `jsonl` (JSON Lines). *(Note: `key` split forces `jsonl`)* |
| `--max-records <N>`   | *Secondary limit:* Max number of items per output file part.                    |
| `--max-size <size>`   | *Secondary limit:* Max approximate size per output file part (e.g., `100MB`).   |
| `--filename-format`   | Customize output file names (see *Filename Formatting* below).                  |
| `--report-interval <N>`| Report progress every N items (default: 10000). Set to 0 to disable.           |
| `-v`, `--verbose`     | Show detailed debug messages.                                                   |

**Key Splitting Options:**
*(Only relevant when using `--split-by key`)*

| Option              | Description                                                                                                               |
| :------------------ | :------------------------------------------------------------------------------------------------------------------------ |
| `--on-missing-key`  | What to do if an item lacks the key: `group` (default, into `__missing_key__` file), `skip`, or `error` (stop script).      |
| `--on-invalid-item` | What to do if an item at `--path` isn't an object: `warn` (default, prints warning and skips), `skip`, or `error` (stop script). |

### Examples

**Example 1: Split by Size**

Split `large_log.json` into files of roughly 100MB each, saving them in the `log_parts/` directory. The data to split is the array found under the `events` key.

```bash
python -m src.main large_log.json --output-dir log_parts/ --base-name log --split-by size --value 100MB --path events.item
```
*Creates files like `log_parts/log_chunk_0000.json`, `log_parts/log_chunk_0001.json`, ...*
*(Note: Adjusted path to `events.item` assuming `events` is an object containing an array named `item`. Adjust if `events` itself is the array)*

**Example 2: Split by Count (JSON Lines)**

Split `user_data.json` into files containing 50,000 users each, outputting as JSON Lines into the `user_outputs/` directory.

```bash
python -m src.main user_data.json --output-dir user_outputs/ --base-name user --split-by count --value 50000 --path results.users.item --output-format jsonl
```
*Creates files like `user_outputs/user_chunk_0000.jsonl`, `user_outputs/user_chunk_0001.jsonl`, ...*

**Example 3: Split by Key**

Group items from `orders.json` based on their `customer_id`, saving files into `customer_orders/`. The items are in the root array. Output will be JSON Lines format.

```bash
# Use 'item' or empty string for root array path
python -m src.main orders.json --output-dir customer_orders/ --base-name order --split-by key --value customer_id --path item
```
*Creates files like `customer_orders/order_key_cust101.jsonl`, `customer_orders/order_key_cust456.jsonl`, ... (Note the `.jsonl` extension)*

### 3. Configuration File

You can define all splitting parameters in a YAML configuration file and load it using the `--config` option. This is useful for complex or frequently used configurations.

```bash
python -m src.main --config my_split_config.yaml
```

**Argument Precedence:**

1.  **Command-Line Arguments:** Have the highest priority and will override any settings from the config file.
2.  **Configuration File:** Values specified in the loaded YAML file override the built-in defaults.
3.  **Built-in Defaults:** Have the lowest priority (e.g., `output_dir="."`, `base_name="chunk"`).

See the `config.example.yaml` file in the repository for a template and examples of all available options.

### Filename Formatting

You can control the output filenames using `--filename-format`. Available placeholders:

-   `{base_name}`: The `--base-name` you provided (default: `chunk`).
-   `{type}`: How the split was done (`chunk` for count/size, `key` for key split).
-   `{index}`: The chunk number (e.g., `0000`, `0001`) or the sanitized key value (e.g., `user123`).
-   `{part}`: An optional part suffix (e.g., `_part_0001`) added if secondary limits (`max-records`/`max-size`) cause a split *within* a primary chunk/key.
-   `{ext}`: The file extension (`json` or `jsonl`).

**Default Format:** `{base_name}_{type}_{index:04d}{part}.{ext}` (for count/size) or `{base_name}_key_{index}{part}.{ext}` (for key).

## 💡 Good to Know

-   **Input Must Be Valid JSON:** The script expects a syntactically correct JSON file. If you have issues, validate your input file first.
-   **Memory Use with Many Keys:** Splitting by `key` on data with millions of unique keys uses an LRU cache to manage open file handles, limited by a fixed size (currently 1000, see `MAX_OPEN_FILES_KEY_SPLIT` in `splitters.py`). This prevents hitting OS limits but means files for less frequent keys might be closed and reopened, impacting performance slightly compared to keeping all files open. If you encounter memory issues with extreme key cardinality, this limit might need adjustment in the code.
-   **Key Split Output Format:** Splitting by `key` *always* produces output files in JSON Lines (`.jsonl`) format, regardless of the `--output-format` setting. This is more efficient for appending items to many different files.
-   **Size Estimation:** Splitting by `size` is an *approximation*. Actual file sizes may vary slightly due to JSON formatting overhead and how items are grouped.
-   **JSON Path:** The `--path` argument uses `ijson`'s dot notation (e.g., `data.records.item`). If your target array is at the root of the JSON, use `item` or leave the path empty (`--path ""`).
-   **Pretty JSON Output:** When using the default `--output-format json`, the output JSON files will be pretty-printed with indentation for better readability.

## 🤝 Contributing

Found a bug or have an idea? Feel free to open an Issue or Pull Request!

## 📜 License

This project is licensed under the [MIT License](LICENSE). 