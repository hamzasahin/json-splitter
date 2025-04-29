# JSON Splitter ✨

A simple, memory-efficient Python tool to split large JSON files into smaller, manageable chunks.

## 🤔 What does it do?

It takes a large JSON file (even multiple gigabytes!) and splits the data inside (specifically, an array of objects) into multiple smaller files. You can choose *how* to split it:

-   By a specific **number of items** per file (`count`).
-   By an approximate **file size** for each file (`size`, e.g., `10MB`).
-   By the **value of a specific key** within the data (`key`, e.g., grouping all items with the same `user_id` together).

## ✅ Why use it?

-   **Handles Huge Files:** Designed for files too large to fit into memory, using efficient streaming.
-   **Flexible Splitting:** Choose the method that best suits your needs.
-   **Works with Nested Data:** Can target data deep within the JSON structure using a simple path.
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
    source venv/bin/activate  # On Windows use: venv\Scripts\activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## 💻 How to Use

You have two ways to run the splitter:

### 1. Interactive Mode (Easy Start)

If you're unsure about the options, just run the script without any arguments. It will guide you step-by-step:

```bash
python src/json_splitter.py
```

### 2. Command-Line Interface (CLI)

For scripting or direct control, use the command line:

```bash
python src/json_splitter.py <input_file> <output_prefix> --split-by <strategy> --value <split_value> --path <json_path> [options]
```

**Core Arguments:**

| Argument        | Description                                                                 |
| :-------------- | :-------------------------------------------------------------------------- |
| `input_file`    | Path to your large input JSON file.                                         |
| `output_prefix` | Path and prefix for the output files (e.g., `output/chunk`). Directory created if needed. |
| `--split-by`    | How to split: `count`, `size`, or `key`.                                    |
| `--value`       | The value for the split strategy (e.g., `10000`, `50MB`, `product_id`).      |
| `--path`        | Path to the array you want to split (e.g., `item`, `data.records.item`).    |

**Common Options:**

| Option              | Description                                                                     |
| :------------------ | :------------------------------------------------------------------------------ |
| `--output-format`   | `json` (default) or `jsonl` (JSON Lines). *(Note: `key` split forces `jsonl`)* |
| `--max-records <N>` | *Secondary limit:* Max number of items per output file part.                    |
| `--max-size <size>` | *Secondary limit:* Max approximate size per output file part (e.g., `100MB`).   |
| `--filename-format` | Customize output file names (see *Filename Formatting* below).                  |
| `-v`, `--verbose`   | Show detailed debug messages.                                                   |

**Key Splitting Options:**
*(Only relevant when using `--split-by key`)*

| Option              | Description                                                                                                               |
| :------------------ | :------------------------------------------------------------------------------------------------------------------------ |
| `--on-missing-key`  | What to do if an item lacks the key: `group` (default, into `__missing_key__` file), `skip`, or `error` (stop script).      |
| `--on-invalid-item` | What to do if an item at `--path` isn't an object: `warn` (default, prints warning and skips), `skip`, or `error` (stop script). |

### Examples

**Example 1: Split by Size**

Split `large_log.json` into files of roughly 100MB each. The data to split is the array found under the `events` key.

```bash
python src/json_splitter.py large_log.json output/log_part --split-by size --value 100MB --path events
```
*Creates files like `output/log_part_chunk_0000.json`, `output/log_part_chunk_0001.json`, ...*

**Example 2: Split by Count (JSON Lines)**

Split `user_data.json` into files containing 50,000 users each, outputting as JSON Lines. The user objects are in an array under `results.users`.

```bash
python src/json_splitter.py user_data.json output/users --split-by count --value 50000 --path results.users.item --output-format jsonl
```
*Creates files like `output/users_chunk_0000.jsonl`, `output/users_chunk_0001.jsonl`, ...*

**Example 3: Split by Key**

Group items from `orders.json` based on their `customer_id`. The items are in the root array (`item`).

```bash
python src/json_splitter.py orders.json customer_orders/order --split-by key --value customer_id --path item
```
*Creates files like `customer_orders/order_key_cust101.jsonl`, `customer_orders/order_key_cust456.jsonl`, ...*

### Filename Formatting

You can control the output filenames using `--filename-format`. Available placeholders:

-   `{prefix}`: The `<output_prefix>` you provided.
-   `{type}`: How the split was done (`chunk` for count/size, `key` for key split).
-   `{index}`: The chunk number (e.g., `0000`, `0001`) or the sanitized key value (e.g., `user123`).
-   `{part}`: An optional part suffix (e.g., `_part_0001`) added if secondary limits (`max-records`/`max-size`) cause a split *within* a primary chunk/key.
-   `{ext}`: The file extension (`json` or `jsonl`).

**Default Format:** `{prefix}_{type}_{index:04d}{part}.{ext}` (for count/size) or `{prefix}_key_{index}{part}.{ext}` (for key).

## 💡 Good to Know

-   **Input Must Be Valid JSON:** The script expects a syntactically correct JSON file. If you have issues, validate your input file first.
-   **Memory Use with Many Keys:** Splitting by `key` on data with millions of unique keys *can* still use significant memory (to track the keys and file handles). The tool uses caching to limit open files, but be mindful if your keys have extremely high cardinality.
-   **Size Estimation:** Splitting by `size` is an *approximation*. Actual file sizes may vary slightly due to JSON formatting overhead.

## 🤝 Contributing

Found a bug or have an idea? Feel free to open an Issue or Pull Request!

## 📜 License

This project is licensed under the MIT License. (You should add a `LICENSE` file to your repository containing the MIT License text). 