
Okay, let's break down the potential issues and map out a detailed plan to address them.

**Analysis of Potential Issues:**

1.  **Lengthy/Complex Logic:**
    *   **Observation:** Functions like `split_by_count`, `split_by_size`, and `split_by_key` handle multiple responsibilities: iterating through JSON items, estimating size, checking primary and secondary split conditions, managing state (chunk data, file indices, key-specific state), formatting filenames, and writing data. This makes them hard to read, test, and modify.
    *   **Impact:** Reduced maintainability, increased risk of bugs when making changes, harder testing.

2.  **`split_by_key` Memory Usage:**
    *   **Observation:** The `key_states` dictionary holds file handles and metadata for *every* unique key encountered during the stream. For datasets with millions of unique keys (e.g., user IDs in a large log file), this dictionary can grow very large, potentially exceeding available memory.
    *   **Impact:** Potential `MemoryError` for large, high-cardinality datasets, limiting the tool's applicability. The current warning helps but doesn't solve the underlying scaling issue.

3.  **Duplicated Error Handling:**
    *   **Observation:** `FileNotFoundError` and `ijson.JSONError` exceptions are caught and handled similarly (logging an error, returning `False`) within each of the main `split_by_*` functions. The main `execute_split` function also has some overarching error catching.
    *   **Impact:** Violates DRY (Don't Repeat Yourself) principle, making error handling logic harder to update consistently.

4.  **File Closing:**
    *   **Observation:** `split_by_key` uses a `finally` block to ensure its many file handles are closed. `split_by_count` and `split_by_size` use `_write_chunk`, which employs a `with open(...)` statement for each write, automatically handling closure for *that specific write*.
    *   **Impact:** This isn't necessarily an *issue* as both methods achieve file closure. However, it highlights the different state management approaches (`split_by_key` maintains many open handles, others open/close per chunk). The `finally` block in `split_by_key` is crucial due to its design.

5.  **Inconsistent Logging/Formatting:**
    *   **Observation:** Log messages might use slightly different phrasing for similar events (e.g., starting a split, writing a chunk). Filename formatting defaults and handling might differ slightly or could be centralized.
    *   **Impact:** Minor readability issues, potentially confusing log output.

**Detailed Fix Plan:**

Here’s a phased approach to refactor and improve the code:

**Phase 1: Refactor Core Splitting Logic & Common Helpers**

*   **Goal:** Break down the large splitting functions into smaller, single-responsibility units and create common helpers.
*   **Steps:**
    1.  **Create `_estimate_item_size(item, log)`:**
        *   Extract the `json.dumps(item).encode('utf-8')` logic into a helper.
        *   Handle potential `TypeError` within this helper, log a warning, and return 0.
        *   Replace size estimation code in `split_by_count`, `split_by_size`, and `split_by_key` with calls to this helper.
    2.  **Refactor `split_by_count`:**
        *   Create `_check_count_split_conditions(...)`: Takes current chunk state, item size, limits, returns flags (`split_needed`, `primary_split`, `part_split`, `carry_over_item`). Move the complex condition checking logic here.
        *   Simplify the main loop in `split_by_count` to:
            *   Call `_estimate_item_size`.
            *   Call `_check_count_split_conditions`.
            *   If split needed: handle writing (`_write_chunk`), reset state, handle carry-over item.
    3.  **Refactor `split_by_size`:**
        *   Create `_check_size_split_conditions(...)`: Takes current chunk state, item size, limits, returns flags (`split_needed`, `primary_split`). Move condition checking here.
        *   Simplify the main loop similarly to `split_by_count`.
    4.  **Refactor `_write_chunk`:**
        *   Review filename generation logic. Ensure consistent handling of `filename_format` errors and provide a robust default fallback. Consider moving default format strings to constants.
        *   Ensure clear logging within the function.
    5.  **Refactor `split_by_key` (Initial Helpers):**
        *   Create `_handle_key_acquisition(item, key_name, item_count_total, path, on_missing_key, on_invalid_item, log)`: Handles non-dict items, gets key value, deals with missing/complex keys based on policy, returns `(sanitized_value, should_skip, success)`.
        *   Create `_get_or_init_key_state(key_states, sanitized_value, base_overhead, log, warned_flag, threshold)`: Manages the `key_states` dictionary, including initialization and the high-cardinality warning logic. Returns `(state, warned_flag)`.

**Phase 2: Refine `split_by_key` & Error Handling**

*   **Goal:** Complete the `split_by_key` refactoring, address memory concerns conceptually, and centralize error handling.
*   **Steps:**
    1.  **Refactor `split_by_key` (File Management):**
        *   Create `_check_key_part_split(...)`: Checks secondary limits (`max_records`, `max_size_bytes`) for the current key's file part *before* writing.
        *   Create `_manage_key_file_handle(state, output_prefix, sanitized_value, file_ext, filename_format, log)`: Handles opening new files (or parts) for a key when needed (first item or after a part split). Includes filename formatting specific to keys.
        *   Create `_write_to_key_file(state, item_str, item_size, per_item_overhead, log)`: Performs the actual write and updates count/size state.
        *   Simplify the main `split_by_key` loop to orchestrate calls to these helpers.
    2.  **Centralize `ijson.JSONError` Handling:**
        *   Modify the main loop wrappers in `split_by_count`, `split_by_size`, and `split_by_key`. Instead of detailed logging inside each, catch `ijson.JSONError`, log a concise error pointing to the main handler, and return `False`.
        *   Enhance the error handling in `execute_split` to provide more detailed context (like line/col numbers from the exception) when a split function returns `False` due to `ijson.JSONError`.
    3.  **Address `split_by_key` Memory (Documentation/Warning):**
        *   Improve the code comments around `key_states` and `MAX_UNIQUE_KEYS_WARN_THRESHOLD`. Clearly explain the memory implication.
        *   *Optional Consideration (No Code Change Yet):* Add a comment suggesting potential future strategies like an LRU cache for file handles if memory limits are hit in practice.
    4.  **Refine `finally` block in `split_by_key`:** Ensure it correctly iterates through the potentially modified `key_states` structure after refactoring and closes all handles.

**Phase 3: Logging, Consistency, and Final Review**

*   **Goal:** Ensure consistent logging, formatting, and perform a final review.
*   **Steps:**
    1.  **Review Logging:** Read through all `log.*` calls. Standardize verbosity (e.g., use `log.debug` for detailed state changes, `log.info` for major steps/progress). Ensure progress reports are consistent.
    2.  **Review Filename Formatting:** Consolidate default format strings as constants. Double-check the logic in `_write_chunk` and `_manage_key_file_handle` for correctness and robust fallback behavior.
    3.  **Docstrings:** Update docstrings for all modified functions and add them for all new helper functions.
    4.  **Final Code Review:** Read through the entire refactored `json_splitter.py` file, checking for clarity, consistency, and potential remaining issues.
    5.  **(Manual) Testing:** As outlined previously, test thoroughly with different modes and edge cases.

This plan breaks the work into manageable phases, prioritizing the most impactful changes (refactoring complex logic) first. Each step aims to improve modularity, readability, and maintainability.
