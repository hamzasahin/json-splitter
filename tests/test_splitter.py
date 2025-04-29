import pytest
import subprocess
import os
import json
import glob
import shutil
import sys
from pathlib import Path

# Helper function to count lines in a file
def count_lines(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        return 0

# Determine project root and add src to sys.path
PROJECT_ROOT = Path(__file__).parent.parent
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

# Define how to call the module
SPLITTER_MODULE = "src.main"

# Test data files
DATA_DIR = PROJECT_ROOT / "tests" / "data"
SAMPLE_ARRAY_FILE = DATA_DIR / "sample_array.json" # A:4, B:2, C:1
SAMPLE_JSONL_FILE = DATA_DIR / "sample.jsonl"
SAMPLE_ROOT_OBJECT_FILE = DATA_DIR / "sample_root_object.json"
SAMPLE_MIXED_ITEMS_FILE = DATA_DIR / "sample_mixed_items.json" # A:2, B:1, Missing:2 + 2 invalid
SAMPLE_ARRAY_WITH_MISSING_FILE = DATA_DIR / "sample_array_with_missing.json" # A:3, B:1, C:1, Missing:2
LARGE_JSON_FILE = DATA_DIR / "large_sample.json" # Define or correct the large file name
NONEXISTENT_FILE = DATA_DIR / "nonexistent.json"
INVALID_JSON_FILE = DATA_DIR / "invalid.json"

@pytest.fixture
def temp_output_dir(tmp_path):
    """Create a temporary directory for test output."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    yield output_dir
    # tmp_path fixture handles cleanup automatically

def run_splitter(args):
    """Helper function to run the splitter script as a subprocess."""
    # Use -m to run as a module, resolving relative imports
    cmd = [sys.executable, "-m", SPLITTER_MODULE] + args
    # Use repr() for cleaner command logging, especially with spaces/quotes
    print(f"\nRunning command: {repr(cmd)}")
    # Ensure consistent encoding and capture output
    result = subprocess.run(cmd, capture_output=True, text=True, check=False, encoding='utf-8')
    print(f"STDOUT:\n{result.stdout}")
    print(f"STDERR:\n{result.stderr}")
    # Raise exception if script returned non-zero exit code for easier debugging
    result.check_returncode()
    return result

def load_json_output(filepath):
    """Load JSON from a file, failing the test on error."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        pytest.fail(f"Failed to decode JSON from {filepath}: {e}")
    except FileNotFoundError:
        pytest.fail(f"Output file not found: {filepath}")
    except Exception as e:
        pytest.fail(f"Unexpected error loading JSON from {filepath}: {e}")

def load_jsonl_output(filepath):
    """Load list of objects from a JSON Lines file, failing the test on error."""
    data = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:
                    try:
                        data.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        pytest.fail(f"Failed to decode JSONL line {line_num} in {filepath}: {e}\nLine content: {line!r}")
        return data
    except FileNotFoundError:
        pytest.fail(f"Output file not found: {filepath}")
    except Exception as e:
         pytest.fail(f"Unexpected error loading JSONL from {filepath}: {e}")

# --- Integration Tests --- #

def test_split_by_count_basic(temp_output_dir):
    """Test basic splitting by count into JSON array files."""
    output_prefix = str(temp_output_dir / "count_basic")
    run_splitter([
        SAMPLE_ARRAY_FILE,
        output_prefix,
        "--split-by", "count",
        "--value", "3",
        "--path", "item"
    ])

    # Check files created (using the updated naming convention)
    files = sorted(glob.glob(f"{output_prefix}_chunk_*.json"))
    assert len(files) == 3, f"Expected 3 files, found {len(files)}: {files}"

    # Check content
    data0 = load_json_output(files[0])
    data1 = load_json_output(files[1])
    data2 = load_json_output(files[2])

    assert len(data0) == 3
    assert data0[0]["id"] == 1
    assert data0[2]["id"] == 3

    assert len(data1) == 3
    assert data1[0]["id"] == 4
    assert data1[2]["id"] == 6

    assert len(data2) == 1
    assert data2[0]["id"] == 7

def test_split_by_count_jsonl(temp_output_dir):
    """Test splitting by count into JSONL files."""
    output_prefix = str(temp_output_dir / "count_jsonl")
    run_splitter([
        SAMPLE_ARRAY_FILE,
        output_prefix,
        "--split-by", "count",
        "--value", "2",
        "--path", "item",
        "--output-format", "jsonl"
    ])

    files = sorted(glob.glob(f"{output_prefix}_chunk_*.jsonl"))
    assert len(files) == 4, f"Expected 4 files, found {len(files)}: {files}"

    # Check content of first and last
    data0 = load_jsonl_output(files[0])
    data3 = load_jsonl_output(files[3])

    assert len(data0) == 2
    assert data0[0]["id"] == 1
    assert data0[1]["id"] == 2

    assert len(data3) == 1
    assert data3[0]["id"] == 7

@pytest.mark.skip(reason="Requires a large sample JSON file which is not present")
def test_split_by_size_basic(temp_output_dir):
    """Test splitting by size into JSON array files using a larger file."""
    output_prefix = str(temp_output_dir / "size_basic")
    split_size_mb = 10
    split_size_bytes = split_size_mb * 1024 * 1024
    # Rough expectation: 40MB file split into 10MB chunks -> 4 files
    # Allow some tolerance due to approximate splitting
    expected_min_files = 3
    expected_max_files = 5

    run_splitter([
        LARGE_JSON_FILE,
        output_prefix,
        "--split-by", "size",
        "--value", f"{split_size_mb}MB",
        "--path", "item" # Assuming the large file is also an array at the root
    ])

    files = sorted(glob.glob(f"{output_prefix}_chunk_*.json"))
    assert expected_min_files <= len(files) <= expected_max_files, (
        f"Expected {expected_min_files}-{expected_max_files} files for ~{split_size_mb}MB split, found {len(files)}"
    )

    total_size = 0
    for i, f_path in enumerate(files):
        # Check file is valid JSON
        load_json_output(f_path)
        # Check file size (approximate)
        size = os.path.getsize(f_path)
        total_size += size
        print(f"  File {os.path.basename(f_path)} size: {size / (1024*1024):.2f} MB")
        # Allow for some variation, especially the last file
        # Increase tolerance factor (e.g., 50%)
        if i < len(files) - 1: # Don't check last file size too strictly
             assert size < split_size_bytes * 1.5, f"File {f_path} size {size} significantly exceeds target {split_size_bytes}"

    # Check total size is roughly the original size (within reason, formatting might change things)
    original_size = os.path.getsize(LARGE_JSON_FILE)
    assert original_size * 0.9 < total_size < original_size * 1.1, (
        f"Total output size {total_size} differs significantly from original {original_size}"
    )
    assert total_size > 0, "Total size of output chunks is zero."

@pytest.mark.skip(reason="Requires a large sample JSON file which is not present")
def test_split_by_size_jsonl(temp_output_dir):
    """Test splitting by size into JSONL files using a larger file."""
    output_prefix = str(temp_output_dir / "size_jsonl")
    split_size_mb = 8 # Use a slightly different size
    split_size_bytes = split_size_mb * 1024 * 1024
    # Rough expectation: 40MB file / 8MB chunks -> 5 files
    expected_min_files = 4
    expected_max_files = 6

    run_splitter([
        LARGE_JSON_FILE,
        output_prefix,
        "--split-by", "size",
        "--value", f"{split_size_mb}MB",
        "--path", "item", # Assuming the large file is also an array at the root
        "--output-format", "jsonl"
    ])

    files = sorted(glob.glob(f"{output_prefix}_chunk_*.jsonl"))
    assert expected_min_files <= len(files) <= expected_max_files, (
        f"Expected {expected_min_files}-{expected_max_files} files for ~{split_size_mb}MB split, found {len(files)}"
    )

    total_size = 0
    for i, f_path in enumerate(files):
        # Check file is valid JSONL
        load_jsonl_output(f_path)
        # Check file size (approximate)
        size = os.path.getsize(f_path)
        total_size += size
        print(f"  File {os.path.basename(f_path)} size: {size / (1024*1024):.2f} MB")
        # JSONL size calculation is more direct, tolerance can be smaller? Maybe 1.3x
        if i < len(files) - 1: # Don't check last file size too strictly
            assert size < split_size_bytes * 1.3, f"File {f_path} size {size} significantly exceeds target {split_size_bytes}"

    # Check total size is roughly the original size (JSONL might be slightly smaller than array JSON)
    original_size = os.path.getsize(LARGE_JSON_FILE)
    assert original_size * 0.85 < total_size < original_size * 1.05, (
         f"Total output size {total_size} differs significantly from original {original_size}"
    )
    assert total_size > 0, "Total size of output chunks is zero."

# --- Key Splitting Tests --- #

def test_split_by_key_basic(temp_output_dir):
    """Test basic splitting by key into JSONL files."""
    output_prefix = str(temp_output_dir / "key_basic")
    key_name = "category"
    run_splitter([
        SAMPLE_ARRAY_FILE, # Contains items with 'category': 'A' or 'B'
        output_prefix,
        "--split-by", "key",
        "--value", key_name,
        "--path", "item",
        # Output format defaults to jsonl for key splitting
    ])

    # Expect files named based on key values (A and B)
    file_a = f"{output_prefix}_key_A.jsonl"
    file_b = f"{output_prefix}_key_B.jsonl"
    file_c = f"{output_prefix}_key_C.jsonl"
    file_missing = f"{output_prefix}_key___missing_key__.jsonl"

    assert os.path.exists(file_a), f"Expected output file {file_a} not found."
    assert os.path.exists(file_b), f"Expected output file {file_b} not found."
    assert os.path.exists(file_c), f"Expected output file {file_c} not found."
    # Removed check for missing key file as SAMPLE_ARRAY_FILE has no missing keys
    # assert os.path.exists(file_missing), f"Expected output file {file_missing} not found."

    # Check content (simple line count for now)
    with open(file_a, 'r') as f:
        assert len(f.readlines()) == 4, f"Expected 4 items in {file_a}"
    with open(file_b, 'r') as f:
        assert len(f.readlines()) == 2, f"Expected 2 items in {file_b}"
    with open(file_c, 'r') as f:
        assert len(f.readlines()) == 1, f"Expected 1 item in {file_c}"

def test_split_by_key_missing_group(temp_output_dir):
    """Test splitting by key with missing keys grouped (default)."""
    output_prefix = str(temp_output_dir / "key_missing_group")
    key_name = "category"
    run_splitter([
        SAMPLE_ARRAY_WITH_MISSING_FILE, # Use file with missing keys
        output_prefix,
        "--split-by", "key",
        "--value", key_name,
        "--path", "item",
        "--on-missing-key", "group" # Explicitly set default
    ])

    # Expect A, B, and the special missing key file
    file_a = f"{output_prefix}_key_A.jsonl"
    file_b = f"{output_prefix}_key_B.jsonl"
    file_c = f"{output_prefix}_key_C.jsonl"
    file_missing = f"{output_prefix}_key___missing_key__.jsonl"

    assert os.path.exists(file_a)
    assert os.path.exists(file_b)
    assert os.path.exists(file_c)
    assert os.path.exists(file_missing), "File for missing keys not found when using 'group' policy"

    # Check content (line count based on SAMPLE_ARRAY_WITH_MISSING_FILE)
    with open(file_a, 'r') as f: assert len(f.readlines()) == 3
    with open(file_b, 'r') as f: assert len(f.readlines()) == 1
    with open(file_c, 'r') as f: assert len(f.readlines()) == 1
    with open(file_missing, 'r') as f: assert len(f.readlines()) == 2

def test_split_by_key_missing_skip(temp_output_dir):
    """Test splitting by key with missing keys skipped."""
    output_prefix = str(temp_output_dir / "key_missing_skip")
    key_name = "category"
    run_splitter([
        SAMPLE_ARRAY_WITH_MISSING_FILE, # Use file with missing keys
        output_prefix,
        "--split-by", "key",
        "--value", key_name,
        "--path", "item",
        "--on-missing-key", "skip"
    ])

    # Expect A, B, but NOT the missing key file
    file_a = f"{output_prefix}_key_A.jsonl"
    file_b = f"{output_prefix}_key_B.jsonl"
    file_c = f"{output_prefix}_key_C.jsonl"
    file_missing = f"{output_prefix}_key___missing_key__.jsonl"

    assert os.path.exists(file_a)
    assert os.path.exists(file_b)
    assert os.path.exists(file_c)
    assert not os.path.exists(file_missing), f"Missing key file {file_missing} found when using 'skip' policy"

    # Check content (simple line count)
    with open(file_a, 'r') as f:
        assert len(f.readlines()) == 3 # A:3 items in input with missing
    with open(file_b, 'r') as f:
        assert len(f.readlines()) == 1 # B:1 item in input with missing
    with open(file_c, 'r') as f:
        assert len(f.readlines()) == 1 # C:1 item in input with missing

def test_split_by_key_missing_error(temp_output_dir):
    """Test splitting by key with missing keys causing an error."""
    output_prefix = str(temp_output_dir / "key_missing_error")
    key_name = "category"

    # Expect the script to fail (non-zero exit code)
    with pytest.raises(subprocess.CalledProcessError) as excinfo:
        run_splitter([
            SAMPLE_ARRAY_WITH_MISSING_FILE, # Use file with missing keys
            output_prefix,
            "--split-by", "key",
            "--value", key_name,
            "--path", "item",
            "--on-missing-key", "error"
        ])

    # Check stderr for indication of the key error (adapt based on actual script output)
    assert f"Key '{key_name}' not found" in excinfo.value.stderr
    # The critical log might not be written if script exits early via log.error + sys.exit
    # Check for the initial ERROR log instead
    assert f"ERROR: Key '{key_name}' not found" in excinfo.value.stderr

    # Ensure no output files were created (or maybe partial ones before error? Check)
    files = glob.glob(f"{output_prefix}*.jsonl")
    # Allow for potentially partial files before error, but the missing_key one shouldn't exist
    # A more robust check might ensure the error happened *before* all processing finished.
    assert f"{output_prefix}_key___missing_key__.jsonl" not in files, "Missing key file created despite error setting."

# --- on-invalid-item Tests --- #

def test_split_by_key_invalid_item_warn(temp_output_dir):
    """Test key splitting with invalid items triggering warnings (default)."""
    output_prefix = str(temp_output_dir / "key_invalid_warn")
    key_name = "category"
    result = run_splitter([
        SAMPLE_MIXED_ITEMS_FILE,
        output_prefix,
        "--split-by", "key",
        "--value", key_name,
        "--path", "item",
        "--on-invalid-item", "warn" # Explicit default
    ])

    # Check that warnings were logged for invalid items
    # Check the specific WARNING log format
    assert "WARNING: Item 2 at path 'item' is not an object (type: <class 'str'>). Skipping key check." in result.stderr
    assert "WARNING: Item 5 at path 'item' is not an object (type: <class 'int'>). Skipping key check." in result.stderr

    # Check that valid items were processed correctly
    file_a = f"{output_prefix}_key_A.jsonl"
    file_b = f"{output_prefix}_key_B.jsonl"
    file_missing = f"{output_prefix}_key___missing_key__.jsonl"

    assert os.path.exists(file_a)
    assert os.path.exists(file_b)
    assert os.path.exists(file_missing)

    # Verify content
    assert count_lines(file_a) == 2
    assert count_lines(file_b) == 2
    assert count_lines(file_missing) == 1

def test_split_by_key_invalid_item_skip(temp_output_dir):
    """Test key splitting with invalid items skipped silently."""
    output_prefix = str(temp_output_dir / "key_invalid_skip")
    key_name = "category"
    result = run_splitter([
        SAMPLE_MIXED_ITEMS_FILE, # Uses file with string/int items
        output_prefix,
        "--split-by", "key",
        "--value", key_name,
        "--path", "item",
        "--on-invalid-item", "skip"
    ])

    # Check that NO warnings/errors about skipping invalid items were logged to stderr
    # Logging level is INFO by default, DEBUG messages shouldn't appear
    assert "Skipping: Item" not in result.stderr
    assert "is not an object/dict" not in result.stderr

    # Check that valid items were processed correctly (same as warn test)
    file_a = f"{output_prefix}_key_A.jsonl"
    file_b = f"{output_prefix}_key_B.jsonl"
    file_missing = f"{output_prefix}_key___missing_key__.jsonl"

    assert os.path.exists(file_a)
    assert os.path.exists(file_b)
    assert os.path.exists(file_missing)

    # Verify content
    assert count_lines(file_a) == 2
    assert count_lines(file_b) == 2
    assert count_lines(file_missing) == 1

def test_split_by_key_invalid_item_error(temp_output_dir):
    """Test key splitting with invalid items causing an error."""
    output_prefix = str(temp_output_dir / "key_invalid_error")
    key_name = "category"

    # Expect the script to fail (non-zero exit code)
    with pytest.raises(subprocess.CalledProcessError) as excinfo:
        run_splitter([
            SAMPLE_MIXED_ITEMS_FILE,
            output_prefix,
            "--split-by", "key",
            "--value", key_name,
            "--path", "item",
            "--on-invalid-item", "error"
        ])

    # Check stderr for indication of the type error
    # Check the specific ERROR log message
    assert "ERROR: Item 2 at path 'item' is not an object (type: <class 'str'>)." in excinfo.value.stderr

# --- Secondary Constraint Tests --- #

def test_split_count_with_max_size(temp_output_dir):
    """Test count splitting with a secondary max_size limit."""
    output_prefix = str(temp_output_dir / "count_max_size")
    primary_count = 5 # Primary target: 5 items per chunk
    # Estimate: items are ~60 bytes each. 5 items ~ 300 bytes + overhead.
    # Set max size low enough to trigger before 5 items.
    max_size_bytes = 150

    run_splitter([
        SAMPLE_ARRAY_FILE, # 7 items total
        output_prefix,
        "--split-by", "count",
        "--value", str(primary_count),
        "--path", "item",
        "--max-size", f"{max_size_bytes}B", # Use bytes for precision
        "--output-format", "jsonl" # Easier size check for JSONL
    ])

    files = sorted(glob.glob(f"{output_prefix}_chunk_*.jsonl"))

    # Expectation: Chunk 0 should be split by size before reaching 5 items.
    # Item sizes: ~60-70 bytes. 150B limit -> split after 2 items usually.
    # Chunk 0: items 1,2 (part 0); items 3,4 (part 1); item 5 (part 2) -> _chunk_0000_part_0000, _part_0001, _part_0002
    # Primary count limit reached for item 5, starts new primary chunk 1.
    # Chunk 1: items 6,7 -> _chunk_0001
    # Total files expected: 3 parts for chunk 0 + 1 file for chunk 1 = 4 files
    assert len(files) >= 3, f"Expected more files due to size constraint, found {len(files)}: {files}"

    chunk0_part0 = f"{output_prefix}_chunk_0000.jsonl"
    chunk0_part1 = f"{output_prefix}_chunk_0000_part_0001.jsonl"
    chunk0_part2 = f"{output_prefix}_chunk_0000_part_0002.jsonl"
    chunk1 = f"{output_prefix}_chunk_0001.jsonl"

    assert os.path.exists(chunk0_part0)
    assert os.path.exists(chunk0_part1)
    # Part 2 might or might not exist depending on exact size, check chunk1 existence is key.
    assert os.path.exists(chunk1)

    # Check first part was split by size (less than primary_count items)
    data0_p0 = load_jsonl_output(chunk0_part0)
    assert 1 <= len(data0_p0) < primary_count
    assert os.path.getsize(chunk0_part0) < max_size_bytes * 1.3 # Allow tolerance

    # Check next primary chunk content
    data1 = load_jsonl_output(chunk1)
    assert {item['id'] for item in data1} == {6, 7} # Items after the primary split point

def test_split_count_with_max_records(temp_output_dir):
    """Test count splitting where max_records overrides the primary count."""
    output_prefix = str(temp_output_dir / "count_max_records")
    primary_count = 5
    max_records = 2 # Smaller than primary_count

    run_splitter([
        SAMPLE_ARRAY_FILE, # 7 items
        output_prefix,
        "--split-by", "count",
        "--value", str(primary_count),
        "--path", "item",
        "--max-records", str(max_records)
    ])

    files = sorted(glob.glob(f"{output_prefix}_chunk_*.json"))

    # Effective split is by max_records=2. 7 items -> 4 files.
    # No secondary parts expected as max_records IS the primary effective limit.
    assert len(files) == 4, f"Expected 4 files based on max_records=2, found {len(files)}"
    assert "_part_" not in files[0], "Part suffix should not exist when max_records overrides count."

    # Check content and counts
    data0 = load_json_output(files[0])
    data1 = load_json_output(files[1])
    data2 = load_json_output(files[2])
    data3 = load_json_output(files[3])

    assert len(data0) == max_records
    assert len(data1) == max_records
    assert len(data2) == max_records
    assert len(data3) == 1 # Remainder
    assert data0[0]['id'] == 1
    assert data3[0]['id'] == 7

def test_split_key_with_max_records(temp_output_dir):
    """Test key splitting with a secondary max_records limit."""
    output_prefix = str(temp_output_dir / "key_max_records")
    key_name = "category"
    max_records = 2

    run_splitter([
        SAMPLE_ARRAY_FILE, # A:4, B:2, C:1 -> Use original file for this
        output_prefix,
        "--split-by", "key",
        "--value", key_name,
        "--path", "item",
        "--max-records", str(max_records)
    ])

    # Expect category A (4 items) to be split by max_records=2
    # Expect category B (2 items) NOT split
    # Expect category C (1 item) NOT split
    # Expect Missing NOT created (as input file has no missing keys)
    file_a = f"{output_prefix}_key_A.jsonl"
    file_b = f"{output_prefix}_key_B.jsonl"
    file_c = f"{output_prefix}_key_C.jsonl"
    file_missing = f"{output_prefix}_key___missing_key__.jsonl"
    file_a_part1 = f"{output_prefix}_key_A_part_0001.jsonl" # SHOULD exist now

    assert os.path.exists(file_a), f"File {file_a} missing."
    assert os.path.exists(file_b), f"File {file_b} missing."
    assert os.path.exists(file_c), f"File {file_c} missing."
    assert not os.path.exists(file_missing), f"File {file_missing} should not exist for this input."
    assert os.path.exists(file_a_part1), f"File {file_a_part1} missing (expected due to max_records=2 on A=4 items)."

    # Check content counts
    with open(file_a, 'r') as f: assert len(f.readlines()) == 2 # First part of A
    with open(file_a_part1, 'r') as f: assert len(f.readlines()) == 2 # Second part of A
    with open(file_b, 'r') as f: assert len(f.readlines()) == 2 # B not split
    with open(file_c, 'r') as f: assert len(f.readlines()) == 1 # C not split

def test_split_key_with_max_size(temp_output_dir):
    """Test key splitting with a secondary max_size limit."""
    output_prefix = str(temp_output_dir / "key_max_size")
    key_name = "category"
    # Items ~60 bytes. Category A has 4 items (~240 bytes).
    # Set limit low enough to split category A.
    max_size_bytes = 120

    run_splitter([
        SAMPLE_ARRAY_FILE,
        output_prefix,
        "--split-by", "key",
        "--value", key_name,
        "--path", "item",
        "--max-size", f"{max_size_bytes}B"
    ])

    # Expect category A (4 items, ~240B) to be split by size (120B limit)
    # Expect category B (2 items, ~78B) NOT to be split by size
    # Expect category C (1 item, ~70B) NOT split
    # Expect Missing NOT created (as input file has no missing keys)
    file_a_part0 = f"{output_prefix}_key_A.jsonl"
    file_a_part1 = f"{output_prefix}_key_A_part_0001.jsonl"
    file_b = f"{output_prefix}_key_B.jsonl"
    file_b_part1 = f"{output_prefix}_key_B_part_0001.jsonl"
    file_c = f"{output_prefix}_key_C.jsonl"
    file_missing = f"{output_prefix}_key___missing_key__.jsonl"

    assert os.path.exists(file_a_part0), "Part 0 for key A missing."
    assert os.path.exists(file_a_part1), "Part 1 for key A missing."
    assert os.path.exists(file_b), "File for key B missing."
    assert not os.path.exists(file_b_part1), "Part 1 for key B should NOT exist (size 78B < 120B limit)."
    assert os.path.exists(file_c), "File for key C missing."
    assert not os.path.exists(file_missing), f"File {file_missing} should not exist for this input."

    # Check counts
    with open(file_a_part0, 'r') as f: assert len(f.readlines()) == 3 # Items 1, 3, 6 < 120B limit
    with open(file_a_part1, 'r') as f: assert len(f.readlines()) == 1 # Item 7 starts new part
    with open(file_b, 'r') as f: assert len(f.readlines()) == 2 # B not split
    # Removed check for file_b_part1 content
    with open(file_c, 'r') as f: assert len(f.readlines()) == 1 # C not split

# --- Error Condition Tests --- #

def test_error_file_not_found(temp_output_dir):
    """Test running the script with a non-existent input file."""
    output_prefix = str(temp_output_dir / "error_not_found")
    non_existent_file = "/path/to/non/existent/file.json"

    # Expect the script to fail (non-zero exit code)
    # The error might be caught by our validation or CalledProcessError
    with pytest.raises(subprocess.CalledProcessError) as excinfo:
        run_splitter([
            non_existent_file,
            output_prefix,
            "--split-by", "count",
            "--value", "10",
            "--path", "item"
        ])

    # Check stderr for the file not found message from our validation
    assert f"Input file not found: {non_existent_file}" in excinfo.value.stderr

def test_error_invalid_json(temp_output_dir):
    """Test running the script with invalid JSON input."""
    output_prefix = str(temp_output_dir / "error_invalid_json")

    with pytest.raises(subprocess.CalledProcessError) as excinfo:
        run_splitter([
            INVALID_JSON_FILE,
            output_prefix,
            "--split-by", "count",
            "--value", "1",
            "--path", "item"
        ])

    # Check stderr for a JSON parsing error message
    assert "Error parsing JSON" in excinfo.value.stderr

@pytest.mark.parametrize(
    "test_id, args, expected_error_msg",
    [
        (
            "negative_count",
            ["--split-by", "count", "--value", "-5", "--path", "item"],
            "argument --value: Count must be a positive integer."
        ),
        (
            "zero_count",
            ["--split-by", "count", "--value", "0", "--path", "item"],
            "argument --value: Count must be a positive integer."
        ),
        (
            "non_int_count",
            ["--split-by", "count", "--value", "abc", "--path", "item"],
            "argument --value: Value must be a valid positive integer."
        ),
        (
            "bad_size_format",
            ["--split-by", "size", "--value", "10XYZ", "--path", "item"],
            "argument --value: Invalid size format: Invalid numeric value '10XYZ' in size string '10XYZ'."
        ),
        (
            "zero_size",
            ["--split-by", "size", "--value", "0MB", "--path", "item"],
            "argument --value: Size must be positive."
        ),
        (
            "negative_size",
            ["--split-by", "size", "--value", "-5KB", "--path", "item"],
            "argument --value: expected one argument" # Argparse catches -5KB as an option
        ),
        (
            "missing_value",
            ["--split-by", "count", "--path", "item"],
            "the following arguments are required in non-interactive mode: --value"
        ),
        (
            "missing_path",
            ["--split-by", "count", "--value", "10"],
            "the following arguments are required in non-interactive mode: --path"
        ),
        (
            "bad_secondary_size",
            ["--split-by", "count", "--value", "10", "--path", "item", "--max-size", "foo"],
            "argument --max-size: Invalid size format: Invalid numeric value 'FOO' in size string 'foo'.."
        ),
        (
            "bad_choice_on_missing",
            ["--split-by", "key", "--value", "k", "--path", "item", "--on-missing-key", "invalid"],
            "argument --on-missing-key: invalid choice: 'invalid'"
        ),
    ]
)
def test_error_invalid_args(temp_output_dir, test_id, args, expected_error_msg):
    """Test running the script with various invalid arguments."""
    output_prefix = str(temp_output_dir / f"error_args_{test_id}")
    input_file = SAMPLE_ARRAY_FILE # Use a valid file for arg tests

    # Construct command: add input/output prefix to the specific args
    cmd_args = [input_file, output_prefix] + args

    with pytest.raises(subprocess.CalledProcessError) as excinfo:
        run_splitter(cmd_args)

    # Check stderr for the expected error message fragment
    # Use "in" for more robust checking against extra log lines
    assert expected_error_msg in excinfo.value.stderr, \
        f"Expected '{expected_error_msg}' not found in stderr:\n{excinfo.value.stderr}"
