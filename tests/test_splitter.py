import pytest
import subprocess
import os
import json
import glob
import shutil
import sys

# Add project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Location of the script to test
SCRIPT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../src/json_splitter.py'))
# Location of test data
TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
SAMPLE_ARRAY_FILE = os.path.join(TEST_DATA_DIR, 'sample_array.json')
LARGE_JSON_FILE = os.path.join(TEST_DATA_DIR, 'json-40mb.json')

@pytest.fixture
def temp_output_dir(tmp_path):
    """Create a temporary directory for test output."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    yield output_dir
    # tmp_path fixture handles cleanup automatically

def run_splitter(args):
    """Helper function to run the splitter script as a subprocess."""
    cmd = [sys.executable, SCRIPT_PATH] + args
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
    assert len(fails) == 4, f"Expected 4 files, found {len(files)}: {files}" # 7 items, chunks of 2 -> 4 files

    # Check content of first and last
    data0 = load_jsonl_output(files[0])
    assert len(data0) == 2
    assert data0[0]["id"] == 1
    assert data0[1]["id"] == 2

    data3 = load_jsonl_output(files[3])
    assert len(data3) == 1
    assert data3[0]["id"] == 7

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