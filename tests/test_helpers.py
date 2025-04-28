import pytest
import sys
import os

# Add project root to the Python path to allow importing the main module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Assuming your main script is src/json_splitter.py
# Adjust the import based on your actual structure if needed
from src.json_splitter import _parse_size, _sanitize_filename

# Tests for _parse_size
def test_parse_size_bytes():
    assert _parse_size("1024") == 1024

def test_parse_size_kb():
    assert _parse_size("1KB") == 1024
    assert _parse_size("1kb") == 1024
    assert _parse_size("10kB") == 10 * 1024

def test_parse_size_mb():
    assert _parse_size("1MB") == 1024 * 1024
    assert _parse_size("1mb") == 1024 * 1024
    assert _parse_size("5MB") == 5 * 1024 * 1024

def test_parse_size_gb():
    assert _parse_size("1GB") == 1024 * 1024 * 1024
    assert _parse_size("1gb") == 1024 * 1024 * 1024
    assert _parse_size("2GB") == 2 * 1024 * 1024 * 1024

def test_parse_size_invalid_unit():
    with pytest.raises(ValueError, match="Invalid size format"):
        _parse_size("1TB")

def test_parse_size_invalid_value():
    with pytest.raises(ValueError):
        _parse_size("MB")
    with pytest.raises(ValueError):
        _parse_size("1.5MB")
    with pytest.raises(ValueError):
        _parse_size("abcMB")

# Tests for _sanitize_filename
def test_sanitize_basic():
    assert _sanitize_filename("simple") == "simple"

def test_sanitize_spaces():
    assert _sanitize_filename("with spaces") == "with_spaces"
    assert _sanitize_filename("  leading trailing  ") == "leading_trailing"

def test_sanitize_special_chars():
    assert _sanitize_filename('a/b\\c:d*e?f"g<h>i|j') == "a_b_c_d_e_f_g_h_i_j"

def test_sanitize_combined():
    assert _sanitize_filename("  a / b * c?  ") == "a_b_c"

def test_sanitize_long_filename():
    long_name = "a" * 150
    assert _sanitize_filename(long_name) == "a" * 100

def test_sanitize_numbers():
    assert _sanitize_filename(123) == "123"
    assert _sanitize_filename(123.45) == "123.45" # '.' is typically allowed

def test_sanitize_none():
    assert _sanitize_filename(None) == "None" 