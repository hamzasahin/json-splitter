import logging
import re
import os
import json

# --- Logging Setup ---
# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
# Get logger (individual modules can get their own logger using logging.getLogger(__name__))
log = logging.getLogger("json_splitter") # Use a common root logger name

# --- Constants ---
PROGRESS_REPORT_INTERVAL = 10000 # Report progress every N items

# --- Helper Functions ---

def parse_size(size_str):
    """Parses a size string (e.g., 100KB, 5MB, 1GB, 150B, 2048) into bytes."""
    size_str_orig = size_str # Keep original for error messages
    size_str = size_str.strip().upper()
    if not size_str:
        raise ValueError("Size string cannot be empty.")

    multiplier = 1
    suffix = None
    if size_str.endswith('KB'):
        multiplier = 1024
        suffix = 'KB'
    elif size_str.endswith('MB'):
        multiplier = 1024 * 1024
        suffix = 'MB'
    elif size_str.endswith('GB'):
        multiplier = 1024 * 1024 * 1024
        suffix = 'GB'
    elif size_str.endswith('B'): # Handle explicit bytes suffix
        multiplier = 1
        suffix = 'B'
    # No else here, check if the remaining part is numeric after potentially stripping suffix

    numeric_part = size_str
    if suffix:
        numeric_part = size_str[:-len(suffix)].strip()

    if not numeric_part:
         raise ValueError(f"Missing numeric value before suffix in '{size_str_orig}'.")

    try:
        # Use float first to allow for decimal values (e.g., 1.5MB)
        # then convert to int after multiplying
        value = float(numeric_part)
        if value < 0:
             raise ValueError("Size value cannot be negative.")
        return int(value * multiplier)
    except ValueError:
         # Raise specific error if conversion fails
         raise ValueError(f"Invalid numeric value '{numeric_part}' in size string '{size_str_orig}'.")


def sanitize_filename(value):
    """Removes or replaces characters problematic for filenames.

    Also handles empty values, leading/trailing whitespace/underscores,
    and attempts to truncate based on UTF-8 byte length to avoid exceeding
    common filesystem limits (approx 100 bytes), respecting character boundaries.

    Args:
        value: The value to sanitize (will be converted to string).

    Returns:
        str: A sanitized string suitable for use in filenames.
    """
    s_value = str(value)
    # Remove leading/trailing whitespace FIRST
    s_value = s_value.strip()
    # Replace sequences of problematic characters with a SINGLE underscore
    # Problematic chars: whitespace, * \ / : ? " < > |
    s_value = re.sub(r'[\s*\\/:?"<>|]+', '_', s_value) # Corrected escaping for *
    # Remove any leading/trailing underscores that might result from replacement
    s_value = s_value.strip('_')
    # Ensure filename is not empty after sanitization
    if not s_value:
        s_value = "__empty__"

    # Limit length
    try:
        encoded_value = s_value.encode('utf-8')
        max_len_bytes = 100
        if len(encoded_value) > max_len_bytes:
            # New truncation logic: Find boundary by iterating forward
            cut_off = max_len_bytes
            # Adjust cut_off backwards until it's the start of a character boundary
            # ensuring we don't cut in the middle of a multi-byte sequence.
            while cut_off > 0 and (encoded_value[cut_off] & 0xC0) == 0x80:
                cut_off -= 1
            # Slice up to the identified boundary
            s_value = encoded_value[:cut_off].decode('utf-8', 'ignore')

    except UnicodeEncodeError as e:
        log.warning(f"Could not encode '{s_value}' to UTF-8 for length check: {e}")
        s_value = s_value[:max_len_bytes] # Fallback: simple character slice
    except UnicodeDecodeError as e:
        log.warning(f"Could not decode truncated bytes back to UTF-8 for '{s_value}': {e}")
        # Fallback might be tricky, maybe return original truncated string?
        s_value = s_value[:max_len_bytes]
    except Exception as e:
         log.warning(f"Could not properly truncate filename '{s_value}': {e}")
         s_value = s_value[:max_len_bytes]

    log.debug(f"Final sanitized filename part: '{s_value}'")
    return s_value 