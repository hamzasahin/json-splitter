import re
import logging

def parse_size(size_str: str | int | float) -> int:
    """Parses a size string (e.g., '10MB', '200KB', 1024) into bytes."""
    if isinstance(size_str, (int, float)):
        if size_str >= 0:
            return int(size_str)
        else:
            raise ValueError(f"Size must be non-negative, got: {size_str}")
            
    size_str = str(size_str).strip().upper()
    if not size_str:
         raise ValueError("Size string cannot be empty.")
         
    match = re.match(r'^(\d+(\.\d+)?)\s*([KMGT]B|[KMGT])?$', size_str) # Allow K, M, G, T or KB, MB etc.
    if not match:
        # Check if it's just a number (already handled by isinstance but good robustness)
        try:
             val = int(size_str)
             if val >= 0:
                 return val
             else:
                 raise ValueError(f"Size must be non-negative, got: {size_str}")
        except ValueError:
             # If it's not just a number and didn't match regex, it's invalid
             raise ValueError(f"Invalid size format: {size_str}")

    value = float(match.group(1))
    unit = match.group(3)

    factors = {
        'B': 1,
        'KB': 1024, 'K': 1024, 
        'MB': 1024**2, 'M': 1024**2,
        'GB': 1024**3, 'G': 1024**3,
        'TB': 1024**4, 'T': 1024**4
    }
    
    factor = 1 # Default to bytes if no unit
    if unit:
        if unit in factors:
            factor = factors[unit]
        else:
             # This case should theoretically not be reached due to regex, but safeguard anyway
             raise ValueError(f"Invalid size unit in: {size_str}")

    result = value * factor
    if result < 0:
         raise ValueError(f"Calculated size is negative ({result}), input: {size_str}")
         
    return int(result) 