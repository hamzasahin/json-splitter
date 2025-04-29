import sys

# Adjust path for sibling imports if necessary, especially if running as a script
# import os
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Use relative import
from .cli import main as run_cli

def main():
    """Main entry point for the JSON Splitter application."""
    run_cli()

if __name__ == "__main__":
    main() 