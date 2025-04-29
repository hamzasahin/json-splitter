import argparse
import sys
import os
import logging
import yaml # Added for config file loading

from .utils import log, parse_size # Import necessary utils
from .splitters import CountSplitter, SizeSplitter, KeySplitter # Import splitter classes

# --- Helper Functions for Interactive Mode ---

def _prompt_with_validation(prompt_text, required=True, validation_func=None, choices=None, default=None):
    """Generic function to prompt user with validation and choices."""
    while True:
        prompt_suffix = f" [{default}]" if default is not None else ""
        # Modify prompt to include choices if available
        choices_display = f" ({' / '.join(choices)})" if choices else ""
        try:
            user_input = input(f"{prompt_text}{choices_display}{prompt_suffix}: ").strip()
            if not user_input:
                if default is not None:
                    return default # Return default if user just hits Enter
                elif required:
                    print("  Error: Input is required.")
                    continue
                else:
                    return None # Allow empty input if not required and no default

            if choices:
                # Case-insensitive matching
                matched_choice = next((c for c in choices if c.lower() == user_input.lower()), None)
                if matched_choice is None:
                    print(f"  Error: Invalid choice. Please choose from: {' / '.join(choices)}")
                    continue
                user_input = matched_choice # Use the actual choice value

            if validation_func:
                is_valid, error_msg_or_value = validation_func(user_input)
                if not is_valid:
                    print(f"  Error: {error_msg_or_value}")
                    continue
                # Validation might return the processed value
                if error_msg_or_value is not None and error_msg_or_value != True:
                     # Check if validation returned processed value (like int/size str)
                     # If it did, return that directly
                     # Be careful: Ensure validation_func contract is clear.
                     # Let's assume it returns the validated string or processed value for now.
                    return error_msg_or_value

            return user_input # Return the validated (potentially matched) input
        except EOFError: # Handle Ctrl+D
            print("\nOperation cancelled.")
            sys.exit(0)
        except KeyboardInterrupt:
             print("\nOperation cancelled by user.")
             sys.exit(0)

def _validate_input_file(filepath):
    if not filepath:
         return False, "Input file path cannot be empty."
    if not os.path.isfile(filepath):
        return False, f"File not found at '{filepath}'."
    if not os.access(filepath, os.R_OK):
        return False, f"File is not readable (check permissions): '{filepath}'."
    return True, filepath # Return path on success

def _validate_output_dir(dir_path):
     if not dir_path:
          return False, "Output directory path cannot be empty."
     # Basic check for potentially invalid characters - less strict for dirs than files
     # but still good to catch common issues if not using os.makedirs robustness
     # dirname = os.path.basename(dir_path) # No, use the whole path for check
     # invalid_chars = ':*?"<>|'
     # if any(c in invalid_chars for c in dir_path):
     #     return False, f"Output directory path '{dir_path}' contains potentially invalid characters: {invalid_chars}"

     # Check if path exists
     if not os.path.exists(dir_path):
         # Try creating it
         log.info(f"Output directory does not exist. Attempting to create: {dir_path}")
         try:
             os.makedirs(dir_path, exist_ok=True)
             # Check writability *after* creation attempt
             if not os.access(dir_path, os.W_OK):
                 return False, f"Created output directory is not writable: {dir_path}"
         except OSError as e:
             return False, f"Could not create output directory '{dir_path}': {e}"
         except Exception as e:
             return False, f"An unexpected error occurred creating directory '{dir_path}': {e}"
     # If path exists, check if it's actually a dir and writable
     elif not os.path.isdir(dir_path):
         return False, f"Output path exists but is not a directory: {dir_path}"
     elif not os.access(dir_path, os.W_OK):
         return False, f"Output directory is not writable: {dir_path}"

     return True, dir_path # Return dir_path on success

def _validate_path(path_str):
     if not path_str:
          return False, "JSON path cannot be empty."
     # Basic check - doesn't validate ijson syntax but ensures non-empty
     # Could add regex check for basic patterns if needed
     return True, path_str

def _validate_split_value(value_str, split_by):
    if not value_str:
        return False, "Split value cannot be empty."
    if split_by == 'count':
        try:
            count = int(value_str)
            if count <= 0:
                return False, "Count must be a positive integer."
            return True, count # Return the integer value
        except ValueError:
            return False, "Value must be a valid positive integer."
    elif split_by == 'size':
        try:
            # Use parse_size for validation
            size_bytes = parse_size(value_str)
            if size_bytes <= 0:
                return False, "Size must be positive."
            return True, value_str # Return the original string if valid
        except ValueError as e:
            return False, f"Invalid size format: {e}."
    elif split_by == 'key':
        # Key name just needs to be non-empty string
        if not isinstance(value_str, str) or not value_str:
             return False, "Key name must be a non-empty string."
        return True, value_str
    else:
         return False, "Invalid split_by type for value validation."

def _validate_optional_int(value_str):
    if not value_str or value_str.lower() == 'none': # Allow 'None' as input
        return True, None
    try:
        num = int(value_str)
        if num <= 0:
             return False, "Value must be a positive integer if provided."
        return True, num # Return the int
    except ValueError:
         return False, "Value must be a valid positive integer."

def _validate_optional_size(value_str):
     if not value_str or value_str.lower() == 'none':
        return True, None # Return None if empty or 'None'
     try:
        size_bytes = parse_size(value_str)
        if size_bytes <= 0:
            return False, "Size must be positive if provided."
        return True, value_str # Return the original string if valid
     except ValueError as e:
        return False, f"Invalid size format: {e}."

# --- End Interactive Helpers ---

def run_interactive_mode():
    """Prompts the user for arguments interactively."""
    log.info("✨ Welcome to JSON Splitter Interactive Mode! ✨")
    log.info("Let's configure the splitting process step-by-step.")
    args = argparse.Namespace()

    # Set defaults mirroring argparse
    args.output_format = 'json'
    args.max_records = None
    args.max_size = None
    args.on_missing_key = 'group'
    args.on_invalid_item = 'warn'
    args.verbose = False
    args.filename_format = None # Will be set later based on split_by
    args.report_interval = 10000 # Add default for interactive

    try:
        print("\n--- 📝 Required Settings ---")
        args.input_file = _prompt_with_validation(
            "📄 Enter path to the input JSON file",
            validation_func=_validate_input_file
        )
        args.output_dir = _prompt_with_validation(
            "📂 Enter output directory path (e.g., output/, . for current)",
            default=".",
            validation_func=_validate_output_dir # Use new validation function
        )
        args.base_name = _prompt_with_validation(
            "🏷️ Enter base name for output files (e.g., chunk, data_part)",
            default="chunk",
            required=True # Require a base name
        )
        args.split_by = _prompt_with_validation(
            "✂️ Split by which criterion? (count / size / key)",
            choices=['count', 'size', 'key']
        )

        # Provide context for value prompt
        value_prompt = f"🔢 Enter value for '{args.split_by}' split"
        if args.split_by == 'count': value_prompt += " (e.g., 10000)"
        elif args.split_by == 'size': value_prompt += " (e.g., 15MB, 500KB)"
        elif args.split_by == 'key': value_prompt += " (e.g., user_id)"
        args.value = _prompt_with_validation(value_prompt, validation_func=lambda v: _validate_split_value(v, args.split_by))

        args.path = _prompt_with_validation(
            "🎯 Enter JSON path to items (e.g., `item`, `data.records.item`)",
            validation_func=_validate_path
        )

        print("\n--- 🤔 Optional Settings --- (Press Enter to use defaults)")
        set_optionals = _prompt_with_validation("Configure optional settings?", required=False, choices=['y', 'n'], default='n')

        if set_optionals.lower() == 'y':
            log.info("\n🔧 Configuring optional settings...")
            args.output_format = _prompt_with_validation(
                "📦 Output format?", choices=['json', 'jsonl'],
                default=args.output_format, required=False
            )
            args.max_records = _prompt_with_validation(
                "📏 Max records per part (secondary limit)?", default="None",
                validation_func=_validate_optional_int, required=False
            )
            args.max_size = _prompt_with_validation(
                "💾 Max size per part (secondary limit)?", default="None",
                validation_func=_validate_optional_size, required=False
            )

            if args.split_by == 'key':
                 log.info("\n🔑 Key Split Specific Options:")
                 args.on_missing_key = _prompt_with_validation(
                     "❓ Action for missing key?", choices=['group', 'skip', 'error'],
                     default=args.on_missing_key, required=False
                 )
                 args.on_invalid_item = _prompt_with_validation(
                     "⚠️ Action for invalid items?", choices=['warn', 'skip', 'error'],
                     default=args.on_invalid_item, required=False
                 )

            # Set default format based on split type *before* prompting
            default_ff = "{base_name}_key_{index}{part}.{ext}" if args.split_by == 'key' else "{base_name}_{type}_{index:04d}{part}.{ext}"
            ff_prompt = "🏷️ Output filename format?"
            args.filename_format = _prompt_with_validation(ff_prompt, default=default_ff, required=False)

            verbose_resp = _prompt_with_validation("🐞 Enable verbose logging?", choices=['y', 'n'], default='n', required=False)
            args.verbose = (verbose_resp.lower() == 'y')

            # Add prompt for report interval
            args.report_interval = _prompt_with_validation(
                 "📊 Report progress every N items?",
                 default=str(args.report_interval), # Use current default
                 validation_func=_validate_optional_int, required=False
            )
            # Ensure it's an int (or None if validation returns None)
            if args.report_interval is None: args.report_interval = 0 # Treat None as 0 (disabled)
        else:
            # Ensure filename_format gets a default even if optionals skipped
            args.filename_format = "{base_name}_key_{index}{part}.{ext}" if args.split_by == 'key' else "{base_name}_{type}_{index:04d}{part}.{ext}"

        # --- Configuration Summary and Confirmation ---
        print("\n" + "="*40)
        log.info("⚙️ Summary of Configuration:")
        print("-"*40)
        print(f"  Input File:        {args.input_file}")
        print(f"  Output Directory:  {os.path.abspath(args.output_dir)}") # Show absolute path
        print(f"  Base Filename:     {args.base_name}")
        print(f"  Split Strategy:    {args.split_by}")
        print(f"  Split Value:       {args.value}")
        print(f"  JSON Path:         {args.path}")
        print(f"  Output Format:     {args.output_format}{' (Forced by key split)' if args.split_by == 'key' and args.output_format != 'jsonl' else ''}")
        print(f"  Max Records/Part:  {args.max_records if args.max_records is not None else 'None'}")
        print(f"  Max Size/Part:     {args.max_size if args.max_size is not None else 'None'}")
        if args.split_by == 'key':
            print(f"  On Missing Key:    {args.on_missing_key}")
            print(f"  On Invalid Item:   {args.on_invalid_item}")
        print(f"  Filename Format:   {args.filename_format}")
        print(f"  Report Interval:   {args.report_interval if args.report_interval > 0 else 'Disabled'}")
        print(f"  Verbose Logging:   {args.verbose}")
        print("="*40)

        confirm = _prompt_with_validation("🚀 Proceed with these settings?", choices=['y', 'n'], default='y')

        if confirm.lower() == 'y':
            log.info("\n✅ Configuration confirmed. Proceeding with splitting...")
            return args
        else:
            log.info("Operation cancelled by user.")
            sys.exit(0)

    except (KeyboardInterrupt, EOFError):
        # Already handled in _prompt_with_validation, but catch here too
        log.info("\nOperation cancelled during setup.")
        sys.exit(0)

def execute_split(args):
    """Instantiates and runs the appropriate splitter based on args."""
    log.info("Starting JSON splitting process...")
    created_files = set() # Track files for potential cleanup

    # Configure logging level based on args
    if args.verbose:
        log.setLevel(logging.DEBUG)
        log.debug("Verbose logging enabled.")
    else:
        log.setLevel(logging.INFO)

    # --- Input Validation (File Existence/Readability) ---
    # Directory validation/creation is now handled by _validate_output_dir
    if not os.path.isfile(args.input_file):
        log.error(f"Input file not found: {args.input_file}")
        return False
    if not os.access(args.input_file, os.R_OK):
        log.error(f"Input file not readable: {args.input_file}")
        return False

    # --- Prepare Splitter Arguments --- # Note: Some validation now in splitter __init__
    splitter_kwargs = {
        'input_file': args.input_file,
        'output_dir': args.output_dir,
        'base_name': args.base_name,
        'path': args.path,
        'output_format': args.output_format,
        'max_records': args.max_records,
        'max_size': args.max_size, # Pass size string
        'filename_format': args.filename_format,
        'verbose': args.verbose,
        'created_files_set': created_files,
        'report_interval': args.report_interval # Pass report_interval
    }

    splitter = None
    success = False
    try:
        if args.split_by == 'count':
            # Count validation happens in interactive or argparse type check/validator
            count_val = int(args.value)
            splitter = CountSplitter(count=count_val, **splitter_kwargs)

        elif args.split_by == 'size':
            # Size validation (parsing) happens in SplitterBase/SizeSplitter __init__
            splitter = SizeSplitter(size=args.value, **splitter_kwargs)

        elif args.split_by == 'key':
            # Pass key-specific args
            splitter_kwargs.update({
                'on_missing_key': args.on_missing_key,
                'on_invalid_item': args.on_invalid_item
            })
            splitter = KeySplitter(key_name=args.value, **splitter_kwargs)

        else:
             # Should be caught by argparse choices
            log.error(f"Internal error: Unknown split_by type '{args.split_by}'")
            return False

        # Pass the set to the splitter instance
        splitter.created_files_set = created_files

        success = splitter.split() # Execute the split

    except (ValueError, TypeError, KeyError, FileNotFoundError, MemoryError, ijson.common.JSONError) as e:
        log.error(f"Splitting pre-check or setup failed: {e}")
        success = False # Ensure success is false
    except Exception as e:
        log.exception(f"An unexpected error occurred before splitting could start: {e}")
        success = False # Ensure success is false

    # Handle result - REMOVED CLEANUP LOGIC
    if not success:
        # Log failure, but do not attempt cleanup
        log.error("Splitting process reported failure.") # Changed level to ERROR
        # The detailed error should have been logged within the splitter's method

        # Cleanup logic removed as per user request
        # log.warning("Splitting process failed. Attempting cleanup...")
        # cleaned_count = 0
        # Use the set passed to the splitter instance
        # files_to_check = splitter.created_files_set if splitter else created_files
        # for filename in files_to_check:
        #     try:
        #         if os.path.exists(filename):
        #             os.remove(filename)
        #             log.debug(f"  Removed potentially partial file: {filename}")
        #             cleaned_count += 1
        #     except OSError as rm_err:
        #         log.warning(f"  Could not remove partial file '{filename}': {rm_err}")
        #     except Exception as E:
        #          log.warning(f"  Unexpected error removing '{filename}': {E}")
        # log.warning(f"Cleaned up {cleaned_count} file(s).")

    if success:
        log.info("Splitting process completed successfully.")
    else:
        log.error("Splitting process failed or was terminated early.")

    return success

def main():
    """Parses arguments or runs interactive mode, then executes splitting."""
    # Argument Parser Setup
    parser = argparse.ArgumentParser(
        description="Split large JSON files using streaming.",
        formatter_class=argparse.RawTextHelpFormatter # Keep help text formatting
    )

    # --- Positional Arguments (Required for CLI) --- #
    parser.add_argument("input_file", nargs='?', default=None, # Optional for interactive mode
                        help="Path to the input JSON file.")
    # parser.add_argument("output_prefix", nargs='?', default=None,
    #                     help="Prefix for the output files (e.g., 'output/chunk').") # Removed positional

    # --- Core Splitting Options (Required for CLI) --- #
    parser.add_argument("--split-by", choices=['count', 'size', 'key'],
                        help="Criterion to split by.")
    parser.add_argument("--value", type=str,
                         help="Value for splitting criterion:\n"
                              "  count: Number of items (e.g., 10000)\n"
                              "  size: Approx size (e.g., 100MB, 1GB)\n"
                              "  key: JSON key name (e.g., user_id)")
    parser.add_argument("--path", help="JSON path to the array/objects to split (e.g., 'item', 'data.records.item').")

    # --- Common Optional Arguments --- #
    parser.add_argument("--output-format", choices=['json', 'jsonl'], default='json',
                        help="Output format. Default: json. (Note: 'key' split forces 'jsonl')")
    # Add --output-dir and --base-name
    parser.add_argument("--output-dir", type=str, default=".",
                        help="Directory to save output files (default: current directory).")
    parser.add_argument("--base-name", type=str, default="chunk",
                        help="Base name for output files (default: chunk).")
    parser.add_argument("--max-records", type=int, default=None,
                         help="Secondary constraint: Max records per output file part.")
    parser.add_argument("--max-size", type=str, default=None,
                         help="Secondary constraint: Max approx size per output file part (e.g., '50MB').")
    parser.add_argument("--filename-format", type=str, default=None, # Default handled based on split_by later
                         help="Format string for output filenames. Placeholders:\n"
                              "  {base_name} (provided base name, default 'chunk'),\n"
                              "  {type} ('chunk' or 'key'),\n"
                              "  {index} (number or key value), {part} (_part_XXXX),\n"
                              "  {ext} (json/jsonl). Default varies by split type.")
    parser.add_argument("-v", "--verbose", action="store_true",
                         help="Enable verbose debug logging.")
    # Add report interval argument
    parser.add_argument("--report-interval", type=int, default=10000,
                         help="How often to report progress (number of items). Set to 0 to disable. Default: 10000.")
    # Add config file argument
    parser.add_argument("--config", type=str, default=None,
                        help="Path to a YAML configuration file.")

    # --- Key Splitting Options --- #
    key_group = parser.add_argument_group('Key Splitting Options')
    key_group.add_argument("--on-missing-key", choices=['group', 'skip', 'error'], default='group',
                           help="Action for items missing the key (default: group into '__missing_key__' file).")
    key_group.add_argument("--on-invalid-item", choices=['warn', 'skip', 'error'], default='warn',
                            help="Action for items at path not being objects (default: warn and skip).")

    # --- Load Config File (if provided) and Set Defaults --- #
    # Parse only the --config argument first to load defaults
    config_parser = argparse.ArgumentParser(add_help=False) # Suppress help here
    config_parser.add_argument("--config", type=str, default=None)
    config_args, remaining_argv = config_parser.parse_known_args()

    config_values = {}
    if config_args.config:
        log.info(f"Loading configuration from: {config_args.config}")
        try:
            with open(config_args.config, 'r') as f:
                loaded_config = yaml.safe_load(f)
                if loaded_config:
                    config_values = loaded_config
                else:
                    log.warning(f"Configuration file '{config_args.config}' is empty.")
        except FileNotFoundError:
            log.error(f"Configuration file not found: {config_args.config}")
            sys.exit(1) # Exit if specified config not found
        except yaml.YAMLError as e:
            log.error(f"Error parsing configuration file '{config_args.config}': {e}")
            sys.exit(1) # Exit if config is invalid
        except Exception as e:
            log.error(f"An unexpected error occurred reading configuration file '{config_args.config}': {e}")
            sys.exit(1)

    # Set the loaded config values as defaults for the main parser
    # Any explicit CLI args parsed next will override these
    parser.set_defaults(**config_values)

    # --- Parse Arguments (using potentially updated defaults) --- #
    # args = parser.parse_args()
    # Parse the *remaining* arguments, now using the full parser
    # This ensures CLI args override config defaults
    args = parser.parse_args(remaining_argv)

    # --- Decide Mode: Interactive or CLI --- #
    # Run interactive mode only if specifically requested (no args) AND stdin is a TTY
    # AND no config file was provided
    # run_interactive = len(sys.argv) == 1 and sys.stdin.isatty()
    run_interactive = (len(sys.argv) == 1 and not config_args.config and sys.stdin.isatty())

    if run_interactive:
        # Fully interactive mode
        final_args = run_interactive_mode()
    else:
        # CLI Mode (or non-interactive execution like tests)
        # Check if required args for CLI were provided *by argparse*
        # If not, argparse should have already exited.
        # We primarily need to validate the *content* of args here.

        # Re-check core args presence in case called programmatically without full CLI args
        # but also not in interactive mode (e.g., tests missing args)
        is_missing_core_cli = not (args.input_file and args.split_by and args.value and args.path)
        if is_missing_core_cli and not run_interactive:
             # If not interactive and missing core args, it's an error
             # Construct the message manually as argparse might not have been triggered with full checks
             missing_required = []
             if not args.input_file: missing_required.append('input_file')
             # if not args.output_prefix: missing_required.append('output_prefix') # Removed
             if not args.split_by: missing_required.append('--split-by')
             if not args.value: missing_required.append('--value')
             if not args.path: missing_required.append('--path')
             parser.error(f"the following arguments are required in non-interactive mode: {', '.join(missing_required)}")

        # CLI Mode: argparse handles missing required args automatically by exiting.
        # We just need to validate the formats of provided args.

        # Validate split_by value format for CLI mode
        is_valid, msg_or_val = _validate_split_value(args.value, args.split_by)
        if not is_valid:
             parser.error(f"argument --value: {msg_or_val}")

        # Validate secondary constraints format if provided
        if args.max_size:
             is_valid, msg_or_val = _validate_optional_size(args.max_size)
             if not is_valid:
                 parser.error(f"argument --max-size: {msg_or_val}")

        # Set default filename format if not provided by user
        if args.filename_format is None:
             # Use {base_name} instead of {prefix}
             args.filename_format = "{base_name}_key_{index}{part}.{ext}" if args.split_by == 'key' else "{base_name}_{type}_{index:04d}{part}.{ext}"

        final_args = args

    # --- Execute Splitting with Final Args --- #
    if final_args:
        success = execute_split(final_args)
        sys.exit(0 if success else 1)
    else:
        # Should not happen if interactive mode exits properly
        log.info("Setup cancelled or failed.")
        sys.exit(1)

# Note: The main execution logic is now within this file.
# A separate main.py can simply import and call cli.main() 