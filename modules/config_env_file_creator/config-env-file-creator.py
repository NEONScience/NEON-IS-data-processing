#!/usr/bin/env python3
"""
Configuration environment file creator.

This script reads a structured YAML configuration file and generates
one environment file per top-level section, named <section>.env.
Each key:value pair in a section becomes an exported shell variable
whose name is the key exactly as written.

Sections to skip are controlled by the EXCLUDED_SECTIONS environment
variable (comma-separated list; defaults to DEFAULT_EXCLUDED_SECTIONS).
"""

import os
import sys
import yaml
from pathlib import Path

DEFAULT_EXCLUDED_SECTIONS = None  # Example: 'schemas'


def load_config(config_file: str) -> dict:
    """Load YAML configuration file."""
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        print(f"ERROR: Configuration file not found: {config_file}", file=sys.stderr)
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"ERROR: Failed to parse YAML configuration: {e}", file=sys.stderr)
        sys.exit(1)


def write_env_file(section_name: str, env_vars: dict, output_dir: str) -> None:
    """Write environment variables to a shell-sourceable .env file."""
    output_file = Path(output_dir) / f'{section_name}.env'
    with open(output_file, 'w') as f:
        for key, value in env_vars.items():
            # Escape embedded double-quotes in the value
            escaped = str(value).replace('"', '\\"')
            f.write(f'export {key}="{escaped}"\n')
    print(f"Generated {output_file}")


def main():
    config_input_file = os.getenv('CONFIG_INPUT_FILE', '/etc/config-in/config-env.yaml')
    config_output_dir = os.getenv('CONFIG_OUTPUT_DIR', '/etc/config-out')
    excluded_raw = os.getenv('EXCLUDED_SECTIONS')
    if excluded_raw is None:
        excluded_raw = DEFAULT_EXCLUDED_SECTIONS
    excluded_sections = (
        {s.strip() for s in excluded_raw.split(',') if s.strip()}
        if excluded_raw is not None
        else set()
    )

    # Ensure output directory exists
    Path(config_output_dir).mkdir(parents=True, exist_ok=True)

    # Load configuration
    config = load_config(config_input_file)

    # Generate one .env file per non-excluded section
    for section_name, section_data in config.items():
        if section_name in excluded_sections:
            print(f"Skipping excluded section: {section_name}")
            continue
        if not isinstance(section_data, dict):
            print(f"Skipping non-mapping section: {section_name}")
            continue
        write_env_file(section_name, section_data, config_output_dir)

    print("Configuration normalization completed successfully.")


if __name__ == '__main__':
    main()
