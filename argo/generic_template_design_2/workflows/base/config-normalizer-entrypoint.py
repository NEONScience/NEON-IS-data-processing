#!/usr/bin/env python3
"""
Configuration normalizer for calibration-group-and-convert workflow.

This script reads a structured YAML configuration file and generates
environment-specific files for each container in the workflow.
This abstracts platform-specific paths and configuration formats
from the workflow template, making it portable across orchestrators.
"""

import os
import sys
import yaml
from pathlib import Path


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


def generate_load_data_env(config: dict, output_dir: str) -> None:
    """Generate environment file for load-data container."""
    data_config = config.get('data_loading', {})
    workflow_config = config.get('workflow', {})

    env_vars = {
        'BUCKET_NAME': data_config.get('l0_bucket_name'),
        'BUCKET_VERSION_PATH': data_config.get('l0_bucket_version_path'),
        'CAL_BUCKET_NAME': data_config.get('calibration_bucket_name'),
        'CAL_BUCKET_PREFIX': data_config.get('calibration_bucket_prefix'),
        'SOURCE_TYPE_INDEX': str(data_config.get('source_type_index', 0)),
        'YEAR_INDEX': str(data_config.get('year_index', 1)),
        'MONTH_INDEX': str(data_config.get('month_index', 2)),
        'DAY_INDEX': str(data_config.get('day_index', 3)),
        'SOURCE_ID_INDEX': str(data_config.get('source_id_index', 4)),
        'OUT_PATH': data_config.get('out_path_l0', '/inputs/DATA_PATH_ARCHIVE'),
        'OUT_PATH_CAL': data_config.get('out_path_calibration', '/inputs/CALIBRATION_PATH'),
        'LOG_LEVEL': workflow_config.get('log_level', 'INFO'),
    }

    output_file = Path(output_dir) / 'load-data.env'
    with open(output_file, 'w') as f:
        for key, value in env_vars.items():
            f.write(f'export {key}="{value}"\n')
    print(f"Generated {output_file}")


def generate_calibration_group_and_convert_env(config: dict, output_dir: str) -> None:
    """Generate environment file for calibration-group-and-convert container."""
    processing_config = config.get('processing', {})
    workflow_config = config.get('workflow', {})
    schemas = config.get('schemas', {})

    env_vars = {
        'CONFIG': processing_config.get('filter_joiner_config', ''),
        'OUT_PATH_JOINER': processing_config.get('out_path_joiner', '/pfs/data_cal_joined'),
        'OUT_PATH_KAFKA_COMB': processing_config.get('out_path_kafka_comb', '/pfs/kafka_combined'),
        'OUT_PATH_CAL_CONV': processing_config.get('out_path_calibration_conversion'),
        'ERR_PATH': workflow_config.get('error_path', '/pfs/errored_datums'),
        'LOG_LEVEL': workflow_config.get('log_level', 'INFO'),
        'RELATIVE_PATH_INDEX': str(processing_config.get('relative_path_index', 3)),
        'LINK_TYPE': processing_config.get('link_type', 'SYMLINK'),
        'PARALLELISM_INTERNAL': str(processing_config.get('parallelism_internal', 3)),
        'KFKA_COMB_R_ARGS': processing_config.get('kafka_combine_r_args', ''),
        'CAL_CONV_R_ARGS': processing_config.get('calibration_conversion_r_args', ''),
    }

    output_file = Path(output_dir) / 'calibration-group-and-convert.env'
    with open(output_file, 'w') as f:
        for key, value in env_vars.items():
            f.write(f'export {key}="{value}"\n')
    print(f"Generated {output_file}")


def generate_data_upload_env(config: dict, output_dir: str) -> None:
    """Generate environment file for data upload (main) container."""
    output_config = config.get('data_output', {})

    env_vars = {
        'OUT_PATH': output_config.get('out_path', '/pfs/out'),
        'OUTPUT_BUCKET_NAME': output_config.get('output_bucket_name'),
        'OUTPUT_BUCKET_PREFIX': output_config.get('output_bucket_prefix'),
    }

    output_file = Path(output_dir) / 'data-upload.env'
    with open(output_file, 'w') as f:
        for key, value in env_vars.items():
            f.write(f'export {key}="{value}"\n')
    print(f"Generated {output_file}")


def main():
    config_input_file = os.getenv('CONFIG_INPUT_FILE', '/etc/config-in/config.yaml')
    config_output_dir = os.getenv('CONFIG_OUTPUT_DIR', '/etc/config-out')

    # Ensure output directory exists
    Path(config_output_dir).mkdir(parents=True, exist_ok=True)

    # Load configuration
    config = load_config(config_input_file)

    # Generate environment files for each container
    generate_load_data_env(config, config_output_dir)
    generate_calibration_group_and_convert_env(config, config_output_dir)
    generate_data_upload_env(config, config_output_dir)

    print("Configuration normalization completed successfully.")


if __name__ == '__main__':
    main()
