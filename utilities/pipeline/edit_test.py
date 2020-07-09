#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

import ruamel.yaml
from ruamel.yaml.util import load_yaml_guess_indent


def add(source_path: Path, image: str):
    """
    Add an environment variable.

    :param source_path: A path containing specification files.
    :param image: The container image to identify files to edit.
    """
    for path in source_path.rglob('*'):
        if path.is_file():
            if path.suffix == '.json':
                with open(str(path)) as json_file:
                    json_data = json.load(json_file)
                    specification_image = json_data['transform']['image']
                    if specification_image == image:
                        print(f'updating file {path}')
                        environment = json_data['transform']['env']
                        environment['NEW_SETTING'] = 'value'
                        json.dump(json_data, open(str(path), 'w'), indent=2)
            elif path.suffix == '.yaml':
                with open(str(path), 'r') as yaml_file:
                    data, indent, block_seq_indent = load_yaml_guess_indent(yaml_file, preserve_quotes=True)
                    specification_image = data['transform']['image']
                    if specification_image == image:
                        print(f'updating file {path}')
                        environment = data['transform']['env']
                        environment['NEW_SETTING'] = 'value'
                        ruamel.yaml.round_trip_dump(data, open(str(path), 'w'), explicit_start=True)


def remove(source_path: Path, image: str):
    """
    Remove an environment variable.

    :param source_path: A path containing specification files.
    :param image: The container image to identify files to edit.
    """
    for path in source_path.rglob('*'):
        if path.is_file():
            if path.suffix == '.json':
                with open(str(path)) as json_file:
                    json_data = json.load(json_file)
                    specification_image = json_data['transform']['image']
                    if specification_image == image:
                        print(f'updating file {path}')
                        environment = json_data['transform']['env']
                        del environment['NEW_SETTING']
                        json.dump(json_data, open(str(path), 'w'), indent=2)
            elif path.suffix == '.yaml':
                with open(str(path), 'r') as open_file:
                    data, indent, block_seq_indent = load_yaml_guess_indent(
                        open_file, preserve_quotes=True)
                    specification_image = data['transform']['image']
                    if specification_image == image:
                        print(f'updating file {path}')
                        environment = data['transform']['env']
                        del environment['NEW_SETTING']
                        ruamel.yaml.round_trip_dump(data, open(str(path), 'w'), explicit_start=True)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--spec_path')
    arg_parser.add_argument('--image')
    args = arg_parser.parse_args()
    add(Path(args.spec_path), args.image)
