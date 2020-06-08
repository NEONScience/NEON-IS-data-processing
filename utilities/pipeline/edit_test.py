#!/usr/bin/env python3
import argparse
import os
import json
import ruamel.yaml
from ruamel.yaml.util import load_yaml_guess_indent


def add(path, image):
    """
    Update to the new image any pipeline specifications using the old image.

    :param path: A path containing specification files.
    :type path: str
    :param image: The container image to identify files to edit.
    :type image: str
    :return:
    """
    for root, dirs, files in os.walk(path):
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith('.json'):
                with open(file_path) as json_file:
                    json_data = json.load(json_file)
                    specification_image = json_data['transform']['image']
                    if specification_image == image:
                        print(f'updating file {file_path}')
                        environment = json_data['transform']['env']
                        environment['NEW_SETTING'] = 'value'
                        json.dump(json_data, open(file_path, 'w'), indent=2)
            elif file.endswith('.yaml'):
                with open(file_path, 'r') as open_file:
                    data, indent, block_seq_indent = load_yaml_guess_indent(
                        open_file, preserve_quotes=True)
                    specification_image = data['transform']['image']
                    if specification_image == image:
                        print(f'updating file {file}')
                        environment = data['transform']['env']
                        environment['NEW_SETTING'] = 'value'
                        ruamel.yaml.round_trip_dump(data, open(file_path, 'w'), explicit_start=True)


def remove(path, image):
    """
    Update to the new image any pipeline specifications using the old image.

    :param path: A path containing specification files.
    :type path: str
    :param image: The container image to identify files to edit.
    :type image: str
    :return:
    """
    for root, dirs, files in os.walk(path):
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith('.json'):
                with open(file_path) as json_file:
                    json_data = json.load(json_file)
                    specification_image = json_data['transform']['image']
                    if specification_image == image:
                        print(f'updating file {file_path}')
                        environment = json_data['transform']['env']
                        del environment['NEW_SETTING']
                        json.dump(json_data, open(file_path, 'w'), indent=2)
            elif file.endswith('.yaml'):
                with open(file_path, 'r') as open_file:
                    data, indent, block_seq_indent = load_yaml_guess_indent(
                        open_file, preserve_quotes=True)
                    specification_image = data['transform']['image']
                    if specification_image == image:
                        print(f'updating file {file}')
                        environment = data['transform']['env']
                        del environment['NEW_SETTING']
                        ruamel.yaml.round_trip_dump(data, open(file_path, 'w'), explicit_start=True)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--spec_path')
    arg_parser.add_argument('--image')
    args = arg_parser.parse_args()
    add(args.spec_path, args.image)
