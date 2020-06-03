#!/usr/bin/env python3
import argparse
import os
import json
import ruamel.yaml
from ruamel.yaml.util import load_yaml_guess_indent


def update(specification_path, old_image, new_image):
    """
    Update to the new image all pipeline specifications using the old image.

    :param specification_path: The specification file path.
    :type specification_path: str
    :param old_image: The image to replace.
    :type old_image: str
    :param new_image: The replacement image.
    :type new_image: str
    :return:
    """
    for root, dirs, files in os.walk(specification_path):
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith('.json'):
                with open(file_path) as json_file:
                    json_data = json.load(json_file)
                    image = json_data['transform']['image']
                    if image == old_image:
                        print(f'updating file {file_path}')
                        json_data['transform']['image'] = new_image
                        json.dump(json_data, open(file_path, 'w'), indent=2)
            elif file.endswith('.yaml'):
                with open(file_path, 'r') as open_file:
                    data, indent, block_seq_indent = load_yaml_guess_indent(
                        open_file, preserve_quotes=True)
                    image = data['transform']['image']
                    if image == old_image:
                        print(f'updating file {file}')
                        data['transform']['image'] = new_image
                        ruamel.yaml.round_trip_dump(data, open(
                            file_path, 'w'), explicit_start=True)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--spec_path')
    arg_parser.add_argument('--old_image')
    arg_parser.add_argument('--new_image')
    args = arg_parser.parse_args()
    update(args.spec_path, args.old_image, args.new_image)
