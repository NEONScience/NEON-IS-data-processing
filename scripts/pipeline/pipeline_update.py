#!/usr/bin/env python3
import os
import json
import argparse
import yaml


def get_files(specification_path):
    """
    Get the specification files in the given directory and any subdirectories.

    :param specification_path: A path to a directory or subdirectories
     containing specification files.
    :return:
    """
    specification_files = []
    for root, dirs, files in os.walk(specification_path):
        for file in files:
            if file.endswith('.json') or file.endswith('.yaml'):
                specification_file = os.path.join(root, file)
                specification_files.append(specification_file)
    return specification_files


def process_files(files, image, reprocess):
    """
    Update any pipelines using the image.

    :param files: The specification files.
    :type files: list
    :param image: The image string to match.
    :type image: str
    :param reprocess: Set 'True' to reprocess files.
    :type reprocess: bool
    :return:
    """
    for file in files:
        with open(file) as open_file:
            if file.endswith('.json'):
                file_data = json.load(open_file)
            elif file.endswith('.yaml'):
                file_data = yaml.load(open_file, Loader=yaml.FullLoader)
            process_file(image, file, file_data, reprocess)


def process_file(image, file, file_data, reprocess):
    specification_image = file_data['transform']['image']
    if specification_image == image:
        print(f'updating pipeline {file}')
        if reprocess:
            command = f'pachctl update pipeline --reprocess -f {file}'
        else:
            command = f'pachctl update pipeline -f {file}'
        print(f'executing: {command}')
        os.system(command)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--spec_path')
    arg_parser.add_argument('--image')
    arg_parser.add_argument('--reprocess', default=False)
    args = arg_parser.parse_args()
    spec_files = get_files(args.spec_path)
    if args.reprocess == 'true':
        process_files(spec_files, args.image, True)
    else:
        process_files(spec_files, args.image, False)
