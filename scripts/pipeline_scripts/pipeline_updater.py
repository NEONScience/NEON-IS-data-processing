#!/usr/bin/env python3
import os
import json
import argparse


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
            if file.endswith('.json'):
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
        with open(file) as json_file:
            json_data = json.load(json_file)
            specification_image = json_data['transform']['image']
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
