#!/usr/bin/env python3
import os
import json
import argparse


def get_files(specification_path):
    files = []
    for root, dirs, files in os.walk(specification_path):
        for file in files:
            if file.endswith('.json'):
                specification_file = os.path.join(root, file)
                files.append(specification_file)


def process_files(files, image, reprocess):
    """
    Update any pipelines using the image.

    :param specification_path: The path for the specification files to search.
    :type specification_path: str
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
    files = get_files(args.spec_path)
    if args.reprocess == 'true':
        process_files(args.spec_path, args.image, True)
    else:
        process_files(args.spec_path, args.image, False)
