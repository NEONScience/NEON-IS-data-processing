#!/usr/bin/env python3
import os
from typing import Iterator
from pathlib import Path
import json
import yaml
import argparse


def get_paths(source_path: Path) -> Iterator[Path]:
    """
    Get all specification files in the path.

    :param source_path: A path containing specification files.
    """
    for path in source_path.rglob('*'):
        if path.is_file():
            if path.suffix == '.json' or path.suffix == '.yaml':
                yield path


def process_paths(source_path: Path, image: str, reprocess: bool):
    """
    Update pipelines using the image.

    :param source_path: A path containing specification files.
    :param image: The image string to match.
    :param reprocess: True to reprocess files.
    """
    for path in get_paths(source_path):
        with open(str(path)) as open_file:
            if path.suffix == '.json':
                file_data = json.load(open_file)
            elif path.suffix == '.yaml':
                file_data = yaml.load(open_file, Loader=yaml.FullLoader)
            process_path(image, path, file_data, reprocess)


def process_path(image, path, file_data, reprocess):
    file_image = file_data['transform']['image']
    if file_image == image:
        print(f'updating pipeline {path}')
        if reprocess:
            command = f'pachctl update pipeline --reprocess -f {path}'
        else:
            command = f'pachctl update pipeline -f {path}'
        print(f'executing: {command}')
        os.system(command)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--spec_path')
    arg_parser.add_argument('--image')
    arg_parser.add_argument('--reprocess', default=False)
    args = arg_parser.parse_args()
    if args.reprocess == 'true':
        process_paths(Path(args.spec_path), args.image, True)
    else:
        process_paths(Path(args.spec_path), args.image, False)
