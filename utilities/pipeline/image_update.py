#!/usr/bin/env python3
import argparse
from pathlib import Path
import json
import ruamel.yaml
from ruamel.yaml.util import load_yaml_guess_indent


def update(source_path: Path, old_image: str, new_image: str):
    """
    Update to the new image all pipeline specifications using the old image.

    :param source_path: Path containing specification files.
    :param old_image: The image to replace.
    :param new_image: The replacement image.
    """
    for path in source_path.rglob('*'):
        if path.is_file():
            if path.suffix == '.json':
                with open(str(path)) as json_file:
                    json_data = json.load(json_file)
                    image = json_data['transform']['image']
                    if image == old_image:
                        print(f'updating file {path}')
                        json_data['transform']['image'] = new_image
                        json.dump(json_data, open(str(path), 'w'), indent=2)
            elif path.suffix == '.yaml':
                with open(str(path), 'r') as open_file:
                    data, indent, block_seq_indent = load_yaml_guess_indent(
                        open_file, preserve_quotes=True)
                    image = data['transform']['image']
                    if image == old_image:
                        print(f'updating file {path}')
                        data['transform']['image'] = new_image
                        ruamel.yaml.round_trip_dump(data, open(
                            str(path), 'w'), explicit_start=True)


if __name__ == '__main__':
    """
    Example usage:
    python3 -B image_update.py
        --spec_path=/Users/home/git/NEON-IS-data-processing/pipe
        --old_image=quay.io/battelleecology/parquet_linkmerge:14
        --new_image=quay.io/battelleecology/parquet_linkmerge:15
    """
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--spec_path')
    arg_parser.add_argument('--old_image')
    arg_parser.add_argument('--new_image')
    args = arg_parser.parse_args()
    update(Path(args.spec_path), args.old_image, args.new_image)
