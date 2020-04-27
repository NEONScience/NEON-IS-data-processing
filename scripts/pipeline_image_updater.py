#!/usr/bin/env python3
import os
import json
import argparse


def update_image(specification_path, old_image, new_image):
    """
    Update to the new image any pipeline specifications using the old image.

    :param specification_path: The path for the specification files to search.
    :type specification_path: str
    :param old_image: The old image to replace.
    :type old_image: str
    :param new_image: The new image.
    :type new_image: str
    :return:
    """
    for root, dirs, files in os.walk(specification_path):
        for file in files:
            if file.endswith('.json'):
                specification_file = os.path.join(root, file)
                with open(specification_file) as json_file:
                    json_data = json.load(json_file)
                    image = json_data['transform']['image']
                    if image == old_image:
                        print(f'updating file {specification_file}')
                        json_data['transform']['image'] = new_image
                        json.dump(json_data, open(specification_file, 'w'), indent=2)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--spec_path')
    arg_parser.add_argument('--old_image')
    arg_parser.add_argument('--new_image')
    args = arg_parser.parse_args()
    update_image(args.spec_path, args.old_image, args.new_image)
