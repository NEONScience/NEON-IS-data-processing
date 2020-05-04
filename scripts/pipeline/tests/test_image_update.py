#!/usr/bin/env python3
import os
import yaml
import json

import unittest

import image_update


class ParserTest(unittest.TestCase):

    def setUp(self):
        self.pipeline_root = os.path.join(os.path.dirname(__file__), 'test_files')
        self.yaml_path = os.path.join(self.pipeline_root, 'pipeline.yaml')
        self.json_path = os.path.join(self.pipeline_root, 'pipeline.json')

    def test_update_image(self):
        old_image = 'quay.io/battelleecology/file_joiner:7'
        new_image = 'quay.io/battelleecology/file_joiner:8'
        # update files to new image
        image_update.update(self.pipeline_root, old_image, new_image)
        with open(self.json_path) as json_file:
            json_data = json.load(json_file)
            image = json_data['transform']['image']
            self.assertTrue(image == new_image)
        with open(self.yaml_path) as open_file:
            file_data = yaml.load(open_file, Loader=yaml.FullLoader)
            image = file_data['transform']['image']
            self.assertTrue(image == new_image)
        # restore files to original state
        image_update.update(self.pipeline_root, new_image, old_image)
        with open(self.json_path) as json_file:
            json_data = json.load(json_file)
            image = json_data['transform']['image']
            self.assertTrue(image == old_image)
        with open(self.yaml_path) as open_file:
            file_data = yaml.load(open_file, Loader=yaml.FullLoader)
            image = file_data['transform']['image']
            self.assertTrue(image == old_image)


if __name__ == '__main__':
    unittest.main()
