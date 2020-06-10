#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import event_asset_loader.event_asset_loader_main as event_asset_loader_main
from event_asset_loader.event_asset_loader import EventAssetLoader


class EventAssetLoaderTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.source_id = '0001'
        self.location_file = f'heater_{self.source_id}_locations.json'
        self.source_path = Path('/inputs/repo/heater', self.source_id, self.location_file)
        self.out_path = Path('/outputs/repo')
        #  Create data input file.
        self.fs.create_file(self.source_path)
        #  Create output directory.
        self.fs.create_dir(self.out_path)
        self.source_type_index = 3
        self.source_id_index = 4
        self.filename_index = 5

    def test_event_asset_loader(self):
        event_asset_loader = EventAssetLoader(source_path=self.source_path,
                                              out_path=self.out_path,
                                              source_type_index=self.source_type_index,
                                              source_id_index=self.source_id_index,
                                              filename_index=self.filename_index)
        event_asset_loader.link_event_files()
        self.check_output()

    def test_main(self):
        os.environ['SOURCE_PATH'] = str(self.source_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = str(self.source_type_index)
        os.environ['SOURCE_ID_INDEX'] = str(self.source_id_index)
        os.environ['FILENAME_INDEX'] = str(self.filename_index)
        event_asset_loader_main.main()
        self.check_output()

    def check_output(self):
        self.output_path = Path(self.out_path, 'heater', self.source_id, f'heater_{self.source_id}_events.json')
        self.assertTrue(self.output_path.exists())
