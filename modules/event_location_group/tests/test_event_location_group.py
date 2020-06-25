#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import event_location_group.event_location_group_main as event_location_group_main
from event_location_group.data_file_path import DataFilePath
from event_location_group.event_location_grouper import EventLocationGrouper


class EventLocationGroupTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.source_id = '00001'
        # input/output paths
        self.out_path = Path('/out')
        self.data_path = Path('/repo/events/heater/2019/01/01', self.source_id)
        self.location_path = Path('/location')
        # create data file
        self.data_file = f'heater_{self.source_id}_events_2019-01-01.json'
        self.input_data_path = Path(self.data_path, self.data_file)
        self.fs.create_file(self.input_data_path)
        # create location file
        self.location_file = f'heater_{self.source_id}_locations.json'
        self.input_location_path = Path(self.location_path, 'heater', self.source_id, self.location_file)
        self.fs.create_file(self.input_location_path)
        # create output directory
        self.fs.create_dir(self.out_path)
        # path indices
        self.source_type_index = 3
        self.year_index = 4
        self.month_index = 5
        self.day_index = 6
        self.source_id_index = 7

    def test_group(self):
        data_file_path = DataFilePath(source_type_index=self.source_type_index,
                                      year_index=self.year_index,
                                      month_index=self.month_index,
                                      day_index=self.day_index,
                                      source_id_index=self.source_id_index)
        event_location_grouper = EventLocationGrouper(data_path=self.data_path,
                                                      location_path=self.location_path,
                                                      out_path=self.out_path,
                                                      data_file_path=data_file_path)
        event_location_grouper.group_files()
        self.check_output()

    def test_main(self):
        os.environ['DATA_PATH'] = str(self.data_path)
        os.environ['LOCATION_PATH'] = str(self.location_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = str(self.source_type_index)
        os.environ['YEAR_INDEX'] = str(self.year_index)
        os.environ['MONTH_INDEX'] = str(self.month_index)
        os.environ['DAY_INDEX'] = str(self.day_index)
        os.environ['SOURCE_ID_INDEX'] = str(self.source_id_index)
        event_location_group_main.main()
        self.check_output()

    def check_output(self):
        root_path = Path(self.out_path, 'heater/2019/01/01/00001')
        output_data_path = Path(root_path, 'data', self.data_file)
        output_location_path = Path(root_path, 'location', self.location_file)
        self.assertTrue(output_data_path.exists())
        self.assertTrue(output_location_path.exists())
