#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import group_path.group_path_main as group_path_main
from group_path.group_path_config import Config
from group_path.group_path import GroupPath


class GroupPathTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.group_member_location = 'CFGLOC110723'
        self.group_member_group = 'rel-humidity_HARV003000'
        # The group to find in the test group member file.
        self.group = 'pressure-air'
        self.group_assignment_path = Path('/pfs/group_assignment')
        self.location_focus_path = Path('/pfs/location_focus')
        self.group_focus_path = Path('/pfs/group_focus')
        self.out_path = Path('/pfs/out')
        self.err_path = Path('/pfs/out/errored')
        self.location_focus_source_type = 'ptb330a'
        self.date_path_1 = Path('2020/01/01')
        inputs_group_assignment = Path(self.group_assignment_path, self.group, self.date_path_1)
        #inputs_location_focus = Path(self.location_focus_path, 'repo', self.location_focus_source_type, self.date_path_1)
        inputs_location_focus = Path(self.location_focus_path, self.location_focus_source_type, self.date_path_1)
        inputs_group_focus = Path(self.group_focus_path, self.date_path_1,)
        group_assignment_member_location_path = Path(inputs_group_assignment, self.group_member_location, 'group/group-member-location.json')
        group_assignment_member_group_path = Path(inputs_group_assignment, self.group_member_group, 'group/group-member-group.json')
        location_focus_data_path = Path(inputs_location_focus, self.group_member_location, 'data/data.ext')
        location_focus_flags_path = Path(inputs_location_focus, self.group_member_location, 'flags/flags.ext')
        group_focus_stats_path = Path(inputs_group_focus, self.group_member_group, 'stats/stats.ext')
        self.fs.create_file(location_focus_data_path)
        self.fs.create_file(location_focus_flags_path)
        self.fs.create_file(group_focus_stats_path)
        # use real group member file for parsing
        actual_group_assignment_member_location_path = Path(os.path.dirname(__file__), 'test-group-member-location.json')
        actual_group_assignment_member_group_path = Path(os.path.dirname(__file__), 'test-group-member-group.json')
        self.fs.add_real_file(actual_group_assignment_member_location_path, target_path=group_assignment_member_location_path)
        self.fs.add_real_file(actual_group_assignment_member_group_path, target_path=group_assignment_member_group_path)
        # path indices
        self.group_assignment_year_index = 4
        self.group_assignment_month_index = 5
        self.group_assignment_day_index = 6
        self.group_assignment_member_index = 7
        self.group_assignment_data_type_index = 8
        self.location_focus_source_type_index = 3
        self.location_focus_year_index = 4
        self.location_focus_month_index = 5
        self.location_focus_day_index = 6
        self.location_focus_location_index = 7
        self.location_focus_data_type_index = 8
        self.group_focus_year_index = 3
        self.group_focus_month_index = 4
        self.group_focus_day_index = 5
        self.group_focus_group_index = 6

    def test_add_groups_to_paths(self):
        config = Config(group_assignment_path=self.group_assignment_path,
                        location_focus_path=self.location_focus_path,
                        group_focus_path=self.group_focus_path,
                        out_path=self.out_path,
                        err_path=self.err_path,
                        group=self.group,
                        group_assignment_year_index=self.group_assignment_year_index,
                        group_assignment_month_index=self.group_assignment_month_index,
                        group_assignment_day_index=self.group_assignment_day_index,
                        group_assignment_member_index=self.group_assignment_member_index,
                        group_assignment_data_type_index=self.group_assignment_data_type_index,
                        location_focus_source_type_index=self.location_focus_source_type_index,
                        location_focus_year_index=self.location_focus_year_index,
                        location_focus_month_index=self.location_focus_month_index,
                        location_focus_day_index=self.location_focus_day_index,
                        location_focus_location_index=self.location_focus_location_index,
                        group_focus_year_index=self.group_focus_year_index,
                        group_focus_month_index=self.group_focus_month_index,
                        group_focus_day_index=self.group_focus_day_index,
                        group_focus_group_index=self.group_focus_group_index)
        group_path = GroupPath(config)
        group_path.add_groups_to_paths()
        self.check_output()

    def test_main(self):
        os.environ['GROUP_ASSIGNMENT_PATH'] = str(self.group_assignment_path)
        os.environ['LOCATION_FOCUS_PATH'] = str(self.location_focus_path)
        os.environ['GROUP_FOCUS_PATH'] = str(self.group_focus_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['ERR_PATH'] = str(self.err_path)
        os.environ['GROUP'] = self.group
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['GROUP_ASSIGNMENT_YEAR_INDEX'] = str(self.group_assignment_year_index)
        os.environ['GROUP_ASSIGNMENT_MONTH_INDEX'] = str(self.group_assignment_month_index)
        os.environ['GROUP_ASSIGNMENT_DAY_INDEX'] = str(self.group_assignment_day_index)
        os.environ['GROUP_ASSIGNMENT_MEMBER_INDEX'] = str(self.group_assignment_member_index)
        os.environ['GROUP_ASSIGNMENT_DATA_TYPE_INDEX'] = str(self.group_assignment_data_type_index)
        os.environ['LOCATION_FOCUS_SOURCE_TYPE_INDEX'] = str(self.location_focus_source_type_index)
        os.environ['LOCATION_FOCUS_YEAR_INDEX'] = str(self.location_focus_year_index)
        os.environ['LOCATION_FOCUS_MONTH_INDEX'] = str(self.location_focus_month_index)
        os.environ['LOCATION_FOCUS_DAY_INDEX'] = str(self.location_focus_day_index)
        os.environ['LOCATION_FOCUS_LOCATION_INDEX'] = str(self.location_focus_location_index)
        os.environ['LOCATION_FOCUS_DATA_TYPE_INDEX'] = str(self.location_focus_data_type_index)
        os.environ['GROUP_FOCUS_YEAR_INDEX'] = str(self.group_focus_year_index)
        os.environ['GROUP_FOCUS_MONTH_INDEX'] = str(self.group_focus_month_index)
        os.environ['GROUP_FOCUS_DAY_INDEX'] = str(self.group_focus_day_index)
        os.environ['GROUP_FOCUS_GROUP_INDEX'] = str(self.group_focus_group_index)
        group_path_main.main()
        self.check_output()

    def check_output(self):
        root_path_1 = Path(self.out_path, self.date_path_1, 'pressure-air_BARC200')
        data_path_1 = Path(root_path_1, self.location_focus_source_type, self.group_member_location, 'data/data.ext')
        flags_path_1 = Path(root_path_1, self.location_focus_source_type, self.group_member_location, 'flags/flags.ext')
        group_path_1 = Path(root_path_1, 'group/group-member-location.json')
        root_path_2 = Path(self.out_path, self.date_path_1, 'pressure-air_HARV000')
        stats_path_2 = Path(root_path_2, self.group_member_group, 'stats/stats.ext')
        group_path_2 = Path(root_path_2, 'group/group-member-group.json')
        root_path_3 = Path(self.out_path, self.date_path_1, 'pressure-air_HARV000060')
        stats_path_3 = Path(root_path_3, self.group_member_group, 'stats/stats.ext')
        group_path_3 = Path(root_path_3, 'group/group-member-group.json')
        self.assertTrue(data_path_1.exists())
        self.assertTrue(flags_path_1.exists())
        self.assertTrue(group_path_1.exists())
        self.assertTrue(stats_path_2.exists())
        self.assertTrue(group_path_2.exists())
        self.assertTrue(stats_path_3.exists())
        self.assertTrue(group_path_3.exists())
