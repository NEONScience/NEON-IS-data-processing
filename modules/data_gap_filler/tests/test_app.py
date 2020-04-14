#!/usr/bin/env python3
import os

from pyfakefs.fake_filesystem_unittest import TestCase

import data_gap_filler.app as app
from lib import log_config as log_config


class AppTest(TestCase):

    def setUp(self):
        # logging
        log_config.configure('DEBUG')
        # location
        self.location_name = 'SENSOR000000'
        # initialize fake file system
        self.setUpPyfakefs()
        #  create output directory
        self.out_path = os.path.join('/', 'outputs', 'repo')
        self.fs.create_dir(self.out_path)
        #  create data repo
        self.create_data_repo()
        #  create location by date repo
        self.create_location_repo()
        # create empty files repo
        self.create_empty_files_repo()
        # directory names to output
        self.output_directories = 'data,location,calibration,uncertainty_data,uncertainty_coef,flags'

    def create_data_repo(self):
        self.data_path = os.path.join('/', 'files', 'repo_name', 'exo2', '2020', '01')
        self.data_file_1 = os.path.join('02', self.location_name, 'data',
                                        'exo2_' + self.location_name + '_2020-01-02.ext')
        self.flags_file_1 = os.path.join('02', self.location_name, 'flags',
                                         'exo2_' + self.location_name + '_2020-01-02_flagsCal.ext')
        self.location_file_1 = os.path.join('02', self.location_name, 'location',
                                            'exo2_' + self.location_name + '_locations.json')
        self.uncertainty_coefficient_file_1 = os.path.join('02', self.location_name, 'uncertainty_coef',
                                                           'exo2_' + self.location_name
                                                           + '_2020-01-02_uncertaintyCoef.json')
        self.uncertainty_file_1 = os.path.join('02', self.location_name, 'uncertainty_data',
                                               'exo2_' + self.location_name + '_2020-01-02_uncertaintyData.ext')
        self.fs.create_file(os.path.join(self.data_path, self.data_file_1))
        self.fs.create_file(os.path.join(self.data_path, self.flags_file_1))
        self.fs.create_file(os.path.join(self.data_path, self.location_file_1))
        self.fs.create_file(os.path.join(self.data_path, self.uncertainty_coefficient_file_1))
        self.fs.create_file(os.path.join(self.data_path, self.uncertainty_file_1))

    def create_location_repo(self):
        self.location_path = os.path.join('/', 'locations', 'repo_name', 'exo2', '2020', '01')
        self.location_file_1 = os.path.join(self.location_path, '01', self.location_name, self.location_name + '.json')
        self.fs.create_file(self.location_file_1)
        self.location_file_2 = os.path.join(self.location_path, '02', self.location_name, self.location_name + '.json')
        self.fs.create_file(self.location_file_2)
        self.location_file_3 = os.path.join(self.location_path, '03', self.location_name, self.location_name + '.json')
        self.fs.create_file(self.location_file_3)

    def create_empty_files_repo(self):
        self.empty_files_path = os.path.join('/', 'empty_files', 'repo_name', 'exo2')
        # data
        self.empty_data_path = os.path.join(self.empty_files_path, 'data')
        self.empty_data_file = os.path.join(self.empty_data_path, 'exo2_location_year-month-day.ext')
        self.fs.create_file(self.empty_data_file)
        # uncertainty data
        self.empty_uncertainty_data_path = os.path.join(self.empty_files_path, 'uncertainty_data')
        self.empty_uncertainty_data_file = \
            os.path.join(self.empty_uncertainty_data_path, 'exo2_location_year-month-day_uncertaintyData.ext')
        self.fs.create_file(self.empty_uncertainty_data_file)
        # flags
        self.empty_flags_path = os.path.join(self.empty_files_path, 'flags')
        self.empty_flags_file = os.path.join(self.empty_flags_path, 'exo2_location_year-month-day_flagsCal.ext')
        self.fs.create_file(self.empty_flags_file)

    def test_main(self):
        os.environ['DATA_PATH'] = self.data_path
        os.environ['LOCATION_PATH'] = self.location_path
        os.environ['EMPTY_FILES_PATH'] = self.empty_files_path
        os.environ['OUTPUT_DIRECTORIES'] = self.output_directories
        os.environ['OUT_PATH'] = self.out_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['START_DATE'] = '2019-12-31'
        os.environ['END_DATE'] = '2020-01-04'
        app.main()
        self.check_output()

    def check_output(self):
        root_path = os.path.join(self.out_path, 'exo2', '2020', '01')
        self.assertTrue(os.path.lexists(os.path.join(root_path, self.data_file_1)))
        self.assertTrue(os.path.lexists(os.path.join(root_path, self.flags_file_1)))
        self.assertTrue(os.path.lexists(os.path.join(root_path, self.location_file_1)))
        self.assertTrue(os.path.lexists(os.path.join(root_path, self.uncertainty_coefficient_file_1)))
        self.assertTrue(os.path.lexists(os.path.join(root_path, self.uncertainty_file_1)))
        location_path = os.path.join(root_path, '02', self.location_name, 'location', self.location_name + '.json')
        self.assertTrue(os.path.lexists(location_path))
        calibration_path = os.path.join(root_path, '02', self.location_name, 'calibration')
        self.assertTrue(os.path.exists(calibration_path))
        # files created for data gaps
        # first missing day
        empty_location_path = os.path.join(root_path, '01', self.location_name, 'location')
        self.assertTrue(os.path.exists(empty_location_path))
        empty_data_path = os.path.join(root_path, '01', self.location_name, 'data')
        self.assertTrue(os.path.exists(empty_data_path))
        empty_calibration_path = os.path.join(root_path, '01', self.location_name, 'calibration')
        self.assertTrue(os.path.exists(empty_calibration_path))
        empty_uncertainty_data_path = os.path.join(root_path, '01', self.location_name, 'uncertainty_data')
        self.assertTrue(os.path.exists(empty_uncertainty_data_path))
        empty_uncertainty_coefficient_path = os.path.join(root_path, '01', self.location_name, 'uncertainty_coef')
        self.assertTrue(os.path.exists(empty_uncertainty_coefficient_path))
        empty_flags_path = os.path.join(root_path, '01', self.location_name, 'flags')
        self.assertTrue(os.path.exists(empty_flags_path))
        # second missing day
        empty_location_path = os.path.join(root_path, '03', self.location_name, 'location')
        self.assertTrue(os.path.exists(empty_location_path))
        empty_data_path = os.path.join(root_path, '03', self.location_name, 'data')
        self.assertTrue(os.path.exists(empty_data_path))
        empty_calibration_path = os.path.join(root_path, '03', self.location_name, 'calibration')
        self.assertTrue(os.path.exists(empty_calibration_path))
        empty_uncertainty_data_path = os.path.join(root_path, '03', self.location_name, 'uncertainty_data')
        self.assertTrue(os.path.exists(empty_uncertainty_data_path))
        empty_uncertainty_coefficient_path = os.path.join(root_path, '03', self.location_name, 'uncertainty_coef')
        self.assertTrue(os.path.exists(empty_uncertainty_coefficient_path))
        empty_flags_path = os.path.join(root_path, '03', self.location_name, 'flags')
        self.assertTrue(os.path.exists(empty_flags_path))
