#!/usr/bin/env python3
import os

from pyfakefs.fake_filesystem_unittest import TestCase

import date_gap_filler.app as app


class AppTest(TestCase):

    def setUp(self):
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

        # path indices
        self.data_source_type_index = '3'
        self.data_year_index = '4'
        self.data_month_index = '5'
        self.data_day_index = '6'
        self.data_location_index = '7'
        self.data_type_index = '8'
        self.data_filename_index = '9'
        self.location_source_type_index = '3'
        self.location_year_index = '4'
        self.location_month_index = '5'
        self.location_day_index = '6'
        self.location_index = '7'
        self.location_filename_index = '8'
        self.empty_file_type_index = '4'

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
        self.empty_files_path = os.path.join('/', 'empty', 'empty_files', 'exo2')
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
        os.environ['DATA_SOURCE_TYPE_INDEX'] = self.data_source_type_index
        os.environ['DATA_YEAR_INDEX'] = self.data_year_index
        os.environ['DATA_MONTH_INDEX'] = self.data_month_index
        os.environ['DATA_DAY_INDEX'] = self.data_day_index
        os.environ['DATA_LOCATION_INDEX'] = self.data_location_index
        os.environ['DATA_TYPE_INDEX'] = self.data_type_index
        os.environ['DATA_FILENAME_INDEX'] = self.data_filename_index
        os.environ['LOCATION_SOURCE_TYPE_INDEX'] = self.location_source_type_index
        os.environ['LOCATION_YEAR_INDEX'] = self.location_year_index
        os.environ['LOCATION_MONTH_INDEX'] = self.location_month_index
        os.environ['LOCATION_DAY_INDEX'] = self.location_day_index
        os.environ['LOCATION_INDEX'] = self.location_index
        os.environ['LOCATION_FILENAME_INDEX'] = self.location_filename_index
        os.environ['EMPTY_FILE_TYPE_INDEX'] = self.empty_file_type_index
        app.main()
        self.check_output()

    def check_output(self):
        root_path = os.path.join(self.out_path, 'exo2', '2020', '01')

        # non-missing day
        self.assertTrue(os.path.lexists(os.path.join(root_path, self.data_file_1)))
        self.assertTrue(os.path.lexists(os.path.join(root_path, self.flags_file_1)))
        self.assertTrue(os.path.lexists(os.path.join(root_path, self.location_file_1)))
        self.assertTrue(os.path.lexists(os.path.join(root_path, self.uncertainty_coefficient_file_1)))
        self.assertTrue(os.path.lexists(os.path.join(root_path, self.uncertainty_file_1)))
        location_path = os.path.join(root_path, '02', self.location_name, 'location', self.location_name + '.json')
        self.assertTrue(os.path.lexists(location_path))

        # check output for filled-in data gaps

        # first missing day
        empty_location_path = os.path.join(root_path, '01', self.location_name, 'location')
        self.assertTrue(os.path.exists(empty_location_path))
        empty_data_path = os.path.join(root_path, '01', self.location_name, 'data',
                                       'exo2_' + self.location_name + '_2020-01-01.ext.empty')
        self.assertTrue(os.path.exists(empty_data_path))
        empty_calibration_path = os.path.join(root_path, '01', self.location_name, 'calibration')
        self.assertTrue(os.path.exists(empty_calibration_path))
        empty_uncertainty_data_path = os.path.join(root_path, '01', self.location_name, 'uncertainty_data',
                                                   'exo2_' + self.location_name
                                                   + '_2020-01-01_uncertaintyData.ext.empty')
        self.assertTrue(os.path.exists(empty_uncertainty_data_path))
        empty_uncertainty_coefficient_path = os.path.join(root_path, '01', self.location_name, 'uncertainty_coef')
        self.assertTrue(os.path.exists(empty_uncertainty_coefficient_path))
        empty_flags_path = os.path.join(root_path, '01', self.location_name, 'flags', 'exo2_' + self.location_name
                                        + '_2020-01-01_flagsCal.ext.empty')
        self.assertTrue(os.path.exists(empty_flags_path))

        # second missing day
        empty_location_path = os.path.join(root_path, '03', self.location_name, 'location')
        self.assertTrue(os.path.exists(empty_location_path))
        empty_data_path = os.path.join(root_path, '03', self.location_name, 'data',
                                       'exo2_' + self.location_name + '_2020-01-03.ext.empty')
        self.assertTrue(os.path.exists(empty_data_path))
        empty_calibration_path = os.path.join(root_path, '03', self.location_name, 'calibration')
        self.assertTrue(os.path.exists(empty_calibration_path))
        empty_uncertainty_data_path = os.path.join(root_path, '03', self.location_name, 'uncertainty_data',
                                                   'exo2_' + self.location_name
                                                   + '_2020-01-03_uncertaintyData.ext.empty')
        self.assertTrue(os.path.exists(empty_uncertainty_data_path))
        empty_uncertainty_coefficient_path = os.path.join(root_path, '03', self.location_name, 'uncertainty_coef')
        self.assertTrue(os.path.exists(empty_uncertainty_coefficient_path))
        empty_flags_path = os.path.join(root_path, '03', self.location_name, 'flags',
                                        'exo2_' + self.location_name + '_2020-01-03_flagsCal.ext.empty')
        self.assertTrue(os.path.exists(empty_flags_path))
