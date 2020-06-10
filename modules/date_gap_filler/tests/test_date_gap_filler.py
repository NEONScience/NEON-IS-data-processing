#!/usr/bin/env python3
import os
from pathlib import Path
from datetime import date

from pyfakefs.fake_filesystem_unittest import TestCase

import date_gap_filler.date_gap_filler as date_gap_filler
from date_gap_filler.date_between import date_is_between


class DateGapFillerTest(TestCase):

    def setUp(self):
        # location
        self.location_name = 'SENSOR000000'
        # initialize fake file system
        self.setUpPyfakefs()
        #  create output directory
        self.out_path = Path('/outputs/repo')
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
        self.data_path = Path('/files/repo_name/exo2/2020/01')
        self.data_file_1 = Path('02', self.location_name, 'data', f'exo2_{self.location_name}_2020-01-02.ext')
        self.flags_file_1 = Path('02', self.location_name, 'flags',
                                 f'exo2_{self.location_name}_2020-01-02_flagsCal.ext')
        self.location_file_1 = Path('02', self.location_name, 'location',
                                    'exo2_' + self.location_name + '_locations.json')
        self.uncertainty_coefficient_file_1 = Path('02', self.location_name, 'uncertainty_coef',
                                                   f'exo2_{self.location_name}_2020-01-02_uncertaintyCoef.json')
        self.uncertainty_file_1 = Path('02', self.location_name, 'uncertainty_data',
                                       f'exo2_{self.location_name}_2020-01-02_uncertaintyData.ext')
        self.fs.create_file(self.data_path.joinpath(self.data_file_1))
        self.fs.create_file(self.data_path.joinpath(self.flags_file_1))
        self.fs.create_file(self.data_path.joinpath(self.location_file_1))
        self.fs.create_file(self.data_path.joinpath(self.uncertainty_coefficient_file_1))
        self.fs.create_file(self.data_path.joinpath(self.uncertainty_file_1))

    def create_location_repo(self):
        self.location_path = Path('/locations/repo_name/exo2/2020/01')
        self.location_file_1 = self.location_path.joinpath('01', self.location_name, f'{self.location_name}.json')
        self.location_file_2 = self.location_path.joinpath('02', self.location_name, f'{self.location_name}.json')
        self.location_file_3 = self.location_path.joinpath('03', self.location_name, f'{self.location_name}.json')
        self.fs.create_file(self.location_file_1)
        self.fs.create_file(self.location_file_2)
        self.fs.create_file(self.location_file_3)

    def create_empty_files_repo(self):
        self.empty_files_path = Path('/empty/empty_files/exo2')
        # data
        self.empty_data_path = self.empty_files_path.joinpath('data')
        self.empty_data_file = self.empty_data_path.joinpath('exo2_location_year-month-day.ext')
        self.fs.create_file(self.empty_data_file)
        # uncertainty data
        self.empty_uncertainty_data_path = self.empty_files_path.joinpath('uncertainty_data')
        self.empty_uncertainty_data_file = \
            self.empty_uncertainty_data_path.joinpath('exo2_location_year-month-day_uncertaintyData.ext')
        self.fs.create_file(self.empty_uncertainty_data_file)
        # flags
        self.empty_flags_path = self.empty_files_path.joinpath('flags')
        self.empty_flags_file = self.empty_flags_path.joinpath('exo2_location_year-month-day_flagsCal.ext')
        self.fs.create_file(self.empty_flags_file)

    def test_date_between(self):
        start_date = date(2020, 1, 1)
        end_date = date(2020, 3, 3)
        result = date_is_between(year=2020, month=2, day=1, start_date=start_date, end_date=end_date)
        self.assertTrue(result)
        start_date = date(2020, 1, 1)
        end_date = date(2020, 3, 31)
        result = date_is_between(year=2020, month=2, day=1, start_date=start_date, end_date=end_date)
        self.assertTrue(result)

    def test_main(self):
        os.environ['DATA_PATH'] = str(self.data_path)
        os.environ['LOCATION_PATH'] = str(self.location_path)
        os.environ['EMPTY_FILES_PATH'] = str(self.empty_files_path)
        os.environ['OUTPUT_DIRECTORIES'] = self.output_directories
        os.environ['OUT_PATH'] = str(self.out_path)
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
        date_gap_filler.main()
        self.check_output()

    def check_output(self):
        root_path = Path(self.out_path, 'exo2/2020/01')

        # non-missing day
        self.assertTrue(Path(root_path, self.data_file_1).exists())
        self.assertTrue(Path(root_path, self.flags_file_1).exists())
        self.assertTrue(Path(root_path, self.location_file_1).exists())
        self.assertTrue(Path(root_path, self.uncertainty_coefficient_file_1).exists())
        self.assertTrue(Path(root_path, self.uncertainty_file_1).exists())
        location_path = Path(root_path, '02', self.location_name, 'location', f'{self.location_name}.json')
        self.assertTrue(location_path.exists())

        # check output for filled-in data gaps

        # first missing day
        empty_location_path = Path(root_path, '01', self.location_name, 'location')
        empty_data_path = Path(root_path, '01', self.location_name, 'data',
                               f'exo2_{self.location_name}_2020-01-01.ext.empty')
        empty_calibration_path = Path(root_path, '01', self.location_name, 'calibration')
        empty_uncertainty_data_path = Path(root_path, '01', self.location_name, 'uncertainty_data',
                                           f'exo2_{self.location_name}_2020-01-01_uncertaintyData.ext.empty')
        empty_uncertainty_coefficient_path = Path(root_path, '01', self.location_name, 'uncertainty_coef')
        empty_flags_path = Path(root_path, '01', self.location_name, 'flags',
                                f'exo2_{self.location_name}_2020-01-01_flagsCal.ext.empty')
        self.assertTrue(empty_location_path.exists())
        self.assertTrue(empty_data_path.exists())
        self.assertTrue(empty_calibration_path.exists())
        self.assertTrue(empty_uncertainty_data_path.exists())
        self.assertTrue(empty_uncertainty_coefficient_path.exists())
        self.assertTrue(empty_flags_path.exists())

        # second missing day
        empty_location_path = Path(root_path, '03', self.location_name, 'location')
        empty_data_path = Path(root_path, '03', self.location_name, 'data',
                               f'exo2_{self.location_name}_2020-01-03.ext.empty')
        empty_calibration_path = Path(root_path, '03', self.location_name, 'calibration')
        empty_uncertainty_data_path = Path(root_path, '03', self.location_name, 'uncertainty_data',
                                           f'exo2_{self.location_name}_2020-01-03_uncertaintyData.ext.empty')
        empty_uncertainty_coefficient_path = Path(root_path, '03', self.location_name, 'uncertainty_coef')
        empty_flags_path = Path(root_path, '03', self.location_name, 'flags',
                                f'exo2_{self.location_name}_2020-01-03_flagsCal.ext.empty')
        self.assertTrue(empty_location_path.exists())
        self.assertTrue(empty_data_path.exists())
        self.assertTrue(empty_calibration_path.exists())
        self.assertTrue(empty_uncertainty_data_path.exists())
        self.assertTrue(empty_uncertainty_coefficient_path.exists())
        self.assertTrue(empty_flags_path.exists())
