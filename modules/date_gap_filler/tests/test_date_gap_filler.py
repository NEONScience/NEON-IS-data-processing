#!/usr/bin/env python3
import os
from pathlib import Path
from datetime import date

from pyfakefs.fake_filesystem_unittest import TestCase

import date_gap_filler.date_gap_filler_main as date_gap_filler_main
from date_gap_filler.date_gap_filler_config import DateGapFillerConfig
from date_gap_filler.data_file_path_config import DataFilePathConfig
from date_gap_filler.location_file_path_config import LocationFilePathConfig
from date_gap_filler.date_gap_filler import DateGapFiller


class DateGapFillerTest(TestCase):

    def setUp(self):
        self.source = 'prt'
        self.year = '2020'
        self.month = '01'
        self.location = 'CFGLOC123'
        self.location_filename = f'{self.location}.json'
        self.output_directories = f'{DateGapFillerConfig.data_dir},' \
                                  f'{DateGapFillerConfig.location_dir},' \
                                  f'{DateGapFillerConfig.calibration_dir},' \
                                  f'{DateGapFillerConfig.uncertainty_data_dir},' \
                                  f'{DateGapFillerConfig.uncertainty_coefficient_dir},' \
                                  f'{DateGapFillerConfig.flag_dir}'
        # path indices
        self.data_source_type_index = 3
        self.data_year_index = 4
        self.data_month_index = 5
        self.data_day_index = 6
        self.data_location_index = 7
        self.data_type_index = 8
        self.location_source_type_index = 3
        self.location_year_index = 4
        self.location_month_index = 5
        self.location_index = 6
        self.empty_file_type_index = 4
        # dirs and files
        self.setUpPyfakefs()
        self.out_path = Path('/out')
        self.fs.create_dir(self.out_path)
        self.create_data_repo()
        self.create_location_repo()
        self.create_empty_file_repo()

    def create_data_repo(self):
        self.data_path = Path('/data/repo', self.source, self.year, self.month)
        day = '02'
        data_file, flag_file, location_file, uncertainty_coefficient_file, uncertainty_data_file = \
            self.get_file_names(day)
        self.data_file = Path(day, self.location, DateGapFillerConfig.data_dir, data_file)
        self.flags_file = Path(day, self.location, DateGapFillerConfig.flag_dir, flag_file)
        self.location_file = Path(day, self.location, DateGapFillerConfig.location_dir, location_file)
        self.uncertainty_coefficient_file = Path(day, self.location, DateGapFillerConfig.uncertainty_coefficient_dir,
                                                 uncertainty_coefficient_file)
        self.uncertainty_data_file = Path(day, self.location, DateGapFillerConfig.uncertainty_data_dir,
                                          uncertainty_data_file)
        self.fs.create_file(Path(self.data_path, self.data_file))
        self.fs.create_file(Path(self.data_path, self.flags_file))
        self.fs.create_file(Path(self.data_path, self.location_file))
        self.fs.create_file(Path(self.data_path, self.uncertainty_coefficient_file))
        self.fs.create_file(Path(self.data_path, self.uncertainty_data_file))

    def create_location_repo(self):
        self.location_path = Path('/location/repo', self.source, self.year, self.month)
        self.fs.create_file(Path(self.location_path, self.location, self.location_filename))

    def create_empty_file_repo(self):
        self.empty_path = Path('/empty/repo', self.source)
        self.empty_data_file = Path(self.empty_path, DateGapFillerConfig.data_dir,
                                    f'{self.source}_location_year-month-day.ext')
        self.empty_uncertainty_data_file = Path(self.empty_path, DateGapFillerConfig.uncertainty_data_dir,
                                                f'{self.source}_location_year-month-day_uncertaintyData.ext')
        self.empty_flag_file = Path(self.empty_path, DateGapFillerConfig.flag_dir,
                                    f'{self.source}_location_year-month-day_flagsCal.ext')
        self.fs.create_file(self.empty_data_file)
        self.fs.create_file(self.empty_uncertainty_data_file)
        self.fs.create_file(self.empty_flag_file)

    def test_fill_gaps(self):
        data_file_path_config = DataFilePathConfig(source_type_index=self.data_source_type_index,
                                                   year_index=self.data_year_index,
                                                   month_index=self.data_month_index,
                                                   day_index=self.data_day_index,
                                                   location_index=self.data_location_index,
                                                   data_type_index=self.data_type_index)
        location_file_path_config = LocationFilePathConfig(source_type_index=self.location_source_type_index,
                                                           year_index=self.location_year_index,
                                                           month_index=self.location_month_index,
                                                           location_index=self.location_index)
        config = DateGapFillerConfig(data_path=self.data_path,
                                     location_path=self.location_path,
                                     empty_file_path=self.empty_path,
                                     out_path=self.out_path,
                                     start_date=date(2019, 12, 31),
                                     end_date=date(2020, 1, 4),
                                     output_directories=self.output_directories.split(','),
                                     empty_file_type_index=self.empty_file_type_index)
        date_gap_filler = DateGapFiller(config=config,
                                        data_file_path_config=data_file_path_config,
                                        location_file_path_config=location_file_path_config)
        date_gap_filler.fill_gaps()
        self.check_output()

    def test_main(self):
        os.environ['DATA_PATH'] = str(self.data_path)
        os.environ['LOCATION_PATH'] = str(self.location_path)
        os.environ['EMPTY_FILES_PATH'] = str(self.empty_path)
        os.environ['OUTPUT_DIRECTORIES'] = self.output_directories
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['START_DATE'] = '2019-12-31'
        os.environ['END_DATE'] = '2020-01-04'
        os.environ['DATA_SOURCE_TYPE_INDEX'] = str(self.data_source_type_index)
        os.environ['DATA_YEAR_INDEX'] = str(self.data_year_index)
        os.environ['DATA_MONTH_INDEX'] = str(self.data_month_index)
        os.environ['DATA_DAY_INDEX'] = str(self.data_day_index)
        os.environ['DATA_LOCATION_INDEX'] = str(self.data_location_index)
        os.environ['DATA_TYPE_INDEX'] = str(self.data_type_index)
        os.environ['LOCATION_SOURCE_TYPE_INDEX'] = str(self.location_source_type_index)
        os.environ['LOCATION_YEAR_INDEX'] = str(self.location_year_index)
        os.environ['LOCATION_MONTH_INDEX'] = str(self.location_month_index)
        os.environ['LOCATION_INDEX'] = str(self.location_index)
        os.environ['EMPTY_FILE_TYPE_INDEX'] = str(self.empty_file_type_index)
        date_gap_filler_main.main()
        self.check_output()

    def check_output(self):
        root_path = Path(self.out_path, self.source, self.year, self.month)
        self.check_inactive_day(root_path, '01')
        self.check_active_day(root_path, '02')
        self.check_inactive_day(root_path, '03')

    def check_active_day(self, root_path: Path, day: str):
        self.assertTrue(Path(root_path, self.data_file).exists())
        self.assertTrue(Path(root_path, self.flags_file).exists())
        self.assertTrue(Path(root_path, self.location_file).exists())
        self.assertTrue(Path(root_path, self.uncertainty_coefficient_file).exists())
        self.assertTrue(Path(root_path, self.uncertainty_data_file).exists())
        location_path = Path(root_path, day, self.location, DateGapFillerConfig.location_dir, self.location_filename)
        self.assertTrue(location_path.exists())

    def check_inactive_day(self, root_path: Path, day: str):
        data_file, flag_file, location_file, uncertainty_coefficient_file, uncertainty_data_file = \
            self.get_file_names(day)
        metadata_path = Path(root_path, day, self.location)
        calibration_path = Path(metadata_path, DateGapFillerConfig.calibration_dir)
        data_path = Path(metadata_path, DateGapFillerConfig.data_dir, f'{data_file}.empty')
        flag_path = Path(metadata_path, DateGapFillerConfig.flag_dir, f'{flag_file}.empty')
        location_path = Path(metadata_path, DateGapFillerConfig.location_dir)
        uncertainty_coefficient_path = Path(metadata_path, DateGapFillerConfig.uncertainty_coefficient_dir)
        uncertainty_data_path = Path(metadata_path, DateGapFillerConfig.uncertainty_data_dir,
                                     f'{uncertainty_data_file}.empty')
        self.assertTrue(calibration_path.exists())
        self.assertTrue(data_path.exists())
        self.assertTrue(flag_path.exists())
        self.assertTrue(location_path.exists())
        self.assertTrue(uncertainty_coefficient_path.exists())
        self.assertTrue(uncertainty_data_path.exists())

    def get_file_names(self, day):
        data_name = f'{self.source}_{self.location}_{self.year}-{self.month}-{day}.ext'
        flag_name = f'{self.source}_{self.location}_{self.year}-{self.month}-{day}_flagsCal.ext'
        location_name = f'{self.source}_{self.location}_locations.json'
        uncertainty_coefficient_name = f'{self.source}_{self.location}_{self.year}-{self.month}-{day}' \
                                       f'_uncertaintyCoef.json'
        uncertainty_data_name = f'{self.source}_{self.location}_{self.year}-{self.month}-{day}' \
                                f'_uncertaintyData.ext'
        return data_name, flag_name, location_name, uncertainty_coefficient_name, uncertainty_data_name
