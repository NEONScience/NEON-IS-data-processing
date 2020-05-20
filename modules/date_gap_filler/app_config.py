#!/usr/bin/env python3
import environs

import lib.log_config as log_config

from date_gap_filler.empty_file_handler import get_paths


class AppConfig(object):

    def __init__(self):
        env = environs.Env()  # extract from environment
        self.data_path = env.str('DATA_PATH', None)
        self.location_path = env.str('LOCATION_PATH', None)
        self.empty_files_path = env.str('EMPTY_FILES_PATH')
        self.output_directories = env.list('OUTPUT_DIRECTORIES')
        self.start_date = env.date('START_DATE', None)
        self.end_date = env.date('END_DATE', None)
        self.out_path = env.str('OUT_PATH')
        self.log_level = env.log_level('LOG_LEVEL', 'INFO')
        self.data_source_type_index = env.int('DATA_SOURCE_TYPE_INDEX')
        self.data_year_index = env.int('DATA_YEAR_INDEX')
        self.data_month_index = env.int('DATA_MONTH_INDEX')
        self.data_day_index = env.int('DATA_DAY_INDEX')
        self.data_location_index = env.int('DATA_LOCATION_INDEX')
        self.data_type_index = env.int('DATA_TYPE_INDEX')
        self.data_filename_index = env.int('DATA_FILENAME_INDEX')
        self.location_source_type_index = env.int('LOCATION_SOURCE_TYPE_INDEX')
        self.location_year_index = env.int('LOCATION_YEAR_INDEX')
        self.location_month_index = env.int('LOCATION_MONTH_INDEX')
        self.location_day_index = env.int('LOCATION_DAY_INDEX')
        self.location_index = env.int('LOCATION_INDEX')
        self.location_filename_index = env.int('LOCATION_FILENAME_INDEX')
        self.empty_file_type_index = env.int('EMPTY_FILE_TYPE_INDEX')

        # configure log
        log_config.configure(self.log_level)

        # empty file paths
        empty_files_paths = get_paths(self.empty_files_path, self.empty_file_type_index)
        self.empty_data_path = empty_files_paths.get('data_path')
        self.empty_flags_path = empty_files_paths.get('flags_path')
        self.empty_uncertainty_data_path = empty_files_paths.get('uncertainty_path')
