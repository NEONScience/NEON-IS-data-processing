#!/usr/bin/env python3
import environs


class AppConfig(object):

    def __init__(self):
        env = environs.Env()
        self.data_path = env.path('DATA_PATH', None)
        self.location_path = env.path('LOCATION_PATH', None)
        self.empty_files_path = env.path('EMPTY_FILES_PATH')
        self.output_directories = env.list('OUTPUT_DIRECTORIES')
        self.start_date = env.date('START_DATE', None)
        self.end_date = env.date('END_DATE', None)
        self.out_path = env.path('OUT_PATH')
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
