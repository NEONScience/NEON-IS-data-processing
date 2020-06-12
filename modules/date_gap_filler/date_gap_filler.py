#!/usr/bin/env python3

from date_gap_filler.date_gap_filler_config import DateGapFillerConfig
from date_gap_filler.data_file_path import DataFilePath
from date_gap_filler.location_file_path import LocationFilePath
from date_gap_filler.data_file_linker import DataFileLinker
from date_gap_filler.location_file_linker import LocationFileLinker


class DateGapFiller(object):

    def __init__(self, *,
                 config: DateGapFillerConfig,
                 data_file_path_config: DataFilePath,
                 location_file_path_config: LocationFilePath):
        self.config = config
        self.data_file_path_config = data_file_path_config
        self.location_file_path_config = location_file_path_config

    def fill_gaps(self):
        if self.config.data_path is not None:
            data_file_linker = DataFileLinker(self.config, self.data_file_path_config)
            data_file_linker.link_files()
        if self.config.location_path is not None:
            location_file_linker = LocationFileLinker(self.config, self.location_file_path_config)
            location_file_linker.link_files()
