#!/usr/bin/env python3
from date_gap_filler.date_gap_filler_config import DateGapFillerConfig
from date_gap_filler.data_file_linker import DataFileLinker
from date_gap_filler.location_file_linker import LocationFileLinker


class DateGapFiller:

    def __init__(self, config: DateGapFillerConfig):
        self.config = config

    def fill_gaps(self):
        if self.config.data_path is not None:
            data_file_linker = DataFileLinker(self.config)
            data_file_linker.link_files()
        if self.config.location_path is not None:
            location_file_linker = LocationFileLinker(self.config)
            location_file_linker.link_files()
