#!/usr/bin/env python3


class DataFilePathConfig(object):

    def __init__(self, *,
                 source_type_index: int,
                 year_index: int,
                 month_index: int,
                 day_index: int,
                 location_index: int,
                 data_type_index: int,
                 filename_index: int):
        self.source_type_index = source_type_index
        self.year_index = year_index
        self.month_index = month_index
        self.day_index = day_index
        self.location_index = location_index
        self.data_type_index = data_type_index
        self.filename_index = filename_index
