#!/usr/bin/env python3


class LocationFilePathConfig(object):

    def __init__(self, *,
                 source_type_index: int,
                 year_index: int,
                 month_index: int,
                 location_index: int):
        self.source_type_index = source_type_index
        self.year_index = year_index
        self.month_index = month_index
        self.location_index = location_index
