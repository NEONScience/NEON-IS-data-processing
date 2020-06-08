#!/usr/bin/env python3


class DataFilename(object):
    """Parse data file names in format [source type]_[source ID]_YYYY-MM-DD.[extension]"""

    def __init__(self, filename: str):
        self.filename = filename.split('.')[0]

    def source_type(self):
        return self.filename.split('_')[0]

    def source_id(self):
        return self.filename.split('_')[1]

    def date(self):
        return self.filename.split('_')[2]
