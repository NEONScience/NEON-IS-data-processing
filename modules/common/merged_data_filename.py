#!/usr/bin/env python3


class MergedDataFilename(object):
    """Class to parse file names in format [source type]_[location]_[YYYY]-[MM]-[DD].[extension]"""

    def __init__(self, filename: str):
        self.filename = filename.split('.')[0]

    def date(self) -> str:
        return self.filename.split('_')[2]

    def location(self) -> str:
        return self.filename.split('_')[1]
