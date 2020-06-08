#!/usr/bin/env python3


class MergedDataFilename(object):
    """Class to parse file names in format [source type]_[location]_[YYYY]-[MM]-[DD].[extension]"""

    def __init__(self, filename: str):
        self.filename = filename.split('.')[0]

    def date(self):
        return self.filename.split('_')[2]

    def location(self):
        return self.filename.split('_')[1]

    @staticmethod
    def build(source_type: str, year: str, month: str, day: str, location: str):
        """
        Build a merged filename for testing.

        :param source_type: The source type.
        :param year: The year.
        :param month: The month.
        :param day: The day.
        :param location: The location.
        :return: The filename.
        """
        filename_format = '{}_{}_{}-{}-{}.extension'
        return filename_format.format(source_type, year, month, day, location)
