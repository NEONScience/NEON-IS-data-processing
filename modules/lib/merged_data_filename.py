#!/usr/bin/env python3


class MergedDataFilename(object):
    """Parse file names of the form prt_CFGLOC112154_2018-01-03.extension"""

    def __init__(self, filename):
        """
        Constructor.

        :param filename: The filename.
        :type filename: str
        """
        self.filename = filename.split('.')[0]

    def date(self):
        return self.filename.split('_')[2]

    def location(self):
        return self.filename.split('_')[1]

    @staticmethod
    def build(source_type, year, month, day, location):
        """
        Build a merged filename.

        :param source_type: The source type.
        :type source_type: str
        :param year: The year.
        :type year: str
        :param month: The month.
        :type month: str
        :param day: The day.
        :type day: str
        :param location: The location.
        :type location: str
        :return: The filename str.
        """
        filename_format = '{}_{}_{}-{}-{}.extension'
        return filename_format.format(source_type, year, month, day, location)
