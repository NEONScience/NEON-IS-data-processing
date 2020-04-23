#!/usr/bin/env python3


class DataFilename(object):
    """Parse data file names of the form prt_769_2018-01-03.<extension>"""

    def __init__(self, filename):
        """
        Constructor.

        :param filename: The filename.
        :type filename: str
        """
        self.filename = filename.split('.')[0]

    def source_type(self):
        return self.filename.split('_')[0]

    def source_id(self):
        return self.filename.split('_')[1]

    def date(self):
        return self.filename.split('_')[2]
