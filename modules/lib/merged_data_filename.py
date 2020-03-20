

class MergedDataFilename(object):
    """Parse file names of the form prt_CFGLOC112154_2018-01-03.extension"""

    def __init__(self, filename):
        self.filename = filename.split('.')[0]

    def date(self):
        return self.filename.split('_')[2]

    def location(self):
        return self.filename.split('_')[1]

    @staticmethod
    def build(source_type, year, month, day, location):
        filename_format = '{}_{}_{}-{}-{}.extension'
        return filename_format.format(source_type, year, month, day, location)
