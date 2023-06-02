from datetime import datetime

from pub_files.input_files.file_metadata import FileMetadata


class DateFormats:
    """Class to hold the different data formatting options for the EML file."""

    def __init__(self, metadata: FileMetadata):
        start_time = metadata.data_files.min_time
        end_time = metadata.data_files.max_time
        self.start_date = self.format_date(start_time)
        self.end_date = self.format_date(end_time)
        self.start_date_dashed = self.format_dashed_date(start_time)
        self.end_date_dashed = self.format_dashed_date(end_time)

    @staticmethod
    def format_date(date: datetime) -> str:
        return date.strftime('%Y%m%d')

    @staticmethod
    def format_dashed_date(date: datetime) -> str:
        return datetime.strftime(date, '%Y-%m-%d')
