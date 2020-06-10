#!/usr/bin/env python3
import structlog
from pathlib import Path

from date_gap_filler.date_gap_filler_config import DateGapFillerConfig
from date_gap_filler.data_file_path_config import DataFilePathConfig

log = structlog.get_logger()


class DataFileLinker(object):

    def __init__(self, config: DateGapFillerConfig, data_file_path_config: DataFilePathConfig):
        self.data_path = config.data_path
        self.out_path = config.out_path
        self.source_type_index = data_file_path_config.source_type_index
        self.year_index = data_file_path_config.year_index
        self.month_index = data_file_path_config.month_index
        self.day_index = data_file_path_config.day_index
        self.location_index = data_file_path_config.location_index
        self.data_type_index = data_file_path_config.data_type_index
        self.filename_index = data_file_path_config.filename_index

    def link_files(self):
        """Link all files between the start and end dates."""
        for path in self.data_path.rglob('*'):
            if path.is_file():
                parts = path.parts
                source_type = parts[self.source_type_index]
                year = parts[self.year_index]
                month = parts[self.month_index]
                day = parts[self.day_index]
                location = parts[self.location_index]
                data_type = parts[self.data_type_index]
                filename = parts[self.filename_index]
                link_path = Path(self.out_path, source_type, year, month, day, location, data_type, filename)
                link_path.parent.mkdir(parents=True, exist_ok=True)
                link_path.symlink_to(path)
