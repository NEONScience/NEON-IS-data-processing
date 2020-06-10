#!/usr/bin/env python3
from pathlib import Path

import structlog

from date_gap_filler.date_between import date_is_between
from date_gap_filler.empty_files import EmptyFiles
from date_gap_filler.empty_file_linker import EmptyFileLinker
from date_gap_filler.app_config import AppConfig

log = structlog.get_logger()


class LocationFileLinker(object):

    def __init__(self, config: AppConfig):
        self.out_path = config.out_path
        self.output_dirs = config.output_directories
        self.location_path = config.location_path
        self.source_type_index = config.location_source_type_index
        self.year_index = config.location_year_index
        self.month_index = config.location_month_index
        self.day_index = config.location_day_index
        self.location_index = config.location_index
        self.filename_index = config.location_filename_index
        self.start_date = config.start_date
        self.end_date = config.end_date
        self.calibration_dir = config.calibration_dir
        self.data_dir = config.data_dir
        self.flags_dir = config.flags_dir
        self.location_dir = config.location_dir
        self.uncertainty_coefficient_dir = config.uncertainty_coefficient_dir
        self.uncertainty_data_dir = config.uncertainty_data_dir
        self.empty_files = EmptyFiles(config)

    def link_files(self):
        """Process the location files and fill date gaps with empty files."""
        link_calibration = True if self.calibration_dir in self.output_dirs else False
        link_data = True if self.data_dir in self.output_dirs else False
        link_flags = True if self.flags_dir in self.output_dirs else False
        link_uncertainty_coefficient = True if self.uncertainty_coefficient_dir in self.output_dirs else False
        link_uncertainty_data = True if self.uncertainty_data_dir in self.output_dirs else False
        for path in self.location_path.rglob('*'):
            if path.is_file():
                log.debug(f'processing location file: {path}')
                parts = path.parts
                source_type = parts[self.source_type_index]
                year = parts[self.year_index]
                month = parts[self.month_index]
                day = parts[self.day_index]
                location = parts[self.location_index]
                filename = parts[self.filename_index]
                if not date_is_between(year=int(year), month=int(month), day=int(day),
                                       start_date=self.start_date, end_date=self.end_date):
                    continue
                root_link_path = Path(self.out_path, source_type, year, month, day, location)
                location_link = Path(root_link_path, self.location_dir, filename)
                location_link.parent.mkdir(parents=True, exist_ok=True)
                location_link.symlink_to(path)
                empty_file_linker = EmptyFileLinker(self.empty_files, location, year, month, day)
                if link_calibration:
                    Path(root_link_path, self.calibration_dir).mkdir(parents=True, exist_ok=True)
                if link_data:
                    empty_file_linker.link_data_file(Path(root_link_path, self.data_dir))
                if link_flags:
                    empty_file_linker.link_flags_file(Path(root_link_path, self.flags_dir))
                if link_uncertainty_coefficient:
                    Path(root_link_path, self.uncertainty_coefficient_dir).mkdir(parents=True, exist_ok=True)
                if link_uncertainty_data:
                    empty_file_linker.link_uncertainty_data_file(Path(root_link_path, self.uncertainty_data_dir))
