#!/usr/bin/env python3
import structlog
from pathlib import Path

from date_gap_filler.date_between import date_between
from date_gap_filler.app_config import AppConfig

log = structlog.get_logger()


def link_data_files(config: AppConfig):
    """
    Link all files between the start and end dates.

    :param config: The path to the data file directory.
    :return: The data files.
    """
    # input/output
    data_path = config.data_path
    out_path = config.out_path
    # path indices
    source_type_index = config.data_source_type_index
    year_index = config.data_year_index
    month_index = config.data_month_index
    day_index = config.data_day_index
    location_index = config.data_location_index
    data_type_index = config.data_type_index
    filename_index = config.data_filename_index
    # dates
    start_date = config.start_date
    end_date = config.end_date

    for path in data_path.rglob('*'):
        if path.is_file():
            parts = path.parts
            source_type = parts[source_type_index]
            year = parts[year_index]
            month = parts[month_index]
            day = parts[day_index]
            location = parts[location_index]
            data_type = parts[data_type_index]
            filename = parts[filename_index]
            if not date_between(int(year), int(month), int(day), start_date, end_date):
                continue
            link_path = Path(out_path, source_type, year, month, day, location, data_type, filename)
            link_path.parent.mkdir(parents=True, exist_ok=True)
            link_path.symlink_to(path)
