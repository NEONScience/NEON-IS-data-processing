#!/usr/bin/env python3
from pathlib import Path
from datetime import datetime, timedelta
from typing import Iterator

import structlog

import common.date_formatter as date_formatter
import common.location_file_parser as file_parser


log = structlog.get_logger()


def link_location_files(*, location_path: Path, out_path: Path, schema_index: int) -> None:
    """
    Link a location file for each active date into path '/schema/yyyy/mm/dd/location/file'.

    :param location_path: The location file path.
    :param out_path: The output directory root path.
    :param schema_index: The file path index of the schema name.
    """
    for path in location_path.rglob('*'):
        if path.is_file():
            parts = path.parts
            schema_name: str = parts[schema_index]
            location_name, active_periods, context = file_parser.parse_location_file(path)
            for period in active_periods:
                period_start_date: str = period.get('start_date')
                period_end_date: str = period.get('end_date')
                log.debug(f'start_date: {period_start_date} end_date: {period_end_date}')
                if period_start_date is not None:
                    start_date = date_formatter.to_datetime(period_start_date)
                else:
                    start_date = None
                if period_end_date is not None:
                    end_date = date_formatter.to_datetime(period_end_date)
                else:
                    # do not proceed past current date
                    end_date = datetime.now()
                if start_date is not None:
                    for date in dates_between(start_date, end_date):
                        year = str(date.year)
                        month = str(date.month).zfill(2)
                        link_path = Path(out_path, schema_name, year, month, location_name, path.name)
                        link_path.parent.mkdir(parents=True, exist_ok=True)
                        if not link_path.exists():
                            link_path.symlink_to(path)


def dates_between(start_date: datetime, end_date: datetime) -> Iterator[datetime]:
    """
    Yield all dates between the start and end dates.

    :param start_date: Date to begin generating dates.
    :param end_date: Date to stop generating dates.
    :return: All dates between the start and end dates.
    """
    delta = end_date - start_date
    for i in range(delta.days + 1):
        yield start_date + timedelta(days=i)
