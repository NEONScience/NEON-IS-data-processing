"""
This module contains functions for converting a
list of log entries into a change log format with
affected dates and locations added into a list
for each log entry.
"""
from datetime import datetime
from typing import NamedTuple, List, Optional

from pub_files.database.log_entries import LogEntry


class DatesLocations(NamedTuple):
    """The locations and their affected time span."""
    date_range_start: datetime
    date_range_end: datetime
    location_affected: List[str]


class ChangeLog(NamedTuple):
    """The final change log format with a list of dates and affected locations."""
    data_product_id: str
    issue: str
    issue_date: datetime
    resolution: str
    resolution_date: Optional[datetime]
    dates_locations: List[DatesLocations]


def get_change_log(data_product_id: str, log_entries: List[LogEntry]) -> List[ChangeLog]:
    """
    Process log entries in order to form the list of dates and locations affected
    for each log entry. (TODO: The logs are currently stored in a flat table. The logic below
    would be unnecessary with an improved data model.)
    """
    change_logs = []

    if log_entries:
        log_values = {}
        dates_and_locations: List[DatesLocations] = []
        locations_affected = []
        issue_date = None
        issue = ''
        resolution_date = None
        resolution = ''
        date_range_start = None
        date_range_end = None
        is_first_loop = True
        is_first_date_location = True

        for log_entry in log_entries:
            # Create a new log if these conditions are met.
            if is_first_loop \
                    or issue_date != log_entry.issue_date \
                    or issue != log_entry.issue \
                    or resolution_date != log_entry.resolution_date \
                    or resolution != log_entry.resolution \
                    or (resolution_date is None and log_entry.resolution_date is None) \
                    or (resolution == '' and log_entry.resolution == ''):

                # Create a new log.
                if not is_first_loop:
                    dates_locations = DatesLocations(date_range_start, date_range_end, locations_affected)
                    dates_and_locations.append(dates_locations)
                    change_logs.append(build_change_log(data_product_id, log_values, dates_and_locations))

                # Pull the log entry values
                issue_date: datetime = log_entry.issue_date
                issue = log_entry.issue
                resolution_date: Optional[datetime] = log_entry.resolution_date
                resolution = log_entry.resolution

                log_values.clear()  # Reset the stored values for the new log entry.
                log_values.update(issue_date=issue_date)
                log_values.update(issue=issue)
                log_values.update(resolution_date=resolution_date)
                log_values.update(resolution=resolution)

                # Reset values for next loop.
                dates_and_locations = []
                locations_affected = []
                is_first_date_location = True

            # Get the dates and locations
            if is_first_date_location \
                    or date_range_start != log_entry.date_range_start \
                    or date_range_end != log_entry.date_range_end:

                if not is_first_date_location:
                    dates_locations = DatesLocations(date_range_start, date_range_end, locations_affected)
                    dates_and_locations.append(dates_locations)
                    locations_affected = []  # Reset locations affected

                date_range_start = log_entry.date_range_start
                date_range_end = log_entry.date_range_end

            if log_entry.location_affected not in locations_affected:
                locations_affected.append(log_entry.location_affected)

            is_first_loop = False
            is_first_date_location = False

        # Build the final change log
        dates_locations = DatesLocations(date_range_start, date_range_end, locations_affected)
        dates_and_locations.append(dates_locations)
        change_logs.append(build_change_log(data_product_id, log_values, dates_and_locations))
    return change_logs


def build_change_log(data_product_id, log_values, dates_and_locations) -> ChangeLog:
    """Populate a change log object."""
    return ChangeLog(data_product_id=data_product_id,
                     issue=log_values['issue'],
                     issue_date=log_values['issue_date'],
                     resolution=log_values['resolution'],
                     resolution_date=log_values['resolution_date'],
                     dates_locations=dates_and_locations)
