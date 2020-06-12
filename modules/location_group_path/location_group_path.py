#!/usr/bin/env python3
from pathlib import Path

from structlog import get_logger

from common.location_file_parser import LocationFileParser

log = get_logger()


class LocationGroupPath(object):
    """Class adds location context groups to location file paths."""

    def __init__(self, *, source_path: Path, out_path: Path, group: str,
                 source_type_index: int,
                 year_index: int,
                 month_index: int,
                 day_index: int,
                 location_index: int,
                 data_type_index: int):
        """
        Constructor.

        :param source_path: The path to containing location files.
        :param group: The group to match in the location files.
        :param source_type_index: The input path index of the source type.
        :param year_index: The input path index of the year.
        :param month_index: The input path index of the month.
        :param day_index: The input path index of the day.
        :param location_index: The input path index of the location.
        :param data_type_index: The input path index of the data type.
        """
        self.source_path = source_path
        self.out_path = out_path
        self.group = group
        self.source_type_index = source_type_index
        self.year_index = year_index
        self.month_index = month_index
        self.day_index = day_index
        self.location_index = location_index
        self.data_type_index = data_type_index

    def add_groups_to_paths(self):
        """
        Add the location context groups, one group per path. Multiple paths are created
        for the same file if more than one context group is present.
        """
        path_data = self.get_paths_and_groups()
        paths = path_data.get('paths')
        groups = path_data.get('groups')
        self.link_files(paths, groups)

    def get_paths_and_groups(self):
        """Get the location file paths and associated context groups."""
        paths = []
        groups = []
        for path in self.source_path.rglob('*'):
            if path.is_file():
                parts = path.parts
                source_type = parts[self.source_type_index]
                year = parts[self.year_index]
                month = parts[self.month_index]
                day = parts[self.day_index]
                location = parts[self.location_index]
                data_type = parts[self.data_type_index]
                remainder = parts[self.data_type_index + 1:]
                path_parts = {
                    "source_type": source_type,
                    "year": year,
                    "month": month,
                    "day": day,
                    "location": location,
                    "data_type": data_type,
                    "remainder": remainder
                }
                # add the original file path and path parts
                paths.append({"path": path, "parts": path_parts})
                # get the location context group from the location file
                if data_type == 'location':
                    location_file_parser = LocationFileParser(path)
                    groups.extend(location_file_parser.matching_context_items(self.group))
        if len(groups) == 0:
            log.error(f'No location directory found for group {self.group}.')
        return {'paths': paths, 'groups': groups}

    def link_files(self, paths: list, groups: list):
        """
        Loop through the files and link into them into the output path while
         adding the location context group into the path.

        :param paths: File paths for linking.
        :param groups: The location context groups.
        """
        for path in paths:
            file_path = path.get('path')
            parts = path.get('parts')
            source_type = parts.get("source_type")
            year = parts.get("year")
            month = parts.get("month")
            day = parts.get("day")
            location = parts.get("location")
            data_type = parts.get("data_type")
            remainder = parts.get("remainder")
            for group in groups:
                link_path = Path(self.out_path, source_type, year, month, day, group, location, data_type,
                                 *remainder[0:])
                link_path.parent.mkdir(parents=True, exist_ok=True)
                log.debug(f'file: {file_path} link: {link_path}')
                link_path.symlink_to(file_path)
