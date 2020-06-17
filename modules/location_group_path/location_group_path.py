#!/usr/bin/env python3
from pathlib import Path

from structlog import get_logger

from common.asset_location_file_parser import AssetLocationFileParser

from location_group_path.data_file_path import DataFilePath

log = get_logger()


class LocationGroupPath(object):
    """Class adds location context groups to location file paths."""

    def __init__(self, *, source_path: Path, out_path: Path, group: str, data_file_path: DataFilePath):
        """
        Constructor.

        :param source_path: The path to containing location files.
        :param group: The group to match in the location files.
        :param data_file_path: The file path parser.
        """
        self.source_path = source_path
        self.out_path = out_path
        self.group = group
        self.data_file_path = data_file_path

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
                source_type, year, month, day, location, data_type, remainder = self.data_file_path.parse(path)
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
                    location_file_parser = AssetLocationFileParser(path)
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
