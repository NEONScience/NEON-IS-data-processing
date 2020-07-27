#!/usr/bin/env python3
from pathlib import Path
from typing import NamedTuple, List

from structlog import get_logger

import common.location_file_parser as file_parser
from location_group_path.location_group_path_config import Config
from location_group_path.path_parser import PathParser

log = get_logger()


class PathGroup(NamedTuple):
    associated_paths: List[Path]
    groups: List[str]


class LocationGroupPath:
    """Class adds location context groups to location file paths."""

    def __init__(self, config: Config) -> None:
        self.source_path = config.source_path
        self.out_path = config.out_path
        self.group = config.group
        self.path_parser = PathParser(config)
        self.location_type = 'location'

    def add_groups_to_paths(self) -> None:
        """
        Add the location context groups, one group per path. Multiple paths are created
        for the same file if more than one context group is present.
        """
        path_groups = self.get_paths_and_groups()
        self.link_files(path_groups)

    def get_paths_and_groups(self) -> List[PathGroup]:
        """Get the location file paths and associated context groups."""
        path_groups: List[PathGroup] = []
        for path in self.source_path.rglob('*'):
            if path.is_file():
                source_type, year, month, day, location, data_type, remainder = self.path_parser.parse(path)
                # get the location context groups from the location file
                if data_type == self.location_type:
                    context = file_parser.get_context(path)
                    groups = file_parser.get_context_matches(context, self.group)
                    associated_paths: List[Path] = []
                    # get all the files in the directory containing this location file
                    location_path = path.parent.parent
                    for associated_path in location_path.rglob('*'):
                        if associated_path.is_file():
                            log.debug(f'associated_path: {associated_path}')
                            associated_paths.append(associated_path)
                    path_groups.append(PathGroup(associated_paths, groups))
        return path_groups

    def link_files(self, path_groups: List[PathGroup]) -> None:
        """
        Link the files into the output path and add the location context groups into the path.

        :param path_groups: File paths for linking and location context groups.
        """
        for path_group in path_groups:
            for path in path_group.associated_paths:
                source_type, year, month, day, location, data_type, remainder = self.path_parser.parse(path)
                for group in path_group.groups:
                    link_path = Path(self.out_path, source_type, year, month, day,
                                     group, location, data_type, *remainder)
                    link_path.parent.mkdir(parents=True, exist_ok=True)
                    if not link_path.exists():
                        log.debug(f'file: {path} link: {link_path}')
                        link_path.symlink_to(path)
