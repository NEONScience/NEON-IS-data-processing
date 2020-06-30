#!/usr/bin/env python3
from pathlib import Path
from typing import NamedTuple, Tuple, List

from structlog import get_logger

import common.location_file_parser as file_parser

from location_group_path.location_group_path_config import Config
from location_group_path.data_path_parser import DataPathParser

log = get_logger()


class PathElements(NamedTuple):
    source_type: str
    year: str
    month: str
    day: str
    location: str
    data_type: str
    remainder: Tuple[str]


class PathParts(NamedTuple):
    path: Path
    elements: PathElements


class PathGroups(NamedTuple):
    paths: List[PathParts]
    groups: List[str]


class LocationGroupPath:
    """Class adds location context groups to location file paths."""

    def __init__(self, config: Config) -> None:
        self.source_path = config.source_path
        self.out_path = config.out_path
        self.group = config.group
        self.data_path_parser = DataPathParser(config)
        self.location_type = 'location'

    def add_groups_to_paths(self) -> None:
        """
        Add the location context groups, one group per path. Multiple paths are created
        for the same file if more than one context group is present.
        """
        path_groups = self._get_paths_and_groups()
        self._link_files(path_groups)

    def _get_paths_and_groups(self) -> PathGroups:
        """Get the location file paths and associated context groups."""
        paths: List[PathParts] = []
        groups: List[str] = []
        for path in self.source_path.rglob('*'):
            if path.is_file():
                source_type, year, month, day, location, data_type, remainder = self.data_path_parser.parse(path)
                elements = PathElements(source_type, year, month, day, location, data_type, remainder)
                # add the original file path and path parts
                paths.append(PathParts(path, elements))
                # get the location context group from the location file
                if data_type == self.location_type:
                    context = file_parser.get_context(path)
                    groups.extend(file_parser.get_context_matches(context, self.group))
        if len(groups) == 0:
            log.error(f'No location directory found for group {self.group}.')
        return PathGroups(paths, groups)

    def _link_files(self, path_groups: PathGroups) -> None:
        """
        Loop through the files and link into them into the output path while
        adding the location context group into the path.

        :param path_groups: File paths for linking and location context groups.
        """
        paths: List[PathParts] = path_groups.paths
        groups: List[str] = path_groups.groups
        for path_parts in paths:
            file_path: Path = path_parts.path
            elements: PathElements = path_parts.elements
            source_type: str = elements.source_type
            year: str = elements.year
            month: str = elements.month
            day: str = elements.day
            location: str = elements.location
            data_type: str = elements.data_type
            remainder: Tuple[str] = elements.remainder
            for group in groups:
                link_path = Path(self.out_path, source_type, year, month, day, group, location, data_type,
                                 *remainder[0:])
                link_path.parent.mkdir(parents=True, exist_ok=True)
                log.debug(f'file: {file_path} link: {link_path}')
                link_path.symlink_to(file_path)
