#!/usr/bin/env python3
# ---------------------------------------------------------------------------
from pathlib import Path
import sys
from typing import NamedTuple,List,Iterator,Tuple

from structlog import get_logger

import common.group_file_parser as group_file_parser
from group_path.group_path_config import Config
from group_path.path_parser import PathParser
from group_path.dictionary_list import DictionaryList
from common.err_datum import err_datum_path

log = get_logger()


class PathGroup(NamedTuple):
    group_file_path: Path
    associated_paths: List[Path]
    groups: List[str]


class GroupPath:
    """Class adds groups to file paths."""

    def __init__(self, config: Config) -> None:
        self.group_assignment_path = config.group_assignment_path
        self.location_focus_path = config.location_focus_path
        self.group_focus_path = config.group_focus_path
        self.out_path = config.out_path
        self.err_path = config.err_path
        self.group = config.group
        self.path_parser = PathParser(config)
        self.group_data_type = 'group'
        self.group_assignment_key_indices = (config.group_assignment_year_index, config.group_assignment_month_index, config.group_assignment_day_index, config.group_assignment_member_index)
        self.location_focus_key_indices = (config.location_focus_year_index, config.location_focus_month_index, config.location_focus_day_index, config.location_focus_location_index)
        self.group_focus_key_indices = (config.group_focus_year_index, config.group_focus_month_index, config.group_focus_day_index, config.group_focus_group_index)

    def add_groups_to_paths(self) -> None:
        """
        Add the groups, one group per path. Multiple paths are created
        for the same file if more than one group is present.
        """
        location_focus_path_groups, group_focus_path_groups = self.get_paths_and_groups()
        if location_focus_path_groups is not None:
            self.link_files(location_focus_path_groups,'location_focus')
        if group_focus_path_groups is not None:
            self.link_files(group_focus_path_groups,'group_focus')

    def get_paths_and_groups(self) -> Tuple[List[PathGroup], List[PathGroup]]:  # List[PathGroup]:
        """Pre-process the group_assignment paths to form keys and associated paths"""
        group_assignment_key_paths = DictionaryList()
        group_assignment_keys: List[set] = self.get_keys(self.group_assignment_path, self.group_assignment_key_indices, 
                                                         group_assignment_key_paths)
        log.debug(f'Group_assignment keys: {group_assignment_keys}')

        """Process the location_focus paths """
        # just need one instance of location_focus here since each location_focus repo will be sent i individually as a union joined with the group_assignment_focus
        if self.location_focus_path is not None:
            # Process the location_focus paths to form keys and associated paths
            location_focus_key_paths = DictionaryList()
        group_assignment_keys: List[set] = self.get_keys(self.group_assignment_path, self.group_assignment_key_indices, group_assignment_key_paths)
        log.debug(f'Group_assignment keys: {group_assignment_keys}')

        """Process the location_focus paths """ 
        # just need one instance of location_focus here since each location_focus repo will be sent i individually as a union joined with the group_assignment_focus
        if self.location_focus_path is not None:
            # Process the location_focus paths to form keys and associated paths
            location_focus_key_paths = DictionaryList()
            location_focus_keys: List[set] = self.get_keys(self.location_focus_path, self.location_focus_key_indices, location_focus_key_paths)
            log.debug(f'Location focus keys: {location_focus_keys}')
            # Get the intersection of the group_assignment keys and location_focus keys
            location_focus_joined_keys: set = location_focus_keys.intersection(group_assignment_keys)
            # Get the group file paths and associated groups for the location focus paths
            location_focus_path_groups: List[PathGroup] = self.get_path_groups(location_focus_joined_keys,group_assignment_key_paths,location_focus_key_paths)
        else: 
            location_focus_path_groups: List[PathGroup] = None
        
        """Process the group_focus paths """ 
        # just need one instance of group_focus here since each group_focus repo will be sent i individually as a union joined with the group_assignment_focus
        if self.group_focus_path is not None:
            group_focus_key_paths = DictionaryList()
            group_focus_keys: List[set] = self.get_keys(self.group_focus_path, self.group_focus_key_indices, group_focus_key_paths)
            log.debug(f'Group focus keys: {group_focus_keys}')
            # Get the intersection of the group_assignment keys and location_focus keys
            group_focus_joined_keys: set = group_focus_keys.intersection(group_assignment_keys)
            # Get the group file paths and associated groups for the group focus paths
            group_focus_path_groups: List[PathGroup] = self.get_path_groups(group_focus_joined_keys,group_assignment_key_paths,group_focus_key_paths)
        else: 
            group_focus_path_groups: List[PathGroup] = None
   
        return location_focus_path_groups, group_focus_path_groups

    def get_path_groups(self, joined_keys: set, group_assignment_key_paths: DictionaryList, data_key_paths: DictionaryList) -> List[PathGroup]:
        """
        Get the group file paths and associated groups.

        :param joined_keys: The intersection of the keys between the group_assignment paths and the data paths.
        :param group_assignment_key_paths: File paths and associated keys for the group_assignment repo.
        :param data_key_paths: File paths and associated keys for the data repo.
        """
        path_groups: List[PathGroup] = []
        for key in joined_keys:
            # Loop through the group_assignment paths for the key to look for the groups file
            for path in group_assignment_key_paths[key]:
                if path.is_file():
                    # If this is the group file, proceed
                    year, month, day, member, data_type, remainder = self.path_parser.parse_group_assignment(path)
                    if data_type == self.group_data_type:
                        # get the groups from the group file
                        groups_all = group_file_parser.get_group(path)
                        log.debug(f'groups found: {groups_all}')
                        groups = group_file_parser.get_group_matches(groups_all, self.group)
                        log.debug(f'groups matched: {groups}')
                        # Get the data key paths associated with this key
                        associated_paths: List[Path] = data_key_paths[key]
                        path_groups.append(PathGroup(path, associated_paths, groups))
        return path_groups

    def link_files(self, path_groups: List[PathGroup], path_type: str) -> None:
        """
        Link the files into the output path and add the groups into the path.

        :param path_groups: File paths for linking and groups.
        :param path_type: The type of data path. Either 'group_focus' or 'location_focus' 
        
        """
        DirErrBase = Path(self.err_path)
        for path_group in path_groups:
            # Parse the group file path and link to output 
            year, month, day, member, data_type, remainder = self.path_parser.parse_group_assignment(path_group.group_file_path)
            if path_group.group_file_path.is_file():
                dataDir_routed = Path(path_group.group_file_path).parent
            try:
                for group in path_group.groups:
                    link_path = Path(self.out_path,year,month,day,group,data_type,*remainder)
                    link_path.parent.mkdir(parents=True,exist_ok=True)
                    if not link_path.exists():
                        log.debug(f'file: {path_group.group_file_path} link: {link_path}')
                        link_path.symlink_to(path_group.group_file_path)
            except Exception:
                err_msg = sys.exc_info()
                err_datum_path(err=err_msg,DirDatm=str(dataDir_routed),DirErrBase=DirErrBase,
                               RmvDatmOut=True,DirOutBase=self.out_path)
            # Parse the associated paths and link to output 
            if path_type == 'location_focus':
                for path in path_group.associated_paths:
                    if path.is_file():
                        source_type, year, month, day, location, remainder = self.path_parser.parse_location_focus(path)
                        for group in path_group.groups:
                            link_path = Path(self.out_path, year, month, day,
                                             group, source_type, location, *remainder)
                            link_path.parent.mkdir(parents=True, exist_ok=True)
                            if not link_path.exists():
                                log.debug(f'file: {path} link: {link_path}')
                                link_path.symlink_to(path)
            elif path_type == 'group_focus':
                for path in path_group.associated_paths:
                    if path.is_file():
                        year, month, day, existing_group, remainder = self.path_parser.parse_group_focus(path)
                        for group in path_group.groups:
                            link_path = Path(self.out_path, year, month, day,
                                             group, existing_group, *remainder)
                            link_path.parent.mkdir(parents=True, exist_ok=True)
                            if not link_path.exists():
                                log.debug(f'file: {path} link: {link_path}')
                                link_path.symlink_to(path)
       
    def get_keys(self, input_path: Path, key_indices, key_paths: DictionaryList) -> set:
        """
        Loop through and associate keys and paths for joining.
    
        :param input_path: The join path.
        :Param key_indices: the path indices to form the key
        :param key_paths: Paths by key.
        :return: The set of keys.
        """
        keys = set()
        for path in input_path.rglob('*'):
            if len(path.parts) > max(key_indices):
                key = self.get_key(path, key_indices)
                keys.add(key)
                key_paths[key] = path
        return keys

    @staticmethod
    def get_key(path: Path, key_indices: list) -> str:
        """
        Create a key by concatenating path elements at the join indices.
        Paths to join will have the same key.
    
        :param path: A path.
        :param key_indices: Path element indices to use for the key.
        :return: The key.
        """
        key = ''
        parts = path.parts
        for index in key_indices:
            part = parts[index]
            key += part
        return key
    
