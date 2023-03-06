#!/usr/bin/env python3
from pathlib import Path
from level1_consolidate.level1_consolidate_config import Config

import structlog

log = structlog.get_logger()

class Level1Consolidate:
    def __init__(self, config: Config) -> None:
        """
        Constructor.
        
        :param config: named tuple.
        """
        self.in_path = config.in_path
        self.out_path = config.out_path
        self.relative_path_index = config.relative_path_index
        self.group_index=config.group_index
        self.group_metadata_index=config.group_metadata_index
        self.group_metadata_names=config.group_metadata_names
        self.data_type_index=config.data_type_index
        self.data_type_names=config.data_type_names
    def consolidate_paths(self) -> None:
        """
        Consolidate paths to the output structure for level 1 data. 

        """
        for path in self.in_path.rglob('*'):
            if path.is_file():
                link_path = self.consolidate_path(path)
                if link_path is not None and not link_path.exists():
                    link_path.parent.mkdir(parents=True, exist_ok=True)
                    log.debug(f'path: {path} link_path: {link_path}')
                    link_path.symlink_to(path)
    def consolidate_path(self, path: Path) -> Path:
        """
        Place an file in the output according to the desired path structure.
    
        :param path: The source path.

        """
        path_parts = path.parts
        new_path = Path(self.out_path)
        # Group metadata
        group_metadata_name_check: str = None
        data_type_name_check: str = None
        if len(path_parts) > self.group_metadata_index:
            group_metadata_name_check: str = path_parts[self.group_metadata_index]
        if len(path_parts) > self.data_type_index:
            data_type_name_check: str = path_parts[self.data_type_index]
        if group_metadata_name_check is not None and group_metadata_name_check in self.group_metadata_names:
            # We have a group metadata file. Place it in the output within the group metadata folder, which is a direct child of the group
            group_part = path_parts[self.relative_path_index:self.group_index+1]
            new_path = new_path.joinpath(Path(*group_part))
            group_metadata_part = path_parts[self.group_metadata_index:]
            new_path = new_path.joinpath(*group_metadata_part)
        elif data_type_name_check is not None and data_type_name_check in self.data_type_names:
            # We have a data output as specified in the input parameters
            group_part = path_parts[self.relative_path_index:self.group_index+1]
            new_path = new_path.joinpath(Path(*group_part))
            data_type_part = path_parts[self.data_type_index:]
            new_path = new_path.joinpath(*data_type_part)
        else:
            new_path: Path = None
            log.debug(f'ignoring: {path}')
        return new_path
