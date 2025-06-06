#!/usr/bin/env python3
from pathlib import Path
import structlog
from typing import List, Dict

import common.location_file_parser as location_file_parser

from context_filter.context_filter_config import Config
from context_filter.path_parser import PathParser

log = structlog.get_logger()


class ContextFilter:

    def __init__(self, config: Config) -> None:
        self.input_path = config.in_path
        self.output_path = config.out_path
        self.context = config.context
        self.trim_index = config.trim_index
        self.path_parser = PathParser(config)

    def filter_files(self) -> None:
        self.link_matching_paths(self.get_matching_paths(self.get_source_paths()))

    def get_source_paths(self) -> Dict[str, List[Dict[str, Path]]]:
        """Organize paths in the input directory by source ID with data types and associated paths."""
        source_paths: Dict[str, List[Dict[str, Path]]] = {}
        for path in self.input_path.rglob('*'):
            if path.is_file():
                source_id, data_type = self.path_parser.parse(path)
                log.debug(f'source_id: {source_id} data_type: {data_type}')
                paths = source_paths.get(source_id)
                # if first iteration for this data source
                if paths is None:
                    paths = []
                paths.append({data_type: path})
                source_paths.update({source_id: paths})
        return source_paths

    def get_matching_paths(self, source_paths: Dict[str, List[Dict[str, Path]]]) -> List[List[Dict[str, Path]]]:
        """
        Find paths matching the context.

        :param source_paths: Paths by data type.
        """
        matching_paths = []
        for source_id, path_list in source_paths.items():
            for paths in path_list:
                for data_type, path in paths.items():
                    if data_type == 'location':
                        file_context = location_file_parser.get_context(path)
                        if ('|' in self.context and self.check_all_context(file_context)) or self.context in file_context:
                            matching_paths.append(path_list)
        return matching_paths

    def check_all_context(self, file_context: List[str]) -> bool:
        """
        When multiple contexts passed in and separated by |, check if all of them are part of file context.

        :param file_context: file context.
        return True or False
        """
        multi_context = self.context.split('|')
        for con in multi_context:
            if con not in file_context:
                return False
        return True

    def link_matching_paths(self, matching_paths: List[List[Dict[str, Path]]]) -> None:
        """
        Link the matching paths into output directory.

        :param matching_paths: Paths organized by data type.
        """
        for path_list in matching_paths:
            for paths in path_list:
                for path in paths.values():
                    link_path = Path(self.output_path, *path.parts[self.trim_index:])
                    log.debug(f'link_path: {link_path}')
                    link_path.parent.mkdir(parents=True, exist_ok=True)
                    if not link_path.exists():
                        link_path.symlink_to(path)
