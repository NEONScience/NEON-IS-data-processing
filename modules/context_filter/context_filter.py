#!/usr/bin/env python3
from pathlib import Path
import structlog
from typing import List, Dict

from common.asset_location_file_parser import AssetLocationFileParser
from context_filter.data_file_path import DataFilePath

log = structlog.get_logger()


class ContextFilter(object):

    def __init__(self, *, input_path: Path, output_path: Path, context: str, data_file_path: DataFilePath):
        """
        Constructor.

        :param input_path: The source file path.
        :param output_path: The output path for linking files.
        :param context: The context to match.
        :param data_file_path: The file path parser.
        """
        self.input_path = input_path
        self.output_path = output_path
        self.context = context
        self.data_file_path = data_file_path
        self.source_id_index = data_file_path.source_id_index
        self.data_type_index = data_file_path.data_type_index
        self.location_type = 'location'

    def filter(self):
        self.link_matching_paths(self.get_matching_paths(self.get_source_paths()))

    def get_source_paths(self) -> Dict[str, List[Dict[str, Path]]]:
        """Organize paths in the input directory by source ID, data types, and associated paths."""
        source_paths: Dict[str, List[Dict[str, Path]]] = {}
        for path in self.input_path.rglob('*'):
            if path.is_file():
                parts = path.parts
                source_id: str = parts[self.source_id_index]
                data_type: str = parts[self.data_type_index]
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
        Group paths by location context group.

        :param source_paths: Paths by data type.
        """
        matching_paths = []
        for source_id, path_list in source_paths.items():
            for paths in path_list:
                for data_type, path in paths.items():
                    if data_type == self.location_type:
                        parser = AssetLocationFileParser(path)
                        if parser.contains_context(self.context):
                            matching_paths.append(path_list)
        return matching_paths

    def link_matching_paths(self, matching_paths: List[List[Dict[str, Path]]]) -> None:
        """
        Pull paths by data type and link into output directory.

        :param matching_paths: Paths organized by data type.
        """
        for path_list in matching_paths:
            for paths in path_list:
                for path in paths.values():
                    source_type, year, month, day, source_id, data_type = self.data_file_path.parse(path)
                    link_path = Path(self.output_path, source_type, year, month, day, source_id,
                                     data_type, *path.parts[self.data_type_index + 1:])
                    link_path.parent.mkdir(parents=True, exist_ok=True)
                    link_path.symlink_to(path)
