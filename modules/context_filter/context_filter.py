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

    def filter(self):
        source_files = self.get_source_files()
        matching_files = self.get_matching_files(source_files)
        self.link_matching_files(matching_files)

    def get_source_files(self) -> Dict[str, List[Dict[str, Path]]]:
        """Organize all files in the input directory by source ID and a list of data types and associated paths."""
        source_files = {}
        for path in self.input_path.rglob('*'):
            if path.is_file():
                parts = path.parts
                source_id: str = parts[self.source_id_index]
                data_type: str = parts[self.data_type_index]
                log.debug(f'source_id: {source_id} data_type: {data_type}')
                files = source_files.get(source_id)
                # if first iteration for this data source
                if files is None:
                    files = []
                files.append({data_type: path})
                source_files.update({source_id: files})
        return source_files

    def get_matching_files(self, source_files: Dict[str, List[Dict[str, Path]]]) -> List[List[Dict[str, Path]]]:
        """
        Group files by location context group.

        :param source_files: File paths by data type.
        """
        matching_files = []
        for source_id in source_files:
            file_paths: List[Dict[str, Path]] = source_files.get(source_id)
            for data_type_path in file_paths:
                for data_type in data_type_path:
                    file: Path = data_type_path.get(data_type)
                    if data_type == 'location':
                        location_file_parser = AssetLocationFileParser(file)
                        if location_file_parser.contains_context(self.context):
                            matching_files.append(file_paths)
        return matching_files

    def link_matching_files(self, matching_files: List[List[Dict[str, Path]]]):
        """
        Pull files by data type and link into output directory.

        :param matching_files: Files organized by data type.
        """
        for file_paths in matching_files:
            for data_type_files in file_paths:
                for data_type in data_type_files:
                    path = data_type_files.get(data_type)
                    source_type, year, month, day, source_id, file_type = self.data_file_path.parse(path)
                    link_path = Path(self.output_path, source_type, year, month, day, source_id,
                                     file_type, *path.parts[self.data_type_index + 1:])
                    link_path.parent.mkdir(parents=True, exist_ok=True)
                    link_path.symlink_to(path)
