#!/usr/bin/env python3
from pathlib import Path
import structlog

from common.location_file_parser import LocationFileParser

log = structlog.get_logger()


class ContextFilter(object):

    def __init__(self, *, input_path: Path, output_path: Path, context: str,
                 source_type_index: int,
                 year_index: int,
                 month_index: int,
                 day_index: int,
                 source_id_index: int,
                 data_type_index: int):
        """
        Constructor.

        :param input_path: The source file path.
        :param output_path: The output path for linking files.
        :param context: The context to match.
        :param source_type_index: The path index for the source type.
        :param year_index: The path index for the year.
        :param month_index: The path index for the month.
        :param day_index: The path index for the day.
        :param source_id_index: The path index for the source ID.
        :param data_type_index: The path index for the data type.
        """
        self.input_path = input_path
        self.output_path = output_path
        self.context = context
        self.source_type_index = source_type_index
        self.year_index = year_index
        self.month_index = month_index
        self.day_index = day_index
        self.source_id_index = source_id_index
        self.data_type_index = data_type_index

    def filter(self):
        files_by_source = self.get_source_files()
        matching_files = self.get_matching_files(files_by_source)
        self.link_matching_files(matching_files)

    def get_source_files(self):
        """Organize all files in the input directory by source ID."""
        source_files = {}
        for path in self.input_path.rglob('*'):
            if path.is_file():
                parts = path.parts
                source_id = parts[self.source_id_index]
                data_type = parts[self.data_type_index]
                log.debug(f'source_id: {source_id} data_type: {data_type}')
                files = source_files.get(source_id)
                # if first iteration for this data source
                if files is None:
                    files = []
                files.append({data_type: path})
                source_files.update({source_id: files})
        return source_files

    def get_matching_files(self, source_files: dict):
        """
        Group files by location context group and write to output if the
        location file context matches the given context.

        :param source_files: File paths by data type.
        """
        matching_files = []
        for source_id in source_files:
            file_paths = source_files.get(source_id)
            for data_type_path in file_paths:
                for data_type in data_type_path:
                    file = data_type_path.get(data_type)
                    if data_type == 'location':
                        location_file_parser = LocationFileParser(file)
                        if location_file_parser.contains_context(self.context):
                            matching_files.append(file_paths)
        return matching_files

    def link_matching_files(self, matching_files: list):
        """
        Pull files by the data type and link into output directory.

        :param matching_files: Files organized by data type.
        """
        for file_paths in matching_files:
            for data_type_files in file_paths:
                for data_type in data_type_files:
                    path = data_type_files.get(data_type)
                    parts = path.parts
                    source_type = parts[self.source_type_index]
                    year = parts[self.year_index]
                    month = parts[self.month_index]
                    day = parts[self.day_index]
                    source_id = parts[self.source_id_index]
                    file_data_type = parts[self.data_type_index]
                    link_path = Path(self.output_path, source_type, year, month, day,
                                     source_id, file_data_type, *parts[self.data_type_index + 1:])
                    link_path.parent.mkdir(parents=True, exist_ok=True)
                    link_path.symlink_to(path)
