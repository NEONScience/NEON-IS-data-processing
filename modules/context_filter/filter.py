#!/usr/bin/env python3
import os
import pathlib

import structlog

import lib.file_linker as file_linker
import lib.file_crawler as file_crawler
import lib.location_file_context as location_file_context

log = structlog.get_logger()


class ContextFilter(object):

    def __init__(self, source_type_index, year_index, month_index, day_index, source_id_index, data_type_index):
        """
        Constructor.

        :param source_type_index: The index for the source type in the file path.
        :type source_type_index: int
        :param year_index: The index for the year in the file path.
        :type year_index: int
        :param month_index: The index for the month in the file path.
        :type month_index: int
        :param day_index: The index for the day in the file path.
        :type day_index: int
        :param source_id_index: The index for the source ID in the file path.
        :type source_id_index: int
        :param data_type_index: The index for the data type in the file path.
        :type data_type_index: int
        """
        self.source_type_index = source_type_index
        self.year_index = year_index
        self.month_index = month_index
        self.day_index = day_index
        self.source_id_index = source_id_index
        self.data_type_index = data_type_index

    def filter(self, in_path, out_path, context):
        """
        Group files in the input directory by context.

        :param in_path: The input path.
        :type in_path: str
        :param out_path: The output path.
        :type out_path: str
        :param context: The context to match.
        :type context: str
        """
        sources = {}
        for file_path in file_crawler.crawl(in_path):
            parts = pathlib.Path(file_path).parts
            source_id = parts[self.source_id_index]
            data_type = parts[self.data_type_index]
            log.debug(f'source_id: {source_id} data_type: {data_type}')
            paths = sources.get(source_id)
            if paths is None:
                paths = []
            paths.append({data_type: file_path})
            sources.update({source_id: paths})
        self.group_sources(sources, context, out_path)

    def group_sources(self, sources, context, out_path):
        """
        Group the source files from the input directory.

        :param sources: File paths by data type.
        :type sources: dict
        :param context: The context to match.
        :type context: str
        :param out_path: The output path.
        :type out_path: str
        """
        for source in sources:
            file_paths = sources.get(source)
            for path in file_paths:
                for data_type in path:
                    file_path = path.get(data_type)
                    if data_type == 'location' and location_file_context.match(file_path, context):
                        self.link_source(file_paths, out_path)  # Link all the files for this source.

    @staticmethod
    def link_source(file_paths_by_type, out_path):
        """
        Get file paths by data type and link into output directory.

        :param file_paths_by_type: File paths by data type.
        :type file_paths_by_type dict
        :param out_path: The output path.
        :type out_path: str
        """
        for path_by_type in file_paths_by_type:
            for data_type in path_by_type:
                file_path = path_by_type.get(data_type)
                parts = pathlib.Path(file_path).parts
                destination = os.path.join(out_path, *parts[3:])
                log.debug(f'source: {file_path} destination: {destination}')
                file_linker.link(file_path, destination)
