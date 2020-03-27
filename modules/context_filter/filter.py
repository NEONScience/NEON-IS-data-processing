import os
import pathlib

import structlog

import lib.file_linker as file_linker
import lib.file_crawler as file_crawler
import lib.location_file_context as location_file_context

log = structlog.get_logger()


class ContextFilter(object):

    def __init__(self, source_type_index, year_index, month_index, day_index, source_id_index, data_type_index):
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
        :param out_path: The output path.
        :param context: The context to match.
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
        :param sources: Dict of file paths by data type.
        :param context: The context to match.
        :param out_path: The output path.
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
        :param file_paths_by_type: Dict of file paths by data type.
        :param out_path: The output path.
        """
        for path_by_type in file_paths_by_type:
            for data_type in path_by_type:
                file_path = path_by_type.get(data_type)
                parts = pathlib.Path(file_path).parts
                destination = os.path.join(out_path, *parts[3:])
                log.debug(f'source: {file_path} destination: {destination}')
                file_linker.link(file_path, destination)
