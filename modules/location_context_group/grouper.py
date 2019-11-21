import os
import pathlib

import structlog

import lib.file_linker as file_linker
import lib.file_crawler as file_crawler
import lib.location_file_context as location_file_context

log = structlog.get_logger()


def group(in_path, out_path, context):
    """
    Group files in the input directory by context.
    :param in_path: The input path.
    :param out_path: The output path.
    :param context: The context to match.
    """
    sources = {}
    for file_path in file_crawler.crawl(in_path):
        parts = pathlib.Path(file_path).parts
        source_id = parts[7]
        data_type = parts[8]
        log.debug(f'source_id: {source_id} data_type: {data_type}')
        paths = sources.get(source_id)
        if paths is None:
            paths = []
        paths.append({data_type: file_path})
        sources.update({source_id: paths})
    group_sources(sources, context, out_path)


def group_sources(sources, context, out_path):
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
                    link_source(file_paths, out_path)  # Link all the paths under this source.


def link_source(file_paths, out_path):
    """
    Get file paths by data type and link into output directory.
    :param file_paths: Dict of file paths by data type.
    :param out_path: The output path.
    """
    for p in file_paths:
        for data_type in p:
            file_path = p.get(data_type)
            link_path(file_path, out_path)


def link_path(file_path, out_path):
    """
    Link the file path into out_dir/dir_name.
    :param file_path: A path to link into the output path.
    :param out_path: The output path.
    """
    parts = pathlib.Path(file_path).parts
    source_type = parts[3]
    year = parts[4]
    month = parts[5]
    day = parts[6]
    source_id = parts[7]
    data_type = parts[8]
    filename = parts[9]
    log.debug(f't: {source_type} Y: {year} M: {month} D: {day} id: {source_id} type: {data_type} file: {filename}')
    target_dir = os.path.join(out_path, source_type, year, month, day, source_id, data_type)
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    log.debug(f'target_dir: {target_dir}')
    destination = os.path.join(target_dir, filename)
    log.debug(f'source: {file_path} destination: {destination}')
    file_linker.link(file_path, destination)
