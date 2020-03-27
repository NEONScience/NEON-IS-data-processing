import os
import sys
import pathlib

from structlog import get_logger

import lib.file_linker as file_linker
import lib.file_crawler as file_crawler
from lib.merged_data_filename import MergedDataFilename

log = get_logger()


def analyze(data_dir, out_dir):
    """
    Analyze time series data to calculate additional time padding required for processing with thresholds.
    :param data_dir:
    :param out_dir:
    :return:
    """
    out_dir_parts = list(pathlib.Path(out_dir).parts)
    manifest_file = 'manifest.txt'
    try:
        for root, dirs, files in os.walk(data_dir):
            for filename in files:
                if filename == manifest_file:
                    # read manifest
                    dates = [date.rstrip() for date in open(os.path.join(root, filename))]
                    # check for existence of complete manifest
                    dates_not_found = []
                    for date in dates:
                        dates_not_found.append(date)
                    for date in dates:
                        for data_file in os.listdir(root):
                            log.debug(f'data_file: {data_file}')
                            if data_file != manifest_file:
                                data_file_date = MergedDataFilename(data_file).date()
                                log.debug(f'checking data file date: {data_file_date} and '
                                          f'manifest date {date} in {dates_not_found}')
                                if date in data_file_date and date in dates_not_found:
                                    log.debug(f'found data for: {date}')
                                    dates_not_found.remove(date)
                    # if complete, symlink to output repository
                    if not dates_not_found:
                        for data_file in os.listdir(root):
                            # TODO: The root is 'data', need to go one directory up.
                            if data_file != manifest_file:
                                source_path = os.path.join(root, data_file)
                                destination_parts = pathlib.Path(source_path).parts
                                destination_parts = list(destination_parts)
                                for index in range(1, len(out_dir_parts)):
                                    destination_parts[index] = out_dir_parts[index]
                                destination_path = os.path.join(*destination_parts)
                                log.debug(f'linking {source_path} to {destination_path}')
                                file_linker.link(source_path, destination_path)
                                write_thresholds(source_path, destination_path)
                        # Go up one directory and get any ancillary files to write.
                        write_ancillary_data(out_dir, root)


    except Exception:
        exception_type, exception_obj, exception_tb = sys.exc_info()
        log.error("Exception at line " + str(exception_tb.tb_lineno) + ": " + str(sys.exc_info()))


def write_thresholds(source_path, destination_path):
    """
    Write thresholds if they exist in the source repository.
    :param source_path:
    :param destination_path:
    :return:
    """
    threshold_dir = 'threshold'
    threshold_filename = 'thresholds.json'
    source_dir = pathlib.Path(source_path).parent.parent
    destination_dir = pathlib.Path(destination_path).parent.parent
    source = os.path.join(source_dir, threshold_dir, threshold_filename)
    if os.path.exists(source):
        destination = os.path.join(destination_dir, threshold_dir, threshold_filename)
        log.debug(f'linking {source} to {destination}')
        file_linker.link(source, destination)


def write_ancillary_data(out_dir, root):
    """
    Write any additional files present in the input directory
    beyond data and thresholds into the output directory.
    :param out_dir:
    :param root:
    :return:
    """
    parent_dir = pathlib.Path(root).parent
    for file_path in file_crawler.crawl(parent_dir):
        file_path = str(file_path)
        if 'data' not in file_path and 'threshold' not in file_path:
            parts = pathlib.Path(file_path).parts
            trimmed_path = os.path.join(*parts[3:])
            output_path = os.path.join(out_dir, trimmed_path)
            file_linker.link(file_path, output_path)
