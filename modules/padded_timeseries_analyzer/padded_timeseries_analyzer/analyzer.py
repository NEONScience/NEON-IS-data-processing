import os
import sys
import pathlib
import lib.file_linker as file_linker

from structlog import get_logger
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
                        for dataFile in os.listdir(root):
                            log.debug(f'dataFile: {dataFile}')
                            if dataFile != manifest_file:
                                data_file_date = MergedDataFilename(dataFile).date()
                                log.debug(f'checking data file date: {data_file_date} and '
                                          f'manifest date {date} in {dates_not_found}')
                                if date in data_file_date and date in dates_not_found:
                                    log.debug(f'found data for: {date}')
                                    dates_not_found.remove(date)
                    # if complete, symlink to output repo
                    if not dates_not_found:
                        for dataFile in os.listdir(root):
                            if dataFile != manifest_file:
                                source_path = os.path.join(root, dataFile)
                                dest_parts = pathlib.Path(source_path).parts
                                dest_parts = list(dest_parts)
                                for idx in range(1, len(out_dir_parts)):
                                    dest_parts[idx] = out_dir_parts[idx]
                                dest_path = os.path.join(*dest_parts)
                                log.debug(f'linking {source_path} to {dest_path}')
                                file_linker.link(source_path, dest_path)
                                write_thresholds(source_path, dest_path)
    except Exception:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        log.error("Exception at line " + str(exc_tb.tb_lineno) + ": " + str(sys.exc_info()))


def write_thresholds(source_path, destination_path):
    threshold_dir = 'threshold'
    threshold_file = 'thresholds.json'
    source_dir = pathlib.Path(source_path).parent.parent
    destination_dir = pathlib.Path(destination_path).parent.parent
    source = os.path.join(source_dir, threshold_dir, threshold_file)
    destination = os.path.join(destination_dir, threshold_dir, threshold_file)
    log.debug(f'linking {source} to {destination}')
    file_linker.link(source, destination)
