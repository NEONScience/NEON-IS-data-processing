import os
import pathlib

import structlog

import lib.file_linker as file_linker
import lib.file_crawler as file_crawler
from lib.data_filename import DataFilename

log = structlog.get_logger()


def group(data_path, calibration_path, out_path):
    """
    Group data and calibration paths into the output path.
    :param data_path: The data path.
    :param calibration_path: The calibration path.
    :param out_path: The path for writing output.
    :return:
    """
    for file_path in file_crawler.crawl(data_path):
        log.debug(f'data file path: {file_path}')
        filename = file_path.name
        parts = file_path.parts
        source_type = parts[3]
        year = parts[4]
        month = parts[5]
        day = parts[6]
        source_id = DataFilename(filename).source_id()
        log.debug(f'type: {source_type} Y: {year} M: {month} D: {day} id: {source_id} file: {filename}')
        common_path = pathlib.Path(out_path, source_type, year, month, day, source_id)
        link_data(common_path, file_path, filename)
        link_calibrations(calibration_path, common_path)


def link_calibrations(calibration_path, common_path):
    """
    Find calibrations for the source ID.
    :param calibration_path: The calibration path.
    :param common_path: The common output directory path for file grouping.
    :return:
    """
    for file_path in file_crawler.crawl(calibration_path):
        log.debug(f'calibration file path: {file_path}')
        parts = file_path.parts
        source_type = parts[3]
        source_id = parts[4]
        stream = parts[5]
        log.debug(f'calibration type: {source_type}, id: {source_id}, stream: {stream}')
        calibration_path = pathlib.Path(common_path, 'calibration', stream)
        if not calibration_path.exists():
            os.makedirs(calibration_path)
        target_path = pathlib.Path(calibration_path, file_path.name)
        file_linker.link(file_path, target_path)


def link_data(common_path, file_path, filename):
    """
    Link the data file into the common directory.
    :param common_path: Common path for writing grouped files.
    :param file_path: A data file path.
    :param filename: A data file name.
    :return:
    """
    data_path = pathlib.Path(common_path, 'data')
    if not data_path.exists:
        os.makedirs(data_path)
    target_path = pathlib.Path(data_path, filename)
    file_linker.link(file_path, target_path)
