#!/usr/bin/env python3
import os
import pathlib

import structlog

import lib.file_linker as file_linker
import lib.file_crawler as file_crawler
from lib.data_filename import DataFilename

log = structlog.get_logger()


def group(data_path, calibration_path, out_path):
    """
    Group data and calibration files into the output path.

    :param data_path: The data path.
    :type data_path: str
    :param calibration_path: The calibration path.
    :type calibration_path: str
    :param out_path: The path for writing output.
    :type out_path: str
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

        # log.debug(f'type: {source_type} Y: {year} M: {month} D: {day} id: {source_id} file: {filename}')
        common_path = pathlib.Path(out_path, source_type, year, month, day, source_id)
        link_data(common_path, file_path, filename)
        link_calibrations(calibration_path, common_path, source_id)


def link_calibrations(calibration_path, common_path, source_id):
    """
    Find calibrations for the source ID. If no files are found create an empty directory.

    :param calibration_path: The calibration path.
    :type calibration_path: str
    :param common_path: The common output directory path for file grouping.
    :type common_path: str
    :param source_id: The source ID of the data file to match on.
    :type source_id: str
    :return:
    """
    for file_path in file_crawler.crawl(calibration_path):
        log.debug(f'calibration file path: {file_path}')
        parts = file_path.parts
        if len(parts) < 5:
            calibration_path = pathlib.Path(common_path, 'calibration')
            if not calibration_path.exists():
                # create an empty calibration directory
                os.makedirs(calibration_path)
        else:
            source_type = parts[3]
            calibration_source_id = parts[4]
            stream = parts[5]
            log.debug(f'calibration type: {source_type}, id: {source_id}, stream: {stream}')
            if calibration_source_id == source_id:
                calibration_dir = pathlib.Path(common_path, 'calibration', stream)
                if not calibration_dir.exists():
                    os.makedirs(calibration_dir)
                target_path = pathlib.Path(calibration_dir, file_path.name)
                file_linker.link(file_path, target_path)


def link_data(common_path, file_path, filename):
    """
    Link the data file into the common directory.

    :param common_path: Common output path for writing files.
    :type common_path: str
    :param file_path: A data file path.
    :type file_path: str
    :param filename: A data file name.
    :type filename: str
    :return:
    """
    data_path = pathlib.Path(common_path, 'data')
    if not data_path.exists:
        os.makedirs(data_path)
    target_path = pathlib.Path(data_path, filename)
    file_linker.link(file_path, target_path)
