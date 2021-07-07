#!/usr/bin/env python3
import os
import pandas as pd
import environs
from sortedcontainers import SortedSet
import datetime

from structlog import get_logger

log = get_logger()


def package(*, prefix_index: int, prefix_length: int, sort_index: int) -> None:
    """
    :param prefix_index: start index of prefix (the prefix typically corresponds to the glob, e.g. CPER/2019/01)
    :param prefix_length: number of indices in prefix
    :param sort_index: index of filename field to sort on (e.g. the day)
    """

    data_path = os.environ['DATA_PATH']
    out_path = os.environ['OUT_PATH']

    # get the package prefix
    dataPath = environs.Env().path('DATA_PATH')
    dataPath_parts = dataPath.parts
    prefix = os.path.join(*dataPath_parts[prefix_index:prefix_index+prefix_length])
    prefix_field = '-'.join(dataPath_parts[prefix_index+1:prefix_index+prefix_length])

    # store set of files to be collated into each package file
    package_files = {}

    # processing timestamp
    timestamp = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')

    for root, dirs, files in os.walk(data_path):
	    for file in files:
            # get the package filename
                filename_fields = file.split('.')[:-1]
                filename_fields[sort_index] = prefix_field
                package_file = '.'.join(filename_fields)
                file_path = os.path.join(root, file)
                if package_file in package_files.keys():
                    package_files[package_file].add(file_path)
                else:
                    package_files[package_file] = SortedSet({file_path})

    for package_file in package_files.keys():
        output_file = os.path.join(out_path, prefix, '.'.join([package_file, timestamp, 'csv']))
        os.makedirs(os.path.join(out_path, prefix), exist_ok=True)
        isFirstFile = True
        for file in package_files[package_file]:
            data = pd.read_csv(file)
            mode = 'a'
            writeHeader = False
            if isFirstFile:
                mode = 'w'
                writeHeader = True
                isFirstFile = False
            data.to_csv(output_file, mode=mode, header=writeHeader, index=False)
