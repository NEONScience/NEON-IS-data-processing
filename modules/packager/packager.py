#!/usr/bin/env python3
import os
import pandas as pd
import environs
from sortedcontainers import SortedSet
import datetime
import hashlib
import csv

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

    # hasData by package file
    hasDataByFile = {}

    # package path by package file
    packagePathByFile = {}

    # processing timestamp
    timestamp = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')

    for root, dirs, files in os.walk(data_path):
        for file in files:
            # if this is the manifest, parse it
            if 'manifest' in file:
                parse_manifest(os.path.join(root, file), hasDataByFile, sort_index, prefix_field, timestamp)
                continue
            # get the package filename
            package_file = get_package_filename(file, sort_index, prefix_field, timestamp)
            file_path = os.path.join(root, file)
            if package_file in package_files.keys():
                package_files[package_file].add(file_path)
            else:
                package_files[package_file] = SortedSet({file_path})

    for package_file in package_files.keys():
        output_file = os.path.join(out_path, prefix, package_file)
        packagePathByFile[package_file] = output_file
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

    # write manifest
    manifest_filepath = os.path.join(out_path, prefix, 'manifest.csv')
    with open(manifest_filepath, 'w') as manifest_csv:
        writer = csv.writer(manifest_csv)
        writer.writerow(['file','hasData','size','checksum'])
        for key in hasDataByFile.keys():
            # get file size and checksum
            package_path = packagePathByFile[key]
            file_size = os.stat(package_path).st_size
            md5_hash = hashlib.md5()
            with open(package_path,"rb") as f:
                for byte_block in iter(lambda: f.read(4096),b""):
                    md5_hash.update(byte_block)
            checksum = md5_hash.hexdigest()
            writer.writerow([key, hasDataByFile[key], file_size, checksum])


def get_package_filename(file, sort_index, prefix_field, timestamp):
    filename_fields = file.split('.')[:-1]
    filename_fields[sort_index] = prefix_field
    filename_fields.extend([timestamp, 'csv'])
    package_file = '.'.join(filename_fields)
    return package_file


def parse_manifest(manifest_file, hasDataByFile, sort_index, prefix_field, timestamp):
    manifest = pd.read_csv(manifest_file, header=0, squeeze=True, index_col=0).to_dict()
    for key in manifest.keys():
        package_file = get_package_filename(key, sort_index, prefix_field, timestamp)
        if package_file in hasDataByFile.keys():
            hasDataByFile[package_file] = hasDataByFile[package_file] | manifest[key]
        else:
            hasDataByFile[package_file] = manifest[key]