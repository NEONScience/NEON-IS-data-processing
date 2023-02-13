#!/usr/bin/env python3
import os
import pandas as pd
from sortedcontainers import SortedSet
import datetime
import hashlib
import csv

from structlog import get_logger

log = get_logger()


def package(*, data_path, out_path, prefix_index: int, prefix_length: int, sort_index: int) -> None:
    """
    Bundles the required files into a package suitable for publication. Note the prefix_index
    typically corresponds to the glob pattern in the pipeline specification file, e.g. CPER/2019/01.)

    :param data_path: The input data path.
    :param out_path: The output path for writing the package files.
    :param prefix_index: start index of prefix
    :param prefix_length: number of indices in prefix
    :param sort_index: index of filename field to sort on (e.g. the day)
    """
    package_files = {}  # the set of files to be collated into each package file
    has_data_by_file = {}  # has_data by package file
    package_path_by_file = {}  # package path by package file

    # processing timestamp
    timestamp = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')

    # get the package prefix
    (prefix, prefix_field) = get_package_prefix(data_path, prefix_index, prefix_length)

    for root, dirs, files in os.walk(data_path):
        for file in files:
            # if this is the manifest, parse it
            if 'manifest' in file:
                parse_manifest(os.path.join(root, file), has_data_by_file, sort_index, prefix_field, timestamp)
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
        package_path_by_file[package_file] = output_file
        os.makedirs(os.path.join(out_path, prefix), exist_ok=True)
        is_first_file = True
        for file in package_files[package_file]:
            data = pd.read_csv(file)
            mode = 'a'
            write_header = False
            if is_first_file:
                mode = 'w'
                write_header = True
                is_first_file = False
            data.to_csv(output_file, mode=mode, header=write_header, index=False)

    write_manifest(out_path, prefix, has_data_by_file, package_path_by_file)


def get_package_filename(file, sort_index, prefix_field, timestamp):
    filename_fields = file.split('.')[:-1]
    filename_fields[sort_index] = prefix_field
    filename_fields.extend([timestamp, 'csv'])
    package_file = '.'.join(filename_fields)
    return package_file


def get_package_prefix(data_path, prefix_index, prefix_length):
    data_path_parts = data_path.parts
    prefix = os.path.join(*data_path_parts[prefix_index: prefix_index + prefix_length])
    prefix_field = '-'.join(data_path_parts[prefix_index + 1: prefix_index + prefix_length])
    return prefix, prefix_field


def write_manifest(out_path, prefix, has_data_by_file, package_path_by_file):
    manifest_filepath = os.path.join(out_path, prefix, 'manifest.csv')
    with open(manifest_filepath, 'w') as manifest_csv:
        writer = csv.writer(manifest_csv)
        writer.writerow(['file', 'hasData', 'size', 'checksum'])
        for key in has_data_by_file.keys():
            # get file size and checksum
            package_path = package_path_by_file[key]
            file_size = os.stat(package_path).st_size
            md5_hash = hashlib.md5()
            with open(package_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    md5_hash.update(byte_block)
            checksum = md5_hash.hexdigest()
            writer.writerow([key, has_data_by_file[key], file_size, checksum])


def parse_manifest(manifest_file, has_data_by_file, sort_index, prefix_field, timestamp):
    manifest = pd.read_csv(manifest_file, header=0, squeeze=True, index_col=0).to_dict()
    for key in manifest.keys():
        package_file = get_package_filename(key, sort_index, prefix_field, timestamp)
        if package_file in has_data_by_file.keys():
            has_data_by_file[package_file] = has_data_by_file[package_file] | manifest[key]
        else:
            has_data_by_file[package_file] = manifest[key]
