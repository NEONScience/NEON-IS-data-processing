#!/usr/bin/env python3
import os
import sys
import pandas as pd
from sortedcontainers import SortedSet
import datetime
import hashlib
import csv
from pathlib import Path
from typing import List,Iterator,Tuple

from structlog import get_logger
from common.err_datum import err_datum_path

log = get_logger()


def pub_package(*, data_path, out_path, err_path, product_index: int, publoc_index: int, date_index: int, date_index_length: int, sort_index: int) -> None:
    """
    Bundles the required files into a package suitable for publication.

    :param data_path: The input data path. Should end at whatever aggregation interval is a publication package (i.e. monthly).
    :param err_path: The error directory, i.e., errored.
    :param out_path: The output path for writing the package files.
    :param product_index: input path index of the data product identifier
    :param publoc_index: input path index of the pub package location (typically the site)
    :param date_index: start input path index of publication date field (e.g. index of the year in the path)
    :param date_index_length: number of input path indices forming the pub date field. e.g. for monthly pub, this will be 2 (year-month)
    :param sort_index: index of filename field to sort on (e.g. the day)
    """

    # Each PUBLOC at the glob level is a datum (e.g. /product/year/month/*/PUBLOC). Get all the PUBLOCS, assuming
    # there is a manifest.csv embedded directly under each PUBLOC directory
    publocs = set()
    # DirErrBase: the user specified error directory, i.e., /tmp/out/errored
    DirErrBase = Path(err_path)
    publoc_date = 0
    for path in data_path.rglob('manifest.csv'):
        parts = path.parts
        publoc = parts[publoc_index]
        publoc_date = parts[publoc_index-1]
        publocs.add(publoc)

    for publoc in publocs:
        log.debug(f'Processing datum {data_path} and {publoc}')

        package_files = {}  # the set of files to be collated into each package file
        has_data_by_file = {}  # has_data by package file
        package_path_by_file = {}  # package path by package file
        visibility_by_file = {} # visibility by package file

        # processing timestamp
        timestamp = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        dataDir_routed = Path(data_path, publoc_date, publoc)
        # get the package path prefix and date field
        for path in data_path.rglob(publoc+'/*'):
            if path.is_file():
                # Get one full path
                break
        (path_prefix, date_field) = get_package_prefix(path, product_index, publoc_index, date_index, date_index_length)

        for path in data_path.rglob(publoc+'/*'):
            try:
                if path.is_file():
                    file = os.path.basename(path)
                    log.debug(f'{file}')
                    if 'manifest' in file:
                        parse_manifest(path, has_data_by_file, visibility_by_file, sort_index, date_field, timestamp)
                        continue
                    # get the package filename
                    package_file = get_package_filename(file, sort_index, date_field, timestamp)
                    if package_file in package_files.keys():
                        package_files[package_file].add(path)
                    else:
                        package_files[package_file] = SortedSet({path})
            except:
                log.debug('.... Errored executing parse_manifest or getting the package filename  ...')
                err_msg = sys.exc_info()
                err_datum_path(err=err_msg, DirDatm=str(dataDir_routed), DirErrBase=DirErrBase,
                           RmvDatmOut=True, DirOutBase=out_path)

        for package_file in package_files.keys():
            output_file = os.path.join(out_path, path_prefix, package_file)
            package_path_by_file[package_file] = output_file
            os.makedirs(os.path.join(out_path, path_prefix), exist_ok=True)
            is_first_file = True
            for file in package_files[package_file]:
                try:
                    data = pd.read_csv(file)
                    mode = 'a'
                    write_header = False
                    if is_first_file:
                        mode = 'w'
                        write_header = True
                        is_first_file = False
                    data.to_csv(output_file, mode=mode, header=write_header, index=False)
                    log.debug(f'Wrote data file {output_file}')
                except:
                    log.debug('.... Errored writing output_file ...')
                    err_msg = sys.exc_info()
                    err_datum_path(err=err_msg, DirDatm=str(dataDir_routed), DirErrBase=DirErrBase,
                           RmvDatmOut=True, DirOutBase=out_path)
        try:
            write_manifest(out_path,path_prefix,has_data_by_file,visibility_by_file,package_path_by_file)
        except:
            log.debug('.... Errored executing write_manifest ...')
            err_msg = sys.exc_info()
            err_datum_path(err=err_msg, DirDatm=str(dataDir_routed), DirErrBase=DirErrBase,
                           RmvDatmOut=True, DirOutBase=out_path)


def get_package_filename(file, sort_index, date_field, timestamp):
    filename_fields = file.split('.')[:-1]
    filename_fields[sort_index] = date_field
    filename_fields.extend([timestamp, 'csv'])
    package_file = '.'.join(filename_fields)
    return package_file


def get_package_prefix(data_path, product_index, publoc_index, date_index, date_index_length):
    data_path_parts = data_path.parts
    path_prefix = os.path.join(data_path_parts[product_index],data_path_parts[publoc_index],*data_path_parts[date_index: date_index + date_index_length])
    date_field = '-'.join(data_path_parts[date_index: date_index + date_index_length])
    return path_prefix, date_field


def write_manifest(out_path, path_prefix, has_data_by_file, visibility_by_file, package_path_by_file):
    manifest_filepath = os.path.join(out_path, path_prefix, 'manifest.csv')
    with open(manifest_filepath, 'w') as manifest_csv:
        writer = csv.writer(manifest_csv)
        writer.writerow(['file', 'hasData', 'visibility', 'size', 'checksum'])
        for key in has_data_by_file.keys():
            # get file size and checksum
            package_path = package_path_by_file[key]
            file_size = os.stat(package_path).st_size
            md5_hash = hashlib.md5()
            with open(package_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    md5_hash.update(byte_block)
            checksum = md5_hash.hexdigest()
            writer.writerow([key, has_data_by_file[key], visibility_by_file[key], file_size, checksum])
    log.debug(f'Wrote manifest {manifest_filepath}')


def parse_manifest(manifest_file, has_data_by_file, visibility_by_file, sort_index, date_field, timestamp):
    manifest_hasData = pd.read_csv(manifest_file, header=0, index_col=0,usecols=['file','hasData'])
    manifest_hasData = manifest_hasData.squeeze("columns").to_dict()
    manifest_visibility = pd.read_csv(manifest_file, header=0, index_col=0,usecols=['file','visibility'])
    manifest_visibility = manifest_visibility.squeeze("columns").to_dict()
    for key in manifest_hasData.keys():
        package_file = get_package_filename(key, sort_index, date_field, timestamp)
        if package_file in has_data_by_file.keys():
            has_data_by_file[package_file] = has_data_by_file[package_file] | manifest_hasData[key]
            visibility_by_file[package_file] = visibility_by_file[package_file]
        else:
            has_data_by_file[package_file] = manifest_hasData[key]
            visibility_by_file[package_file] = manifest_visibility[key]
