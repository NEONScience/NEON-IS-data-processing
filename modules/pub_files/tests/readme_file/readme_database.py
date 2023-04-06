"""Functions to read data from files instead of a database."""
import json
import os
from functools import partial
from pathlib import Path
from typing import List

from pyfakefs.fake_filesystem import FakeFilesystem

from pub_files.data_product import DataProduct
from pub_files.database.queries.geolocation_geometry import Geometry
from pub_files.database.queries.log_entries import LogEntry
from pub_files.output_files.readme.readme_file import ReadmeDatabase
from pub_files.tests.file_date_converter import to_datetime


def get_database(fs: FakeFilesystem) -> ReadmeDatabase:
    get_log_entries_partial = partial(get_log_entries, fs)
    return ReadmeDatabase(get_geometry=get_geometry,
                          get_keywords=get_keywords,
                          get_log_entries=get_log_entries_partial)


def root():
    return Path(os.path.dirname(__file__))


def get_geometry(_location_name: str) -> Geometry:
    return Geometry(geometry='POINT Z (-104.745591 40.815536 1653.9151)', srid=4979)


def get_keywords(_data_product_id: str):
    return ['soil temperature', 'profile', 'soil']


def get_log_entries(fs: FakeFilesystem, _data_product_id: str) -> List[LogEntry]:
    path = Path(root(), 'change_log.json')
    target_path = Path('/change_log.json')
    fs.add_real_file(path, target_path=target_path)
    log_entries = []
    with open(target_path) as file:
        json_data = json.load(file)
        for entry in json_data:
            change_log_id: str = entry['dp_change_log_id']
            data_product_id: str = entry['dp_idq']
            issue_date: str = entry['issue_date']
            resolved_date: str = entry['resolved_date']
            date_range_start: str = entry['date_range_start']
            date_range_end: str = entry['date_range_end']
            location_affected: str = entry['location_affected']
            issue: str = entry['issue']
            resolution: str = entry['resolution']
            log_entries.append(LogEntry(change_log_id=int(change_log_id),
                                        data_product_id=data_product_id,
                                        issue_date=to_datetime(issue_date),
                                        resolution_date=to_datetime(resolved_date),
                                        date_range_start=to_datetime(date_range_start),
                                        date_range_end=to_datetime(date_range_end),
                                        location_affected=location_affected,
                                        issue=issue,
                                        resolution=resolution))
    return log_entries


def get_data_product(fs: FakeFilesystem, _data_product_id: str) -> DataProduct:
    path = Path(root(), 'data_product.json')
    target_path = Path('/data_product.json')
    fs.add_real_file(path, target_path=target_path)
    with open(target_path) as file:
        json_data = json.load(file)
        data = json_data[0]
        data_product_id: str = data['dp_idq']
        dp_name: str = data['dp_name']
        dp_description: str = data['dp_desc']
        category: str = data['category']
        supplier: str = data['supplier']
        dp_shortname: str = data['dp_shortname']
        dp_abstract: str = data['dp_abstract']
        design_description: str = data['design_desc']
        study_description: str = data['study_desc']
        sensor: str = data['sensor']
        basic_description: str = data['basic_desc']
        expanded_desc: str = data['expanded_desc']
        remarks: str = data['remarks']
    return DataProduct(data_product_id=data_product_id,
                       name=dp_name,
                       type_name='TIS Data Product Type',
                       description=dp_description,
                       category=category,
                       supplier=supplier,
                       short_name=dp_shortname,
                       abstract=dp_abstract,
                       design_description=design_description,
                       study_description=study_description,
                       sensor=sensor,
                       basic_description=basic_description,
                       expanded_description=expanded_desc,
                       remarks=remarks)
