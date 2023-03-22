"""Functions to read data from files instead of a database."""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

from pyfakefs.fake_filesystem import FakeFilesystem

from publication_files.database_queries.data_product import DataProduct, remove_prefix
from publication_files.database_queries.log_entries import LogEntry


def get_geometry(_location_name: str) -> str:
    """Mock function for reading the site geometry."""
    return 'POINT Z (-104.745591 40.815536 1653.9151)'


def get_keywords(_data_product_id: str):
    """Mock function to get a data product's keywords."""
    return ['soil temperature', 'profile', 'soil']


def root():
    return Path(os.path.dirname(__file__), 'readme_test_files')


def get_log_entries(fs: FakeFilesystem, _data_product_id: str) -> List[LogEntry]:
    """Mock function to read the change log entries."""
    path = Path(root(), 'dp_change_log.json')
    target_path = Path('/dp_change_log.json')
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
    """Mock function for reading the data product."""
    path = Path(root(), 'dp_catalog.json')
    target_path = Path('/dp_catalog_data.json')
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
                       short_data_product_id=remove_prefix(data_product_id),
                       name=dp_name,
                       type_name='TIS Data Product Type',
                       description=dp_description,
                       category=category,
                       supplier=supplier,
                       supplier_full_name='Terrestrial Instrument System',
                       short_name=dp_shortname,
                       abstract=dp_abstract,
                       design_description=design_description,
                       study_description=study_description,
                       sensor=sensor,
                       basic_description=basic_description,
                       expanded_description=expanded_desc,
                       remarks=remarks)


def get_descriptions(fs: FakeFilesystem) -> Dict[str, str]:
    """Mock function for reading the file descriptions."""
    path = Path(root(), 'pub_table_def.json')
    target_path = Path('/pub_table_def.json')
    fs.add_real_file(path, target_path=target_path)
    file_descriptions = {}
    with open(target_path) as file:
        json_data = json.load(file)
        for data in json_data:
            data_product_id: str = data['dp_idq']
            description: str = data['description']
            file_descriptions[data_product_id] = description
    return file_descriptions


def to_datetime(date: str) -> Optional[datetime]:
    """Convert the formatted dates in files to datetime objects."""
    if date is not None:
        return datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
    else:
        return None
