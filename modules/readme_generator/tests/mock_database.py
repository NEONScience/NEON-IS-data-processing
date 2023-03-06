"""
Module containing mock functions for reading data.
"""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict

from readme_generator.data_product import DataProduct
from readme_generator.file_descriptions import remove_prefix
from readme_generator.generator import DataFunctions
from readme_generator.log_entry import LogEntry


def to_datetime(date: str) -> datetime:
    """Convert file dates to datetime objects."""
    return datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')


class MockDatabase:
    """Class to read data from files."""

    def __init__(self, fs):
        self.fs = fs

    def get_log_entries(self, _dp_idq: str) -> List[LogEntry]:
        """Mock function for getting the change log entries."""
        path = Path(os.path.dirname(__file__), 'generator_test_files/dp_change_log.json')
        target_path = Path('/dp_change_log.json')
        self.fs.add_real_file(path, target_path=target_path)
        log_entries = []
        with open(target_path) as file:
            json_data = json.load(file)
            for entry in json_data:
                change_log_id: str = entry['dp_change_log_id']
                dp_idq: str = entry['dp_idq']
                issue_date: str = entry['issue_date']
                resolved_date: str = entry['resolved_date']
                date_range_start: str = entry['date_range_start']
                date_range_end: str = entry['date_range_end']
                location_affected: str = entry['location_affected']
                issue: str = entry['issue']
                resolution: str = entry['resolution']
                log_entries.append(
                    LogEntry(
                        change_log_id=int(change_log_id),
                        dp_idq=dp_idq,
                        issue_date=to_datetime(issue_date),
                        resolution_date=to_datetime(resolved_date),
                        date_range_start=to_datetime(date_range_start),
                        date_range_end=to_datetime(date_range_end),
                        location_affected=location_affected,
                        issue=issue,
                        resolution=resolution
                    )
                )
        return log_entries

    def get_data_product(self, _dp_idq: str) -> DataProduct:
        """Mock function for getting the data product."""
        path = Path(os.path.dirname(__file__), 'generator_test_files/dp_catalog.json')
        target_path = Path('/dp_catalog_data.json')
        self.fs.add_real_file(path, target_path=target_path)
        with open(target_path) as file:
            json_data = json.load(file)
            data = json_data[0]
            dp_idq: str = data['dp_idq']
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
        return DataProduct(
            idq=dp_idq,
            short_idq=dp_idq.replace('NEON.DOM.SITE.', ''),
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

    def get_descriptions(self) -> Dict[str, str]:
        """Mock function for getting the file descriptions."""
        path = Path(os.path.dirname(__file__), 'generator_test_files/pub_table_def.json')
        target_path = Path('/pub_table_def.json')
        self.fs.add_real_file(path, target_path=target_path)
        file_descriptions = {}
        with open(target_path) as file:
            json_data = json.load(file)
            for data in json_data:
                idq: str = remove_prefix(data['dp_idq'])
                description: str = data['description']
                file_descriptions[idq] = description
        return file_descriptions

    @staticmethod
    def get_geometry(_location_name: str) -> str:
        """Mock function for getting the site geometry."""
        return 'POINT Z (-104.745591 40.815536 1653.9151)'

    @staticmethod
    def get_keywords(_dp_idq: str):
        """Mock function to get a data product's keywords."""
        return ['soil temperature', 'profile', 'soil']

    def get_data_functions(self):
        return DataFunctions(
            get_log_entries=self.get_log_entries,
            get_data_product=self.get_data_product,
            get_geometry=self.get_geometry,
            get_descriptions=self.get_descriptions,
            get_keywords=self.get_keywords)
