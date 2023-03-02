#!/usr/bin/env python3
import json
import logging
import os
from datetime import datetime
from pathlib import Path
import unittest
from typing import List, Dict

from data_access.tests.database_test import DatabaseBackedTest
import readme_generator.generator_main as readme_generator_main
from readme_generator.file_descriptions import remove_prefix
from readme_generator.log_entry import LogEntry
from readme_generator.data_product import DataProduct
from readme_generator.generator import generate_readme, Paths, get_time_span, DataFunctions

log = logging.getLogger()


def to_datetime(date: str) -> datetime:
    return datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')


class ReadmeGeneratorTest(DatabaseBackedTest):

    def setUp(self):
        self.setUpPyfakefs()
        self.in_path = Path('/in/CPER/2020/01/02')
        self.out_path = Path('/out')
        self.fs.create_dir(self.in_path)
        self.fs.create_dir(self.out_path)
        self.add_template_file()
        self.add_data_files()

    def add_template_file(self) -> None:
        template_real_path = Path(os.path.dirname(__file__), 'template.j2')
        self.template_path = Path('/templates/template.j2')
        self.fs.add_real_file(template_real_path, target_path=self.template_path)

    def add_data_files(self) -> None:
        root_path = Path(os.path.dirname(__file__), 'test_files')

        manifest_real_path = Path(root_path, 'manifest.csv')
        manifest_target_path = Path(self.in_path, 'manifest.csv')
        self.fs.add_real_file(manifest_real_path, target_path=manifest_target_path)

        data_1 = 'NEON.D10.CPER.DP1.00041.001.002.506.001.ST_1_minute.2020-01-02.basic.csv'
        data_1_real_path = Path(root_path, data_1)
        self.data_1_target_path = Path(self.in_path, data_1)
        self.fs.add_real_file(data_1_real_path, target_path=self.data_1_target_path)

        data_2 = 'NEON.D10.CPER.DP1.00041.001.002.506.001.ST_1_minute.2020-01-02.expanded.csv'
        data_2_real_path = Path(root_path, data_2)
        data_2_target_path = Path(self.in_path, data_2)
        self.fs.add_real_file(data_2_real_path, target_path=data_2_target_path)

        data_3 = 'NEON.D10.CPER.DP1.00041.001.002.506.030.ST_30_minute.2020-01-02.basic.csv'
        data_3_real_path = Path(root_path, data_3)
        data_3_target_path = Path(self.in_path, data_3)
        self.fs.add_real_file(data_3_real_path, target_path=data_3_target_path)

        data_4 = 'NEON.D10.CPER.DP1.00041.001.002.506.030.ST_30_minute.2020-01-02.expanded.csv'
        data_4_real_path = Path(root_path, data_4)
        data_4_target_path = Path(self.in_path, data_4)
        self.fs.add_real_file(data_4_real_path, target_path=data_4_target_path)

    def test_get_min_max_times(self):
        start_time, end_time = get_time_span(self.data_1_target_path)
        assert start_time == '2020-01-02T00:00:00Z'
        assert end_time == '2020-01-03T00:00:00Z'

    def test_generate_readme(self):

        def get_log_entries(dp_idq: str) -> List[LogEntry]:
            """Mock function for getting the change log entries."""
            real_path = Path(os.path.dirname(__file__), 'dp_change_log_data.json')
            target_path = Path('/dp_change_log_data.json')
            self.fs.add_real_file(real_path, target_path=target_path)
            log_entries = []
            with open(target_path) as file:
                json_data = json.load(file)
                for entry in json_data:
                    change_log_id: str = entry['dp_change_log_id']
                    # dp_idq: str = entry['dp_idq']
                    issue_date: str = entry['issue_date']
                    resolved_date: str = entry['resolved_date']
                    date_range_start: str = entry['date_range_start']
                    date_range_end: str = entry['date_range_end']
                    location_affected: str = entry['location_affected']
                    issue: str = entry['issue']
                    resolution: str = entry['resolution']
                    log_entries.append(LogEntry(change_log_id=int(change_log_id),
                                                dp_idq=dp_idq,
                                                issue_date=to_datetime(issue_date),
                                                resolution_date=to_datetime(resolved_date),
                                                date_range_start=to_datetime(date_range_start),
                                                date_range_end=to_datetime(date_range_end),
                                                location_affected=location_affected,
                                                issue=issue,
                                                resolution=resolution))
            return log_entries

        def get_data_product(dp_idq: str) -> DataProduct:
            """Mock function for getting the data product."""
            log.debug(f'dp_idq: {dp_idq}')
            real_path = Path(os.path.dirname(__file__), 'dp_catalog_data.json')
            target_path = Path('/dp_catalog_data.json')
            self.fs.add_real_file(real_path, target_path=target_path)
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
            return DataProduct(idq=dp_idq,
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

        def get_geometry(location_name: str) -> str:
            """Mock function for getting the site geometry."""
            log.debug(f'location_name: {location_name}')
            return 'POINT Z (-104.745591 40.815536 1653.9151)'

        def get_descriptions() -> Dict[str, str]:
            """Mock function for getting the file descriptions."""
            real_path = Path(os.path.dirname(__file__), 'pub_table_def.json')
            target_path = Path('/pub_table_def.json')
            self.fs.add_real_file(real_path, target_path=target_path)
            file_descriptions = {}
            with open(target_path) as file:
                json_data = json.load(file)
                for data in json_data:
                    idq: str = remove_prefix(data['dp_idq'])
                    description: str = data['description']
                    file_descriptions[idq] = description
            return file_descriptions

        def get_keywords(dp_idq: str):
            """Mock function to get a data product's keywords."""
            log.debug(f'dp_idq: {dp_idq}')
            return ['soil temperature', 'profile', 'soil']

        paths = Paths(in_path=self.in_path, out_path=self.out_path, template_path=self.template_path)
        functions = DataFunctions(get_log_entries=get_log_entries,
                                  get_data_product=get_data_product,
                                  get_geometry=get_geometry,
                                  get_descriptions=get_descriptions,
                                  get_keywords=get_keywords)

        generate_readme(paths=paths, functions=functions)

        readme_files = list(Path(self.out_path, 'CPER', '2020', '01').glob('*.txt'))
        csv_files = list(Path(self.out_path, 'CPER', '2020', '01', '02').glob('*.csv'))
        assert len(readme_files) == 1
        assert len(csv_files) == 5

    @unittest.skip('Integration test skipped.')
    def test_main(self):
        self.configure_mount()
        os.environ['IN_PATH'] = str(self.in_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['TEMPLATE_PATH'] = str(self.template_path)
        readme_generator_main.main()
