#!/usr/bin/env python3
import os
from pathlib import Path
from typing import List,NamedTuple

import unittest
import geojson
import json

from data_access.tests.database_test import DatabaseBackedTest
from common.date_formatter import to_datetime

from pub_workbook_loader import pub_workbook_loader_main,pub_workbook_loader
from data_access.get_pub_workbook import get_pub_workbook
from data_access.types.pub_workbook import PubWorkbookRow
from data_access.types.pub_workbook import PubWorkbook


class PubWorkbookLoaderTest(DatabaseBackedTest):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('out/')
        self.fs.create_dir(self.out_path)

    @unittest.skip('Integration test skipped due to long process time.')
    def test_main(self):
        self.configure_mount()
        pub_workbook_loader_main.main()
        file_path = Path('out/')
        self.assertTrue(os.path.exists(file_path))

    def test_pub_workbook_loader(self):
        workbook_rows_returned: List[PubWorkbookRow] = []
        dp_ids = ["NEON.DOM.SITE.DP1.20015.001","NEON.DOM.SITE.DP1.00066.001"]
        dp_id = "NEON.DOM.SITE.DP1.00066.001"
        out_path = self.out_path
        out_path.mkdir(parents=True,exist_ok=True)

        # self.assertTrue(file_path.exists())

        def get_pub_workbook(data_product_id=dp_id) -> List[PubWorkbookRow]:
            # """Mock function """
            workbook_rows: List[PubWorkbookRow] = []
            for dp_id in dp_ids:
                print('dp_id in for loop:  ',dp_id)
                workbook_rows.append(PubWorkbookRow(rank=1,
                                                    DPName='dp_name',
                                                    dpID=dp_id,
                                                    dpIDSource='NEON.DOM.SITE.DP1.00026.001',
                                                    DPNumber='',
                                                    table='ais_maintenanceGroundwater_pub',
                                                    tableDescription='table_desc',
                                                    fieldName='field_name',
                                                    description='field_description',
                                                    dataType='data_type_code',
                                                    units='unit_name',
                                                    measurementScale='meas_scale',
                                                    ontologyMapping='ontology_mapping',
                                                    pubFormat='pub_format',
                                                    exampleEntry='example_entry',
                                                    usage='usage',
                                                    fieldType='field_type',
                                                    tableType='table_type',
                                                    inputs='inputs',
                                                    filterSampleClass='filter_sample_class',
                                                    timeIndex='time_index',
                                                    timeDescription='time_description',
                                                    downloadPkg='download_package',
                                                    dataCategory='data_category',
                                                    sampleInfo='sample_info',
                                                    lovName='lov_code',
                                                    primaryKey='primary_key',
                                                    redactionFlag='redaction_flag'))
                workbook_rows.append(workbook_rows)
            return PubWorkbook(workbook_rows)

        # test the function
        workbook_rows_returned = pub_workbook_loader.load_pub_workbook(out_path=self.out_path,
                                                                       get_pub_workbook=get_pub_workbook,dp_ids=dp_ids)

        # check the output
        for dp_id in dp_ids:
            file_name = 'publication_workbook_' + dp_id + '.txt'
            file_path = 'out/' + file_name
            self.assertTrue(os.path.exists(file_path))


if __name__ == '__main__':
    unittest.main()
