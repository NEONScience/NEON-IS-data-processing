#!/usr/bin/env python3
import os
import unittest
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import array_parser.array_parser_main as array_parser_main
import array_parser.calibration_file_parser as calibration_file_parser
import array_parser.schema_parser as schema_parser
from array_parser.schema_parser import TermMapping


class ArrayParserTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()

        self.out_path = Path('/out')
        self.data_path = Path('/in/data')
        self.calibration_path = Path('/in/calibration')
        self.metadata_path = Path('tchain/2019/01/12/32610')

        # schema
        actual_path = Path(os.path.dirname(__file__), 'tchain_parsed.avsc')
        target_path = Path(self.data_path, 'tchain_parsed.avsc')
        self.fs.add_real_file(actual_path, target_path=target_path)

        # data
        actual_path = Path(os.path.dirname(__file__), 'tchain_32610_2019-01-12.parquet')
        target_path = Path(self.calibration_path, self.metadata_path, 'tchain_32610_2019-01-12.parquet')
        self.fs.add_real_file(actual_path, target_path=target_path)

        # calibrations
        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87280.xml')
        target_path = Path(self.calibration_path, self.metadata_path, '30000000016555_WO12477_87280.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87330.xml')
        target_path = Path(self.calibration_path, self.metadata_path, '30000000016555_WO12477_87330.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87331.xml')
        target_path = Path(self.calibration_path, self.metadata_path, '30000000016555_WO12477_87331.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87332.xml')
        target_path = Path(self.calibration_path, self.metadata_path, '30000000016555_WO12477_87332.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87333.xml')
        target_path = Path(self.calibration_path, self.metadata_path, '30000000016555_WO12477_87333.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87334.xml')
        target_path = Path(self.calibration_path, self.metadata_path, '30000000016555_WO12477_87334.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87335.xml')
        target_path = Path(self.calibration_path, self.metadata_path, '30000000016555_WO12477_87335.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87336.xml')
        target_path = Path(self.calibration_path, self.metadata_path, '30000000016555_WO12477_87336.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87337.xml')
        target_path = Path(self.calibration_path, self.metadata_path, '30000000016555_WO12477_87337.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87338.xml')
        target_path = Path(self.calibration_path, self.metadata_path, '30000000016555_WO12477_87338.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        self.source_type_index = 3
        self.year_index = 4
        self.month_index = 5
        self.day_index = 6
        self.source_id_index = 7
        self.data_type_index = 8

    def test_calibration_parser(self):
        target_path = Path(self.calibration_path, self.metadata_path, '30000000016555_WO12477_87280.xml')
        stream_id = calibration_file_parser.get_stream_id(target_path)
        assert stream_id == '0'

    def test_schema_parser(self):
        target_path = Path(self.data_path, 'tchain_parsed.avsc')
        term_mapping: TermMapping = schema_parser.parse_schema(target_path)
        assert term_mapping.source == 'tchain'
        term_name = term_mapping.mapping.get('0')
        assert term_name == 'depth0WaterTemp'
        term_name = term_mapping.mapping.get('10')
        assert term_name == 'depth10WaterTemp'

    @unittest.skip('')
    def test_main(self):
        os.environ['CALIBRATION_PATH'] = str(self.calibration_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = str(self.source_type_index)
        os.environ['YEAR_INDEX'] = str(self.year_index)
        os.environ['MONTH_INDEX'] = str(self.month_index)
        os.environ['DAY_INDEX'] = str(self.day_index)
        os.environ['SOURCE_ID_INDEX'] = str(self.source_id_index)
        os.environ['DATA_TYPE_INDEX'] = str(self.data_type_index)
        array_parser_main.main()
        self.check_output()

    def check_output(self):
        root_path = Path(self.out_path, self.metadata_path)
        data_path = Path(root_path, self.metadata_path, 'tchain_32610_2019-01-12.parquet')
        self.assertTrue(data_path.exists())
