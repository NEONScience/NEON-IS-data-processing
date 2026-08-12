#!/usr/bin/env python3
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from pyfakefs.fake_filesystem_unittest import TestCase

import array_parser.calibration_file_parser as calibration_file_parser
import array_parser.schema_parser as schema_parser
import array_parser.array_parser as array_parser
import array_parser.array_parser_main as array_parser_main
from array_parser.schema_parser import SchemaData
from array_parser.array_parser_config import Config


class ArrayParserTest(TestCase):

    def setUp(self) -> None:
        self.setUpPyfakefs()

        self.out_path = Path('/out')
        self.in_path = Path('/in/repo')
        self.metadata_path = Path('tchain/2019/01/12/32610')
        self.calibration_metadata_path = Path(self.metadata_path, 'calibration')

        # schema
        actual_path = Path(os.path.dirname(__file__), 'tchain_parsed.avsc')
        self.schema_path = Path('/in/schema/tchain_parsed.avsc')
        self.fs.add_real_file(actual_path, target_path=self.schema_path)

        # data
        actual_path = Path(os.path.dirname(__file__), 'tchain_32610_2019-01-12.parquet')
        target_path = Path(self.in_path, self.metadata_path, 'data', 'tchain_32610_2019-01-12.parquet')
        self.fs.add_real_file(actual_path, target_path=target_path)

        # calibrations
        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87280.xml')
        target_path = Path(self.in_path, self.calibration_metadata_path, '30000000016555_WO12477_87280.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87330.xml')
        target_path = Path(self.in_path, self.calibration_metadata_path, '30000000016555_WO12477_87330.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87331.xml')
        target_path = Path(self.in_path, self.calibration_metadata_path, '30000000016555_WO12477_87331.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87332.xml')
        target_path = Path(self.in_path, self.calibration_metadata_path, '30000000016555_WO12477_87332.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87333.xml')
        target_path = Path(self.in_path, self.calibration_metadata_path, '30000000016555_WO12477_87333.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87334.xml')
        target_path = Path(self.in_path, self.calibration_metadata_path, '30000000016555_WO12477_87334.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87335.xml')
        target_path = Path(self.in_path, self.calibration_metadata_path, '30000000016555_WO12477_87335.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87336.xml')
        target_path = Path(self.in_path, self.calibration_metadata_path, '30000000016555_WO12477_87336.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87337.xml')
        target_path = Path(self.in_path, self.calibration_metadata_path, '30000000016555_WO12477_87337.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        actual_path = Path(os.path.dirname(__file__), '30000000016555_WO12477_87338.xml')
        target_path = Path(self.in_path, self.calibration_metadata_path, '30000000016555_WO12477_87338.xml')
        self.fs.add_real_file(actual_path, target_path=target_path)

        self.source_type_index = 3
        self.year_index = 4
        self.month_index = 5
        self.day_index = 6
        self.source_id_index = 7
        self.data_type_index = 8
        self.source_type_out = 'tchain'
        self.replace_schema_name = False
        self.write_site_file = False
        self.test_mode = True

    def test_calibration_parser(self) -> None:
        target_path = Path(self.in_path, self.calibration_metadata_path, '30000000016555_WO12477_87280.xml')
        stream_id = calibration_file_parser.get_stream_id(target_path)
        assert stream_id == '0'

    def test_schema_parser(self) -> None:
        schema_data: SchemaData = schema_parser.parse_schema_file(self.schema_path)
        assert schema_data.source_type == 'tchain'
        term_name = schema_data.calibration_mapping.get('0')
        assert term_name == 'depth0WaterTemp'
        term_name = schema_data.calibration_mapping.get('10')
        assert term_name == 'depth10WaterTemp'

    def test_parser(self) -> None:
        config = Config(data_path=self.in_path,
                        schema_path=self.schema_path,
                        out_path=self.out_path,
                        parse_calibration=True,
                        source_type_index=self.source_type_index,
                        source_type_out=self.source_type_out,
                        replace_schema_name=self.replace_schema_name,
                        write_site_file=self.write_site_file,
                        year_index=self.year_index,
                        month_index=self.month_index,
                        day_index=self.day_index,
                        source_id_index=self.source_id_index,
                        data_type_index=self.data_type_index,
                        test_mode=self.test_mode)
        array_parser.parse(config)
        self.check_output()

    def test_main(self) -> None:
        os.environ['DATA_PATH'] = str(self.in_path)
        os.environ['SCHEMA_PATH'] = str(self.schema_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['PARSE_CALIBRATION'] = str(True)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = str(self.source_type_index)
        os.environ['YEAR_INDEX'] = str(self.year_index)
        os.environ['MONTH_INDEX'] = str(self.month_index)
        os.environ['DAY_INDEX'] = str(self.day_index)
        os.environ['SOURCE_ID_INDEX'] = str(self.source_id_index)
        os.environ['DATA_TYPE_INDEX'] = str(self.data_type_index)
        os.environ['TEST_MODE'] = str(True)
        os.environ['REPLACE_SCHEMA_NAME'] = str(False)
        array_parser_main.main()
        self.check_output()

    def check_output(self) -> None:
        root_path = Path(self.out_path, self.metadata_path)
        data_path = Path(root_path, 'data', 'tchain_32610_2019-01-12.parquet')
        calibration_root_path = Path(root_path, 'calibration')
        calibration_path_0 = Path(calibration_root_path, 'depth0WaterTemp', '30000000016555_WO12477_87280.xml')
        calibration_path_1 = Path(calibration_root_path, 'depth1WaterTemp', '30000000016555_WO12477_87330.xml')
        calibration_path_2 = Path(calibration_root_path, 'depth2WaterTemp', '30000000016555_WO12477_87331.xml')
        calibration_path_3 = Path(calibration_root_path, 'depth3WaterTemp', '30000000016555_WO12477_87332.xml')
        calibration_path_4 = Path(calibration_root_path, 'depth4WaterTemp', '30000000016555_WO12477_87333.xml')
        calibration_path_5 = Path(calibration_root_path, 'depth5WaterTemp', '30000000016555_WO12477_87334.xml')
        calibration_path_6 = Path(calibration_root_path, 'depth6WaterTemp', '30000000016555_WO12477_87335.xml')
        calibration_path_7 = Path(calibration_root_path, 'depth7WaterTemp', '30000000016555_WO12477_87336.xml')
        calibration_path_8 = Path(calibration_root_path, 'depth8WaterTemp', '30000000016555_WO12477_87337.xml')
        calibration_path_9 = Path(calibration_root_path, 'depth9WaterTemp', '30000000016555_WO12477_87338.xml')
        self.assertTrue(data_path.exists())
        self.assertTrue(calibration_path_0.exists())
        self.assertTrue(calibration_path_1.exists())
        self.assertTrue(calibration_path_2.exists())
        self.assertTrue(calibration_path_3.exists())
        self.assertTrue(calibration_path_4.exists())
        self.assertTrue(calibration_path_5.exists())
        self.assertTrue(calibration_path_6.exists())
        self.assertTrue(calibration_path_7.exists())
        self.assertTrue(calibration_path_8.exists())
        self.assertTrue(calibration_path_9.exists())


class CloudParserTest(TestCase):

    def setUp(self) -> None:
        self.setUpPyfakefs()

        self.out_path = Path('/out')
        self.data_path = Path('/cloud_data')
        self.schema_path = Path('/in/schema/tchain_parsed.avsc')
        self.source_type = 'tchain'
        self.data_date = '2019-01-12'
        self.source_id = '32610'
        self.source_type_out = 'tchain'
        self.file_version = 'v2'
        self.common_path = '/common/'

        actual_schema = Path(os.path.dirname(__file__), 'tchain_parsed.avsc')
        self.fs.add_real_file(actual_schema, target_path=self.schema_path)

        # Create a fake data file at the hivestyle path that gen_path will return
        self.hivestyle_path = Path('/cloud_data/v2/tchain/2019/01/12/32610')
        self.data_file = Path(self.hivestyle_path, 'tchain_32610_2019-01-12.parquet')
        self.data_file.parent.mkdir(parents=True, exist_ok=True)
        self.data_file.touch()

    def _make_config(self, **kwargs) -> Config:
        defaults = dict(
            data_path=self.data_path,
            schema_path=self.schema_path,
            out_path=self.out_path,
            parse_calibration=False,
            source_type_index=None,
            source_type_out=self.source_type_out,
            replace_schema_name=False,
            write_site_file=False,
            year_index=None,
            month_index=None,
            day_index=None,
            source_id_index=None,
            data_type_index=None,
            test_mode=False,
            source_type=self.source_type,
            data_date=self.data_date,
            source_id_list=self.source_id,
            using_filesystem=False,
            file_version=self.file_version,
            common_path=self.common_path,
        )
        defaults.update(kwargs)
        return Config(**defaults)

    @patch('array_parser.array_parser.data_file_parser.write_restructured_file')
    @patch('array_parser.array_parser.import_module.import_base_module')
    def test_cloud_parse(self, mock_import_base, mock_write) -> None:
        out_file = Path('/out/v2/tchain/2019/01/12/32610/tchain_32610_2019-01-12.parquet')
        mock_gen_path = MagicMock()
        mock_gen_path.get_hivestyle_path.return_value = self.hivestyle_path
        mock_gen_path.get_hivestyle_glob.return_value = '*.parquet'
        mock_gen_path.create_output_path.return_value = out_file
        mock_import_base.return_value = mock_gen_path

        array_parser.parse(self._make_config())

        mock_gen_path.get_hivestyle_path.assert_called_once_with(
            self.data_path,
            self.file_version,
            self.data_date,
            self.source_type,
            self.source_id,
        )
        mock_write.assert_called_once_with(
            self.data_file,
            out_file,
            self.schema_path,
            False,
            False,
            {},
            self.data_date,
        )

    @patch('array_parser.array_parser.importlib.import_module')
    @patch('array_parser.array_parser.data_file_parser.write_restructured_file')
    @patch('array_parser.array_parser.import_module.import_base_module')
    def test_cloud_parse_trigger_table_update(self, mock_import_base, mock_write, mock_importlib) -> None:
        out_file = Path('/out/v2/tchain/2019/01/12/32610/tchain_32610_2019-01-12.parquet')
        mock_gen_path = MagicMock()
        mock_gen_path.get_hivestyle_path.return_value = self.hivestyle_path
        mock_gen_path.get_hivestyle_glob.return_value = '*.parquet'
        mock_gen_path.create_output_path.return_value = out_file
        mock_update_trigger_table = MagicMock()

        def import_base_side_effect(name, path):
            if name == 'gen_path':
                return mock_gen_path
            return MagicMock()

        mock_import_base.side_effect = import_base_side_effect
        mock_importlib.return_value = mock_update_trigger_table

        site = 'CRAM'

        def write_side_effect(path, out_path, schema, replace_schema, write_site, max_date_per_site=None, data_date=None):
            if max_date_per_site is not None and data_date is not None:
                max_date_per_site[site] = data_date

        mock_write.side_effect = write_side_effect

        config = self._make_config(update_trigger_table=True, utils_path='/utils/')
        array_parser.parse(config)

        mock_update_trigger_table.call_update_trigger_table.assert_called_once_with(
            site, self.source_type_out, max_date=self.data_date
        )
