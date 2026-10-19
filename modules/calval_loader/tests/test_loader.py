#!/usr/bin/env python3
import os
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch
# import xml.etree.ElementTree as ElementTree

from calval_loader import load_all_calval_files
from calval_loader.get_calibration_stream_name import get_calibration_stream_name


# from certificate_file_parser import CertificateFileParser
# from get_calibration_stream_name import get_calibration_stream_name
# import s3_client

@unittest.skip('Is this module used?')
class AppTest(unittest.TestCase):

    # def test_s3_download(self):
    #     out_path = Path('/out')
    #     self.fs.create_dir(out_path)
    #     bucket = s3_client.get_bucket()
    #     file_name = '10000000005118_WO29435_164027.xml'
    #     expected_path = Path(out_path, file_name)
    #     bucket.download_file(file_name, str(expected_path))
    #     assert expected_path.exists()

    # def test_certificate_parser(self):
    #     test_file_path = Path(os.path.dirname(__file__), '10000000000084_WO21814_120104.xml')
    #     calibration_path = '/calibrations.xml'
    #     self.fs.add_real_file(test_file_path, target_path=calibration_path)
    #     xml_root = ElementTree.parse(calibration_path).getroot()
    #     parser = CertificateFileParser()
    #     certificate = parser.parse(xml_root)
    #     certificate.show()
    #     assert certificate is not None

    @staticmethod
    def test_calibration_stream_name():
        import psycopg2
        from contextlib import closing

        # database URL in the form: [user]/[pass]@[url]:[port]/[sid]
        db_url = os.getenv('DATABASE_URL')
        with closing(psycopg2.connect(db_url)) as connection:
            stream_name = get_calibration_stream_name(connection, 'exo2', 0)
            assert stream_name == 'conductance'


class LoadAllCalvalFilesTests(unittest.TestCase):
    source_types = [
        'exo2',
        'exo2conductivity',
        'exo2turbidity',
        'exo2fdom',
        'exo2phorp',
        'exo2dissolvedoxygen',
        'exo2algae',
    ]

    def _run_loader_for_assets(self, assets, source_types, legacy_source_type='exo2', legacy_schema_name=None):
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_directory = Path(temporary_directory)
            input_paths = [Path(f'/fake/input/cal-{asset_id}.dat') for asset_id, _ in assets]
            blobs = {}
            for asset_id, schema_name in assets:
                filename = f'cal-{asset_id}.xml'
                blob = MagicMock()
                blob.download_as_bytes.return_value = (
                    f'<Root><SensorID><MxAssetID>{asset_id}</MxAssetID></SensorID>'
                    '<StreamCalVal><StreamID>3</StreamID></StreamCalVal></Root>'
                ).encode()
                blobs[filename] = blob

            class FakeEnv:
                def list(self, name, default=None):
                    return source_types

                def str(self, name, default=None):
                    if name == 'CVAL_INGEST_BUCKET':
                        return 'bucket'
                    if name == 'SOURCE_TYPE':
                        return legacy_source_type
                    if name == 'SCHEMA_NAME':
                        return legacy_schema_name or default
                    return default

                def path(self, name):
                    return Path('/fake/input') if name == 'IN_PATH' else output_directory

                def int(self, name):
                    return 2

            connector = MagicMock()
            with ExitStack() as stack:
                stack.enter_context(patch.object(load_all_calval_files.environs, 'Env', return_value=FakeEnv()))
                storage_client = stack.enter_context(patch.object(load_all_calval_files.storage, 'Client'))
                stack.enter_context(patch.object(load_all_calval_files, 'read_from_mount'))
                stack.enter_context(patch.object(load_all_calval_files, 'DbConnector', return_value=connector))
                get_schema = stack.enter_context(patch.object(
                    load_all_calval_files,
                    'get_avro_schema_name',
                    side_effect=lambda connection, asset_id: next(
                        (schema_name,) for current_id, schema_name in assets if current_id == asset_id
                    ),
                ))
                get_stream = stack.enter_context(patch.object(
                    load_all_calval_files,
                    'get_calibration_stream_name',
                    side_effect=lambda connection, schema_name, stream_id: f'stream-{schema_name}',
                ))
                rglob = stack.enter_context(patch.object(Path, 'rglob', return_value=input_paths))
                stack.enter_context(patch.object(Path, 'is_file', return_value=True))
                storage_client.return_value.bucket.return_value.get_blob.side_effect = blobs.__getitem__
                load_all_calval_files.load()

            return output_directory, blobs, rglob, get_schema, get_stream

    def test_routes_seven_source_types_in_one_input_traversal(self):
        assets = [(str(index), source_type) for index, source_type in enumerate(self.source_types, start=100)]

        output_directory, blobs, rglob, get_schema, get_stream = self._run_loader_for_assets(assets, self.source_types)

        rglob.assert_called_once_with('*')
        self.assertEqual(get_schema.call_count, len(self.source_types))
        self.assertEqual(get_stream.call_count, len(self.source_types))
        for (asset_id, source_type), blob in zip(assets, blobs.values()):
            self.assertEqual(blob.download_as_bytes.call_count, 1)
            output_path = output_directory / source_type / asset_id / f'stream-{source_type}' / f'cal-{asset_id}.xml'
            self.assertTrue(output_path.is_file())

    def test_preserves_single_source_type_and_schema_override(self):
        assets = [('101', 'custom-exo2-schema')]

        output_directory, _, _, _, _ = self._run_loader_for_assets(
            assets,
            [],
            legacy_source_type='exo2',
            legacy_schema_name='custom-exo2-schema',
        )

        output_path = output_directory / 'exo2' / '101' / 'stream-custom-exo2-schema' / 'cal-101.xml'
        self.assertTrue(output_path.is_file())
