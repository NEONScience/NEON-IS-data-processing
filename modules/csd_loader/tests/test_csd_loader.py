import datetime
from pathlib import Path
import sys
import tempfile
from types import ModuleType
import unittest
from unittest.mock import Mock, patch

storage_module = ModuleType('google.cloud.storage')
storage_module.Client = Mock()
cloud_module = ModuleType('google.cloud')
cloud_module.storage = storage_module
google_module = ModuleType('google')
google_module.cloud = cloud_module
environs_module = ModuleType('environs')
environs_module.Env = Mock()
with patch.dict(sys.modules, {
    'google': google_module,
    'google.cloud': cloud_module,
    'google.cloud.storage': storage_module,
    'environs': environs_module,
}):
    import csd_loader.csd_loader as csd_loader_module


class TestCsdLoader(unittest.TestCase):
    def test_lists_from_configured_prefix(self):
        values = {
            'INGEST_BUCKET': 'test-bucket',
            'GCS_ROOT_PREFIX': '/custom-root/',
            'CSD_FOLDER': '/custom-csd/',
        }
        env = Mock()
        env.str.side_effect = lambda key, default=None: values.get(key, default)
        blob = Mock()
        blob.name = 'custom-root/custom-csd/file.csv'
        blob.updated = datetime.datetime(2025, 4, 3, 12, 34, 56)
        bucket = Mock()
        storage_client = Mock()
        storage_client.bucket.return_value = bucket
        storage_client.list_blobs.return_value = [blob]

        with tempfile.TemporaryDirectory() as temp_dir:
            env.path.return_value = Path(temp_dir)
            with patch.object(csd_loader_module.environs, 'Env', return_value=env), \
                    patch.object(csd_loader_module.storage, 'Client', return_value=storage_client):
                csd_loader_module.csd_loader()

            storage_client.list_blobs.assert_called_once_with(bucket, prefix='custom-root/custom-csd')
            output_file = Path(
                temp_dir,
                '2025',
                '04',
                '03',
                'custom-root',
                'custom-csd',
                'file.parquet',
            )
            self.assertEqual(output_file.read_text(), '2025-04-03 12:34:56')


if __name__ == '__main__':
    unittest.main()