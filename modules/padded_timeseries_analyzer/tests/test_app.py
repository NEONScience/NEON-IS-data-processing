import os

from pyfakefs.fake_filesystem_unittest import TestCase

import padded_timeseries_analyzer.padded_timeseries_analyzer.analyzer as analyzer
import padded_timeseries_analyzer.padded_timeseries_analyzer.app as app
import lib.log_config as log_config
from lib.merged_data_filename import MergedDataFilename


class AppTest(TestCase):

    def setUp(self):
        """Set required files in mock filesystem."""

        log_config.configure('DEBUG')

        self.setUpPyfakefs()

        self.threshold_dir = 'threshold'
        self.threshold_file = 'thresholds.json'

        self.out_dir = os.path.join('/', 'tmp', 'outputs')
        self.input_root = os.path.join('/', 'tmp', 'inputs',)

        source_root = os.path.join('prt', '2018', '01')
        self.input_data_dir = os.path.join(self.input_root, source_root, '03')

        location = 'CFGLOC112154'
        self.source_dir = os.path.join(source_root, '03', location)
        self.previous_dir = os.path.join(source_root, '02', location)
        self.next_dir = os.path.join(source_root, '04', location)
        self.outside_range_dir = os.path.join(source_root, '05', location)

        self.previous_data_file = MergedDataFilename.build('prt', location, '2018', '01', '02')
        self.source_data_file = MergedDataFilename.build('prt', location, '2018', '01', '03')
        self.next_data_file = MergedDataFilename.build('prt', location,  '2018', '01', '04')
        self.outside_range_file = MergedDataFilename.build('prt', location, '2018', '01', '05')

        self.data_dir = 'data'

        #  Source data file.
        data_root = os.path.join(self.input_root, self.source_dir, self.data_dir)
        source_data_path = os.path.join(data_root, self.source_data_file)
        self.fs.create_file(source_data_path)

        #  Manifest file.
        manifest_path = os.path.join(data_root, 'manifest.txt')
        test_manifest = os.path.join(os.path.dirname(__file__), 'test_manifest.txt')
        self.fs.add_real_file(test_manifest, target_path=manifest_path)

        #  Previous data file.
        previous_data_path = os.path.join(data_root, self.previous_data_file)
        self.fs.create_file(previous_data_path)

        #  Next data file.
        next_data_path = os.path.join(data_root, self.next_data_file)
        self.fs.create_file(next_data_path)

        outside_range_path = os.path.join(self.outside_range_dir, self.outside_range_file)
        self.fs.create_file(outside_range_path)

        print(f'source_data_path: {source_data_path}')
        print(f'manifest: {manifest_path}')
        print(f'previous_data_path: {previous_data_path}')
        print(f'next_data_path: {next_data_path}')
        print(f'outside_range_path: {outside_range_path}')

    def test_analyzer(self):
        analyzer.analyze(self.input_data_dir, self.out_dir)
        self.check_output()

    def test_main(self):
        os.environ['DATA_PATH'] = self.input_data_dir
        os.environ['OUT_PATH'] = self.out_dir
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.check_output()

    def check_output(self):
        """Check files in the output directory."""
        threshold_path = os.path.join(self.out_dir, self.source_dir, self.threshold_dir, self.threshold_file)
        output_root = os.path.join(self.out_dir, self.source_dir)
        data_path = os.path.join(output_root, self.data_dir, self.source_data_file)
        previous_data_path = os.path.join(output_root, self.data_dir, self.previous_data_file)
        next_data_path = os.path.join(output_root, self.data_dir, self.next_data_file)
        outside_range_path = os.path.join(output_root, self.data_dir, self.outside_range_file)
        self.assertTrue(os.path.lexists(threshold_path))
        self.assertTrue(os.path.lexists(data_path))
        self.assertTrue(os.path.lexists(previous_data_path))
        self.assertTrue(os.path.lexists(next_data_path))
        self.assertFalse(os.path.lexists(outside_range_path))
