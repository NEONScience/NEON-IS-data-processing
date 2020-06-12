#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from padded_timeseries_analyzer.padded_timeseries_analyzer.analyzer import PaddedTimeSeriesAnalyzer
import padded_timeseries_analyzer.padded_timeseries_analyzer.app as app


class PaddedTimeSeriesAnalyzerTest(TestCase):

    def setUp(self):
        """Set required files in mock filesystem."""
        self.setUpPyfakefs()

        self.threshold_dir = 'threshold'
        self.threshold_file = 'thresholds.json'

        self.out_dir = Path('/tmp/outputs')
        self.input_root = Path('/tmp/inputs')

        source_root = Path('prt/2018/01')
        self.input_data_dir = Path(self.input_root, source_root, '03')

        location = 'CFGLOC112154'
        self.source_dir = Path(source_root, '03', location)
        self.previous_dir = Path(source_root, '02', location)
        self.next_dir = Path(source_root, '04', location)
        self.outside_range_dir = Path(source_root, '05', location)

        self.previous_data_file = f'prt_{location}_2018-01-02.ext'
        self.source_data_file = f'prt_{location}_2018-01-03.ext'
        self.next_data_file = f'prt_{location}_2018-01-04.ext'
        self.outside_range_file = f'prt_{location}_2018-01-05.ext'

        # Ancillary location file.
        self.fs.create_file(Path(self.input_root, self.source_dir, 'location', 'locations.json'))

        # Threshold file.
        threshold_path = Path(self.input_root, self.source_dir, self.threshold_dir, self.threshold_file)
        self.fs.create_file(threshold_path)

        self.data_dir = 'data'

        #  Source data file.
        data_root = Path(self.input_root, self.source_dir, self.data_dir)
        source_data_path = Path(data_root, self.source_data_file)
        self.fs.create_file(source_data_path)

        #  Manifest file.
        manifest_path = Path(data_root, 'manifest.txt')
        test_manifest = Path(os.path.dirname(__file__), 'test_manifest.txt')
        self.fs.add_real_file(test_manifest, target_path=manifest_path)

        #  Previous data file.
        previous_data_path = Path(data_root, self.previous_data_file)
        self.fs.create_file(previous_data_path)

        #  Next data file.
        next_data_path = Path(data_root, self.next_data_file)
        self.fs.create_file(next_data_path)

        outside_range_path = Path(self.outside_range_dir, self.outside_range_file)
        self.fs.create_file(outside_range_path)

        self.relative_path_index = 3

    def test_analyzer(self):
        analyzer = PaddedTimeSeriesAnalyzer(self.input_data_dir, self.out_dir, self.relative_path_index)
        analyzer.analyze()
        self.check_output()

    def test_main(self):
        os.environ['DATA_PATH'] = str(self.input_data_dir)
        os.environ['OUT_PATH'] = str(self.out_dir)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['RELATIVE_PATH_INDEX'] = str(self.relative_path_index)
        app.main()
        self.check_output()

    def check_output(self):
        """Check files in the output directory."""
        location_path = Path(self.out_dir, self.source_dir, 'location', 'locations.json')
        threshold_path = Path(self.out_dir, self.source_dir, self.threshold_dir, self.threshold_file)
        output_root = Path(self.out_dir, self.source_dir)
        data_path = Path(output_root, self.data_dir, self.source_data_file)
        previous_data_path = Path(output_root, self.data_dir, self.previous_data_file)
        next_data_path = Path(output_root, self.data_dir, self.next_data_file)
        outside_range_path = Path(output_root, self.data_dir, self.outside_range_file)
        self.assertTrue(location_path.exists())
        self.assertTrue(threshold_path.exists())
        self.assertTrue(data_path.exists())
        self.assertTrue(previous_data_path.exists())
        self.assertTrue(next_data_path.exists())
        self.assertFalse(outside_range_path.exists())
