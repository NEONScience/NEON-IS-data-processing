import os

from pyfakefs.fake_filesystem_unittest import TestCase

import context_filter.app as app
from context_filter.filter import ContextFilter
from lib import log_config as log_config


class AppTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')

        # File path indices.
        self.source_type_index = 3
        self.year_index = 4
        self.month_index = 5
        self.day_index = 6
        self.source_id_index = 7
        self.data_type_index = 8

        self.setUpPyfakefs()

        self.out_path = os.path.join('/', 'outputs')
        self.metadata_path = os.path.join('prt', '2019', '05', '21', '00001')

        self.context = 'aspirated-triple'  # The context to find in the location file.

        self.in_path = os.path.join('/', 'inputs')
        inputs_path = os.path.join(self.in_path, 'merged', self.metadata_path)

        data_path = os.path.join(inputs_path, 'data', 'data.avro')
        flags_path = os.path.join(inputs_path, 'flags', 'flags.avro')
        locations_path = os.path.join(inputs_path, 'location', 'locations.json')
        uncertainty_coefficient_path = os.path.join(inputs_path,
                                                    'uncertainty_coefficient', 'uncertaintyCoefficient.json')

        self.fs.create_file(data_path)
        self.fs.create_file(flags_path)
        self.fs.create_file(uncertainty_coefficient_path)

        # Use real location file for parsing
        actual_location_file_path = os.path.join(os.path.dirname(__file__), 'test-locations.json')
        self.fs.add_real_file(actual_location_file_path, target_path=locations_path)

    def test_filter(self):
        context_filter = ContextFilter(self.source_type_index,
                                       self.year_index,
                                       self.month_index,
                                       self.day_index,
                                       self.source_id_index,
                                       self.data_type_index)
        context_filter.filter(self.in_path, self.out_path, self.context)
        self.check_output()

    def test_main(self):
        os.environ['IN_PATH'] = self.in_path
        os.environ['OUT_PATH'] = self.out_path
        os.environ['CONTEXT'] = self.context
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = str(self.source_type_index)
        os.environ['YEAR_INDEX'] = str(self.year_index)
        os.environ['MONTH_INDEX'] = str(self.month_index)
        os.environ['DAY_INDEX'] = str(self.day_index)
        os.environ['SOURCE_ID_INDEX'] = str(self.source_id_index)
        os.environ['DATA_TYPE_INDEX'] = str(self.data_type_index)
        app.main()
        self.check_output()

    def check_output(self):

        root_path = os.path.join(self.out_path, self.metadata_path)

        data_path = os.path.join(root_path, 'data', 'data.avro')
        flags_path = os.path.join(root_path, 'flags', 'flags.avro')
        locations_path = os.path.join(root_path, 'location', 'locations.json')
        uncertainty_coefficient_path = os.path.join(root_path,
                                                    'uncertainty_coefficient', 'uncertaintyCoefficient.json')

        self.assertTrue(os.path.lexists(data_path))
        self.assertTrue(os.path.lexists(flags_path))
        self.assertTrue(os.path.lexists(locations_path))
        self.assertTrue(os.path.lexists(uncertainty_coefficient_path))
