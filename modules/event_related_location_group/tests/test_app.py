import os

from pyfakefs.fake_filesystem_unittest import TestCase

import event_related_location_group.app as app


class AppTest(TestCase):

    def setUp(self):

        self.setUpPyfakefs()

        self.group = 'aspirated-single-169'

        self.out_path = os.path.join('/', 'outputs')
        self.data_path = os.path.join('/', 'repo', 'data')
        self.event_path = os.path.join('/', 'repo', 'events')
        self.metadata_path = os.path.join('2019', '01', '05', self.group)

        # Data files.
        self.dualfan_data = os.path.join(self.metadata_path, 'dualfan', 'CFGLOC111066', 'data',
                                         'dualfan_CFGLOC111066_2019-01-05.avro')
        self.dualfan_location = os.path.join(self.metadata_path, 'dualfan', 'CFGLOC111066', 'location',
                                             'dualfan_38462_locations.json')

        self.prt_data = os.path.join(self.metadata_path, 'prt', 'CFGLOC111066', 'data',
                                     'prt_CFGLOC111066_2019-01-05.avro')
        self.prt_flags = os.path.join(self.metadata_path, 'prt', 'CFGLOC111066', 'flags',
                                      'prt_CFGLOC111066_2019-01-05_validCal.avro')
        self.prt_location = os.path.join(self.metadata_path, 'prt', 'CFGLOC111066', 'location',
                                         'prt_31052_locations.json')
        self.prt_uncertainty = os.path.join(self.metadata_path, 'prt', 'CFGLOC111066', 'uncertainty',
                                            'prt_CFGLOC111066_2019-01-05_uncertainty.json')
        self.prt_uncertainty_fdas = os.path.join(self.metadata_path, 'prt', 'CFGLOC111066', 'uncertainty_fdas',
                                                 'prt_CFGLOC111066_2019-01-05_FDASUncertainty.avro')

        self.windobserver_data = os.path.join(self.metadata_path, 'windobserverii', 'CFGLOC111062', 'data',
                                              'windobserverii_CFGLOC111062_2019-01-05.avro')
        self.windobserver_location = os.path.join(self.metadata_path, 'windobserverii', 'CFGLOC111062', 'location',
                                                  'windobserverii_3794_locations.json')

        #  Create data files.
        self.fs.create_file(os.path.join(self.data_path, self.dualfan_data))
        self.fs.create_file(os.path.join(self.data_path, self.dualfan_location))

        self.fs.create_file(os.path.join(self.data_path, self.prt_data))
        self.fs.create_file(os.path.join(self.data_path, self.prt_flags))
        self.fs.create_file(os.path.join(self.data_path, self.prt_location))
        self.fs.create_file(os.path.join(self.data_path, self.prt_uncertainty))
        self.fs.create_file(os.path.join(self.data_path, self.prt_uncertainty_fdas))

        self.fs.create_file(os.path.join(self.data_path, self.windobserver_data))
        self.fs.create_file(os.path.join(self.data_path, self.windobserver_location))

        #  Create event files.
        self.event_location = os.path.join('heater', self.group,
                                           '9999999', 'location', 'heater_9999999_locations.json')
        self.event_data = os.path.join('heater', self.group,
                                       '9999999', 'data', 'heater_9999999_events.json')
        self.fs.create_file(os.path.join(self.event_path, self.event_location))
        self.fs.create_file(os.path.join(self.event_path, self.event_data))

        #  Create output directory.
        self.fs.create_dir(self.out_path)

    def test_main(self):
        os.environ['DATA_PATH'] = self.data_path
        os.environ['EVENT_PATH'] = self.event_path
        os.environ['OUT_PATH'] = self.out_path
        os.environ['LOG_LEVEL'] = 'DEBUG'

        app.main()
        self.check_output()

    def check_output(self):

        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.dualfan_data)))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.dualfan_location)))

        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.prt_data)))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.prt_flags)))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.prt_location)))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.prt_uncertainty)))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.prt_uncertainty_fdas)))

        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.windobserver_data)))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.windobserver_location)))

        event_location_path = os.path.join(self.out_path, self.metadata_path, 'heater', '9999999',
                                           'location', 'heater_9999999_locations.json')
        event_data_path = os.path.join(self.out_path, self.metadata_path, 'heater', '9999999',
                                       'data', 'heater_9999999_events.json')

        print(f'event_location_path: {event_location_path}')

        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_path, event_location_path)))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_path, event_data_path)))
