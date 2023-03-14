#!/usr/bin/env python3
import json
import os
import unittest
from pathlib import Path
from typing import List

from pyfakefs.fake_filesystem_unittest import TestCase

from publication_files_generator.database_queries.sensor_locations import Location
from publication_files_generator.sensor_locations_generator import generate_locations_file
from publication_files_generator.timestamp import get_timestamp


@unittest.skip('Not implemented.')
class LocationsGeneratorTest(TestCase):

    def setUp(self) -> None:
        self.setUpPyfakefs()
        self.in_path = Path('/in')
        self.out_path = Path('/out')
        self.domain = 'D10'
        self.site = 'CPER'
        self.year = '2020'
        self.month = '01'
        self.data_product_id = 'NEON.DOM.SITE.DP1.00041.001'
        self.month_path = Path(self.in_path, self.site, self.year, self.month)
        self.locations_path = Path(self.month_path, '02', 'locations')
        self.location_1_path = Path(self.locations_path, 'CFGLOC101775')
        self.location_2_path = Path(self.locations_path, 'CFGLOC101777')
        self.fs.create_dir(self.in_path)
        self.fs.create_dir(self.out_path)
        self.fs.create_dir(self.month_path)
        self.fs.create_dir(self.locations_path)
        self.fs.create_dir(self.location_1_path)
        self.fs.create_dir(self.location_2_path)

        location_1_path = Path(os.path.dirname(__file__), 'sensor_locations_generator_test_files/cfgloc101775.json')
        self.cfgloc_101775_target_path = Path('/cfgloc_101775.json')
        self.fs.add_real_file(location_1_path, target_path=self.cfgloc_101775_target_path)

        location_2_path = Path(os.path.dirname(__file__), 'sensor_locations_generator_test_files/cfgloc101777.json')
        self.cfgloc_101777_target_path = Path('/cfgloc_101777.json')
        self.fs.add_real_file(location_2_path, target_path=self.cfgloc_101777_target_path)

    def get_locations(self, named_location: str) -> List[Location]:
        locations = []
        if named_location == 'CFGLOG101775':
            with open(self.cfgloc_101775_target_path) as file:
                json_data = json.load(file)
                for location in json_data:
                    location_id = location['locn_id']
                    geometry = location['point']
                    start_date = location['locn_nam_locn_strt_date']
                    end_date = location['locn_nam_locn_end_date']
                    alpha = location['locn_alph_ortn']
                    beta = location['locn_beta_ortn']
                    gamma = location['locn_gama_ortn']
                    x_offset = location['locn_x_off']
                    y_offset = location['locn_y_off']
                    z_offset = location['locn_z_off']
                    reference_id = location['nam_locn_id_off']
                    sensor_location = Location(
                        location_id=location_id,
                        geometry=geometry,
                        start_date=start_date,
                        end_date=end_date,
                        alpha=alpha,
                        beta=beta,
                        gamma=gamma,
                        x_offset=x_offset,
                        y_offset=y_offset,
                        z_offset=z_offset,
                        location_properties=[],  # TODO run query for properties
                        named_location_offset_id=reference_id,
                        named_location_offset_name='')  # TODO run query to fill in
                    locations.append(sensor_location)
        return locations

    def test_locations_generator(self):
        timestamp = get_timestamp()
        generate_locations_file(in_path=self.month_path,
                                out_path=self.out_path,
                                domain=self.domain,
                                site=self.site,
                                year=self.year,
                                month=self.month,
                                data_product_id=self.data_product_id,
                                timestamp=timestamp,
                                get_locations=self.get_locations)
