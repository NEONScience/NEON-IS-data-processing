#!/usr/bin/env python3
import json
import os
import unittest
from pathlib import Path
from typing import List

from pyfakefs.fake_filesystem import FakeFilesystem
from pyfakefs.fake_filesystem_unittest import TestCase

from data_access.types.property import Property
from publication_files_generator.database_queries.geolocation_geometry import Coordinates
from publication_files_generator.database_queries.named_location import NamedLocation
from publication_files_generator.database_queries.sensor_geolocations import GeoLocation
from publication_files_generator.sensor_positions_generator import generate_positions_file
from publication_files_generator.timestamp import get_timestamp


def create_locations_path(fs: FakeFilesystem) -> Path:
    locations = Path('/CPER/2020/01/02/locations')
    location_1 = Path(locations, 'CFGLOC101775')
    location_2 = Path(locations, 'CFGLOC101777')
    fs.create_dir(locations)
    fs.create_dir(location_1)
    fs.create_dir(location_2)
    return locations


# @unittest.skip('Not implemented.')
class LocationsGeneratorTest(TestCase):

    def setUp(self) -> None:
        root = os.path.dirname(__file__)
        self.setUpPyfakefs()
        self.in_path = Path('/in')
        self.out_path = Path('/out')
        self.domain = 'D10'
        self.site = 'CPER'
        self.year = '2020'
        self.month = '01'
        self.data_product_id = 'NEON.DOM.SITE.DP1.00041.001'
        self.month_path = Path(self.in_path, self.site, self.year, self.month)

        self.fs.create_dir(self.in_path)
        self.fs.create_dir(self.out_path)
        self.fs.create_dir(self.month_path)

        cfgloc101775 = Path(root, 'sensor_positions_generator_test_files/cfgloc101775.json')
        self.cfgloc101775_target = Path('/cfgloc101775.json')
        self.fs.add_real_file(cfgloc101775, target_path=self.cfgloc101775_target)

        cfgloc101775_properties = Path(root, 'sensor_positions_generator_test_files/cfgloc101775_properties.json')
        self.cfgloc101775_properties_target = Path('/cfgloc101775_properties.json')
        self.fs.add_real_file(cfgloc101775_properties, target_path=self.cfgloc101775_properties_target)

        cfgloc101777 = Path(root, 'sensor_positions_generator_test_files/cfgloc101777.json')
        self.cfgloc101777_target = Path('/cfgloc101777.json')
        self.fs.add_real_file(cfgloc101777, target_path=self.cfgloc101777_target)

        cfg101777_properties = Path(root, 'sensor_positions_generator_test_files/cfgloc101777_properties.json')
        self.cfgloc101777_properties_target = Path('/cfgloc_101777_properties.json')
        self.fs.add_real_file(cfg101777_properties, target_path=self.cfgloc101777_properties_target)

    def test_positions_generator(self):
        timestamp = get_timestamp()
        generate_positions_file(locations_path=create_locations_path(self.fs),
                                location_path_index=6,
                                out_path=self.out_path,
                                domain=self.domain,
                                site=self.site,
                                year=self.year,
                                month=self.month,
                                data_product_id=self.data_product_id,
                                timestamp=timestamp,
                                get_geolocations=self.get_locations,
                                get_named_location=self.get_named_location,
                                get_geometry=self.get_geometry)

    @staticmethod
    def get_property(json_property):
        name = json_property['attr_name']
        value = json_property['string_value']
        return Property(name=name, value=value)

    @staticmethod
    def get_geometry(_geometry: str) -> str:
        """Mock function to return the coordinates for a location."""
        # return Coordinates(latitude='-104.745591', longitude='40.815536', elevation='1653.9151')
        return 'POLYGON Z ((-104.746013 40.815892 1654.009392,-104.745973 40.815922 1654.052064))'

    def get_properties(self, location_name) -> List[Property]:
        """Read properties from a file."""
        properties = []
        if location_name == 'CFGLOG101775':
            with open(self.cfgloc101775_properties_target) as file:
                json_data = json.load(file)
                for json_property in json_data:
                    properties.append(self.get_property(json_property))
        if location_name == 'CFGLOG101777':
            with open(self.cfgloc101777_properties_target) as file:
                json_data = json.load(file)
                for json_property in json_data:
                    properties.append(self.get_property(json_property))
        return properties

    def get_geolocation(self, json_location: dict) -> GeoLocation:
        """Create GeoLocation object from json file data."""
        location_name = json_location['nam_locn_name']
        location_id = json_location['locn_id']
        geometry = json_location['point']
        start_date = json_location['locn_nam_locn_strt_date']
        end_date = json_location['locn_nam_locn_end_date']
        alpha = json_location['locn_alph_ortn']
        beta = json_location['locn_beta_ortn']
        gamma = json_location['locn_gama_ortn']
        x_offset = json_location['locn_x_off']
        y_offset = json_location['locn_y_off']
        z_offset = json_location['locn_z_off']
        reference_id = json_location['nam_locn_id_off']
        return GeoLocation(location_id=location_id,
                           geometry=geometry,
                           start_date=start_date,
                           end_date=end_date,
                           alpha=alpha,
                           beta=beta,
                           gamma=gamma,
                           x_offset=x_offset,
                           y_offset=y_offset,
                           z_offset=z_offset,
                           properties=self.get_properties(location_name),
                           offset_id=reference_id,
                           offset_name='SOILPL101755',
                           offset_description='SOILPL101755')

    def get_locations(self, named_location: str) -> List[GeoLocation]:
        locations: List[GeoLocation] = []
        if named_location == 'CFGLOG101775':
            with open(self.cfgloc101775_target) as file:
                json_data = json.load(file)
                for json_location in json_data:
                    locations.append(self.get_geolocation(json_location))
        if named_location == 'CFGLOG1017757':
            with open(self.cfgloc101777_target) as file:
                json_data = json.load(file)
                for json_location in json_data:
                    locations.append(self.get_geolocation(json_location))
        return locations

    def get_named_location(self, named_location_name) -> NamedLocation:
        """Create a NamedLocation object."""
        properties = self.get_properties(named_location_name)
        if named_location_name == 'CFGLOC101775':
            return NamedLocation(location_id='138773',
                                 name='CFGLOC101775',
                                 description='Central Plains Soil Temp Profile SP2, Z6 Depth',
                                 properties=properties)
        if named_location_name == 'CFGLOC101777':
            return NamedLocation('138775',
                                 'CFGLOC101777',
                                 'Central Plains Soil Temp Profile SP2, Z7 Depth',
                                 properties=properties)
