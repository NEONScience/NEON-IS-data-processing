#!/usr/bin/env python3
import csv
import json
import os
from pathlib import Path
from typing import List

from pyfakefs.fake_filesystem import FakeFilesystem
from pyfakefs.fake_filesystem_unittest import TestCase

from data_access.types.property import Property
from pub_files.database.queries.geolocation_geometry import Geometry
from pub_files.database.queries.geolocations import GeoLocation
from pub_files.database.queries.named_locations import NamedLocation
from pub_files.input_files.file_metadata import PathElements
from pub_files.output_files.sensor_positions.sensor_positions_file import SensorPositionsDatabase
from pub_files.output_files.sensor_positions.sensor_positions_file import SensorPositionsFile
from pub_files.tests.file_date_converter import to_datetime
from pub_files.main import get_timestamp


def create_location_path(fs: FakeFilesystem) -> Path:
    location_root = Path('/2020/01/02/CPER/soil-temp_CPER100200/location')
    location_file_1 = Path(location_root, 'CFGLOC101775.json')
    location_file_2 = Path(location_root, 'CFGLOC101777.json')
    fs.create_dir(location_root)
    fs.create_file(location_file_1)
    fs.create_file(location_file_2)
    return location_root


class PositionsFileTest(TestCase):

    def setUp(self) -> None:
        root = Path(os.path.dirname(__file__), 'data')
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

        soilpl101775 = Path(root, 'soilpl101755.json')
        self.soilpl101775_target = Path('/soilpl101775.json')
        self.fs.add_real_file(soilpl101775, target_path=self.soilpl101775_target)

        soilpl101775_properties = Path(root, 'soilpl101755_properties.json')
        self.soilpl101775_properties_target = Path('/soilpl101775_properties.json')
        self.fs.add_real_file(soilpl101775_properties, target_path=self.soilpl101775_properties_target)

        cfgloc101775 = Path(root, 'cfgloc101775.json')
        self.cfgloc101775_target = Path('/cfgloc101775.json')
        self.fs.add_real_file(cfgloc101775, target_path=self.cfgloc101775_target)

        cfgloc101775_properties = Path(root, 'cfgloc101775_properties.json')
        self.cfgloc101775_properties_target = Path('/cfgloc101775_properties.json')
        self.fs.add_real_file(cfgloc101775_properties, target_path=self.cfgloc101775_properties_target)

        cfgloc101777 = Path(root, 'cfgloc101777.json')
        self.cfgloc101777_target = Path('/cfgloc101777.json')
        self.fs.add_real_file(cfgloc101777, target_path=self.cfgloc101777_target)

        cfg101777_properties = Path(root, 'cfgloc101777_properties.json')
        self.cfgloc101777_properties_target = Path('/cfgloc_101777_properties.json')
        self.fs.add_real_file(cfg101777_properties, target_path=self.cfgloc101777_properties_target)

    def test_write(self):
        timestamp = get_timestamp()
        elements = PathElements(domain=self.domain, site=self.site, year=self.year, month=self.month,
                                data_product_id=self.data_product_id)
        location_path = create_location_path(self.fs)
        database = SensorPositionsDatabase(get_geolocations=self.get_geolocations,
                                           get_named_location=self.get_named_location,
                                           get_geometry=self.get_geometry)
        filename = SensorPositionsFile(location_path=location_path, out_path=self.out_path, elements=elements,
                                       timestamp=timestamp, database=database).write()
        file_path = Path(self.out_path, self.site, self.year, self.month, filename)
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            i = 0
            for row in reader:
                i += 1
                print(row)
            assert i == 3

    @staticmethod
    def get_property(json_property):
        name = json_property['attr_name']
        value = json_property['string_value']
        return Property(name=name, value=value)

    @staticmethod
    def get_geometry(_named_location: str) -> Geometry:
        """Mock function to return the geometry for a location."""
        geometry = 'POLYGON Z ((-104.746013 40.815892 1654.009392,-104.745973 40.815922 1654.052064))'
        return Geometry(geometry=geometry, srid=4979)

    def get_properties(self, location_name) -> List[Property]:
        """Read properties from a file."""
        properties = []
        if location_name == 'CFGLOC101775':
            with open(self.cfgloc101775_properties_target) as file:
                json_data = json.load(file)
                for json_property in json_data:
                    properties.append(self.get_property(json_property))
        if location_name == 'CFGLOC101777':
            with open(self.cfgloc101777_properties_target) as file:
                json_data = json.load(file)
                for json_property in json_data:
                    properties.append(self.get_property(json_property))
        if location_name == 'SOILPL101755':
            with open(self.soilpl101775_properties_target) as file:
                json_data = json.load(file)
                for json_property in json_data:
                    properties.append(self.get_property(json_property))
        return properties

    def get_geolocation(self, json_location: dict) -> GeoLocation:
        """Create GeoLocation object from json file data."""
        location_name = json_location['nam_locn_name']
        location_id = json_location['locn_id']
        geometry = json_location['point']
        start_date = to_datetime(json_location['locn_nam_locn_strt_date'])
        end_date = to_datetime(json_location['locn_nam_locn_end_date'])
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

    def get_geolocations(self, named_location: str) -> List[GeoLocation]:
        locations: List[GeoLocation] = []
        if named_location == 'CFGLOC101775':
            with open(self.cfgloc101775_target) as file:
                json_data = json.load(file)
                for json_location in json_data:
                    locations.append(self.get_geolocation(json_location))
        if named_location == 'CFGLOC1017757':
            with open(self.cfgloc101777_target) as file:
                json_data = json.load(file)
                for json_location in json_data:
                    locations.append(self.get_geolocation(json_location))
        if named_location == 'SOILPL101755':
            with open(self.soilpl101775_target) as file:
                json_data = json.load(file)
                for json_location in json_data:
                    locations.append(self.get_geolocation(json_location))
        return locations

    def get_named_location(self, named_location_name) -> NamedLocation:
        properties = self.get_properties(named_location_name)
        if named_location_name == 'CFGLOC101775':
            return NamedLocation(location_id=138773,
                                 name='CFGLOC101775',
                                 description='Central Plains Soil Temp Profile SP2, Z6 Depth',
                                 properties=properties)
        if named_location_name == 'CFGLOC101777':
            return NamedLocation(138775,
                                 'CFGLOC101777',
                                 'Central Plains Soil Temp Profile SP2, Z7 Depth',
                                 properties=properties)
        if named_location_name == 'SOILPL101755':
            return NamedLocation(138753,
                                 'SOILPL101755',
                                 'Central Plains Soil Plot, SP2',
                                 properties=properties)
