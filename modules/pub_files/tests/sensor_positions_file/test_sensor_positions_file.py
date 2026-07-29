#!/usr/bin/env python3
import csv
import json
import os
from pathlib import Path
from typing import List

from pyfakefs.fake_filesystem import FakeFilesystem
from pyfakefs.fake_filesystem_unittest import TestCase

from data_access.types.property import Property
from pub_files.database.geolocation_geometry import Geometry
from pub_files.database.geolocations import GeoLocation
from pub_files.database.named_locations import NamedLocation
from pub_files.geometry import build_geometry
from pub_files.input_files.file_metadata import PathElements
from pub_files.main import get_timestamp
from pub_files.output_files.sensor_positions.sensor_positions_file import SensorPositionsDatabase
from pub_files.output_files.sensor_positions.sensor_positions_file import get_column_names
from pub_files.output_files.sensor_positions.sensor_positions_file import write_file
from pub_files.output_files.sensor_positions.sensor_specific_processors import create_tchain_rows
from pub_files.tests.file_date_converter import to_datetime


def create_location_path(fs: FakeFilesystem) -> Path:
    in_path = Path('/locations')
    location_path = Path(in_path, '2020/01/02/CPER')
    root_file_path = Path(location_path, 'location/soil-temp_CPER100200/')
    location_file_1 = Path(root_file_path, 'CFGLOC101775.json')
    location_file_2 = Path(root_file_path, 'CFGLOC101777.json')
    fs.create_dir(root_file_path)
    fs.create_file(location_file_1)
    fs.create_file(location_file_2)
    return location_path


class PositionsFileTest(TestCase):

    def setUp(self) -> None:
        data_root_path = Path(os.path.dirname(__file__), 'data')

        self.setUpPyfakefs()
        self.in_path = Path('/in')
        self.domain = 'D10'
        self.site = 'CPER'
        self.year = '2020'
        self.month = '01'
        self.data_product_id = 'NEON.DOM.SITE.DP1.00041.001'
        self.out_path = Path('/out', self.site, self.year, self.month)
        self.fs.create_dir(self.in_path)
        self.fs.create_dir(self.out_path)

        soilpl101775 = Path(data_root_path, 'soilpl101755.json')
        self.soilpl101775_target = Path('/soilpl101775.json')
        self.fs.add_real_file(soilpl101775, target_path=self.soilpl101775_target)

        soilpl101775_properties = Path(data_root_path, 'soilpl101755_properties.json')
        self.soilpl101775_properties_target = Path('/soilpl101775_properties.json')
        self.fs.add_real_file(soilpl101775_properties, target_path=self.soilpl101775_properties_target)

        cfgloc101775 = Path(data_root_path, 'cfgloc101775.json')
        self.cfgloc101775_target = Path('/cfgloc101775.json')
        self.fs.add_real_file(cfgloc101775, target_path=self.cfgloc101775_target)

        cfgloc101775_properties = Path(data_root_path, 'cfgloc101775_properties.json')
        self.cfgloc101775_properties_target = Path('/cfgloc101775_properties.json')
        self.fs.add_real_file(cfgloc101775_properties, target_path=self.cfgloc101775_properties_target)

        cfgloc101777 = Path(data_root_path, 'cfgloc101777.json')
        self.cfgloc101777_target = Path('/cfgloc101777.json')
        self.fs.add_real_file(cfgloc101777, target_path=self.cfgloc101777_target)

        cfg101777_properties = Path(data_root_path, 'cfgloc101777_properties.json')
        self.cfgloc101777_properties_target = Path('/cfgloc_101777_properties.json')
        self.fs.add_real_file(cfg101777_properties, target_path=self.cfgloc101777_properties_target)

    def test_write(self) -> None:
        location_path = create_location_path(self.fs)  # create the path and add location file names.
        print(f'location_path: {location_path}')
        timestamp = get_timestamp()
        elements = PathElements(domain=self.domain,
                                site=self.site,
                                year=self.year,
                                month=self.month,
                                data_product_id=self.data_product_id)
        database = SensorPositionsDatabase(get_geolocations=self.get_geolocations,
                                           get_named_location=self.get_named_location,
                                           get_geometry=self.get_geometry)
        file_path = write_file(location_path=location_path,
                               out_path=self.out_path,
                               elements=elements,
                               timestamp=timestamp,
                               database=database)
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            i = 0
            for row in reader:
                i += 1
                print(row)
            assert i == 3  # header and 2 entries

    @staticmethod
    def get_property(json_property) -> Property:
        name = json_property['attr_name']
        value = json_property['string_value']
        return Property(name=name, value=value)

    @staticmethod
    def get_geometry(_named_location: str) -> Geometry:
        """Mock function to return the geometry for a location."""
        geometry = 'POLYGON Z ((-104.746013 40.815892 1654.009392,-104.745973 40.815922 1654.052064))'
        return build_geometry(geometry=geometry, srid=4979)

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
                    geo_location = self.get_geolocation(json_location)
                    locations.append(geo_location)
        if named_location == 'CFGLOC1017757':
            with open(self.cfgloc101777_target) as file:
                json_data = json.load(file)
                for json_location in json_data:
                    geo_location = self.get_geolocation(json_location)
                    locations.append(geo_location)
        if named_location == 'SOILPL101755':
            with open(self.soilpl101775_target) as file:
                json_data = json.load(file)
                for json_location in json_data:
                    geo_location = self.get_geolocation(json_location)
                    locations.append(geo_location)
        return locations

    def get_named_location(self, name) -> NamedLocation:
        properties = self.get_properties(name)
        if name == 'CFGLOC101775':
            return NamedLocation(location_id=138773,
                                 name='CFGLOC101775',
                                 description='Central Plains Soil Temp Profile SP2, Z6 Depth',
                                 properties=properties)
        if name == 'CFGLOC101777':
            return NamedLocation(location_id=138775,
                                 name='CFGLOC101777',
                                 description='Central Plains Soil Temp Profile SP2, Z7 Depth',
                                 properties=properties)
        if name == 'SOILPL101755':
            return NamedLocation(location_id=138753,
                                 name='SOILPL101755',
                                 description='Central Plains Soil Plot, SP2',
                                 properties=properties)


class _FakeLocation:
    """Minimal stand-in for the NamedLocation that create_tchain_rows inspects
    via `is_tchain_sensor(location)` -> checks for ThermistorDepth{i} properties."""
    def __init__(self, properties):
        self.properties = properties


class _FakeGeolocation:
    """Minimal stand-in — create_tchain_rows reads `.offset_name` before it hands
    the geolocation to add_reference_position_data_func."""
    def __init__(self, offset_name='REFLOC001'):
        self.offset_name = offset_name


class TchainRowShapeTest(TestCase):
    """create_tchain_rows produces one row per thermistor depth. When
    include_effective_dates is False (the default all pipelines hit today), the row
    shape matches the pre-change 22-column schema exactly. When True (the shape the
    follow-up DB-codepath effective-dates work will flip to), two blank cells appear
    between description and positionStart, aligning with get_column_names(True).
    """

    @staticmethod
    def _base_row_stub(_database, _geolocation, row_hor_ver, row_location_id, row_description):
        return {
            'row_hor_ver': row_hor_ver,
            'row_location_id': row_location_id,
            'row_description': row_description,
            'row_position_start_date': '2020-01-01T00:00:00Z',
            'row_position_end_date':   '',
            'row_x_offset': 0.0, 'row_y_offset': 0.0, 'row_z_offset': 0.0,
            'row_pitch': 0.0, 'row_roll': 0.0, 'row_azimuth': 0.0,
            'row_reference_location_id': 'REFLOC001',
            'row_reference_location_description': 'ref desc',
            'row_reference_location_latitude': 40.0,
            'row_reference_location_longitude': -104.0,
            'row_reference_location_elevation': 1500.0,
        }

    @staticmethod
    def _ref_position_stub(_database, base_data, _geolocation, _offset_name):
        row = dict(base_data)
        row.update({
            'row_x_azimuth': 0.0, 'row_y_azimuth': 0.0,
            'row_east_offset': 0.0, 'row_north_offset': 0.0,
            'row_reference_location_start_date': '2010-01-01T00:00:00Z',
            'row_reference_location_end_date':   '',
        })
        return [row]

    @staticmethod
    def _tchain_location():
        return _FakeLocation(properties=[
            Property(name='ThermistorDepth501', value='0.06'),
            Property(name='ThermistorDepth502', value='0.16'),
        ])

    def _run(self, include_effective_dates: bool):
        return create_tchain_rows(
            database=None, location=self._tchain_location(),
            geolocation=_FakeGeolocation(),
            row_hor_ver='000.010', row_location_id='CFGLOC000001',
            row_description='Test Soil Temp',
            create_base_row_data_func=self._base_row_stub,
            add_reference_position_data_func=self._ref_position_stub,
            include_effective_dates=include_effective_dates,
        )

    def test_default_flag_omits_effective_cells(self):
        rows = self._run(include_effective_dates=False)
        self.assertEqual(len(rows), 2)  # one per thermistor depth
        header = get_column_names(include_effective_dates=False)
        self.assertEqual(len(header), 22)
        # Thermistor ids substitute into the VER slot of HOR.VER
        self.assertEqual({row[0] for row in rows}, {'000.501', '000.502'})
        for row in rows:
            self.assertEqual(len(row), 22)
            # Position dates land right after description; no blank cells between.
            self.assertEqual(row[3], '2020-01-01T00:00:00Z')
            self.assertEqual(row[4], '')

    def test_flag_true_inserts_two_blank_cells_matching_header(self):
        rows = self._run(include_effective_dates=True)
        self.assertEqual(len(rows), 2)
        header = get_column_names(include_effective_dates=True)
        self.assertEqual(len(header), 24)
        self.assertEqual(header[3], 'effectiveStartDateTime')
        self.assertEqual(header[4], 'effectiveEndDateTime')
        self.assertEqual({row[0] for row in rows}, {'000.501', '000.502'})
        for row in rows:
            self.assertEqual(len(row), 24)
            # Two blank cells for effective land between description and positionStart.
            # DB codepath doesn't compute the intersection yet; follow-up PR fills these.
            self.assertEqual(row[3], '')
            self.assertEqual(row[4], '')
            self.assertEqual(row[5], '2020-01-01T00:00:00Z')  # positionStart
            self.assertEqual(row[6], '')                      # positionEnd
