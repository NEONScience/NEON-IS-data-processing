#!/usr/bin/env python3
"""
Unit tests for concH2oSoilSalinity_position_history_loader.

Cover the pieces the loader logic hinges on:
  - stream -> VER mapping
  - date-range intersection
  - MultiPoint / Point unwrap for reference-location geometry
  - authoritative-cal selection (highest calibration_id per (asset, stream))
  - future-date placeholder collapse
  - extent-based row merge (same physical position collapses across gaps)
  - end-to-end write_files against mocked PDR callables
"""
import json
from datetime import datetime, timezone
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from data_access.types.asset_install import AssetInstall
from data_access.types.cfgloc_ver import CfglocVer
from data_access.types.cvald1_calibration import Cvald1Calibration
from concH2oSoilSalinity_position_history_loader import (
    concH2oSoilSalinity_position_history_loader as loader,
)


def dt(*args) -> datetime:
    return datetime(*args)


class StreamToVerTest(TestCase):
    def test_maps_rawvswc0_to_501(self):
        self.assertEqual(loader._stream_to_ver('rawVSWC0'), 501)

    def test_maps_rawvswc7_to_508(self):
        self.assertEqual(loader._stream_to_ver('rawVSWC7'), 508)

    def test_returns_none_for_vsic(self):
        # VSIC odd streams are not in the naming pattern we expect
        self.assertIsNone(loader._stream_to_ver('rawVSIC0'))

    def test_returns_none_for_empty_and_junk(self):
        self.assertIsNone(loader._stream_to_ver(None))
        self.assertIsNone(loader._stream_to_ver(''))
        self.assertIsNone(loader._stream_to_ver('rawVSWC'))       # empty suffix
        self.assertIsNone(loader._stream_to_ver('rawVSWCX'))      # non-digit


class IntersectTest(TestCase):
    def test_overlapping_ranges(self):
        result = loader._intersect(
            (dt(2020, 1, 1), dt(2024, 1, 1)),
            (dt(2022, 1, 1), dt(2026, 1, 1)),
        )
        self.assertEqual(result, (dt(2022, 1, 1), dt(2024, 1, 1)))

    def test_open_ended_left(self):
        result = loader._intersect(
            (None, dt(2024, 1, 1)),
            (dt(2022, 1, 1), None),
        )
        self.assertEqual(result, (dt(2022, 1, 1), dt(2024, 1, 1)))

    def test_disjoint_returns_none(self):
        self.assertIsNone(
            loader._intersect(
                (dt(2020, 1, 1), dt(2021, 1, 1)),
                (dt(2022, 1, 1), dt(2023, 1, 1)),
            )
        )

    def test_touching_at_boundary_returns_none(self):
        # start >= end -> None (half-open)
        self.assertIsNone(
            loader._intersect(
                (dt(2020, 1, 1), dt(2022, 1, 1)),
                (dt(2022, 1, 1), dt(2024, 1, 1)),
            )
        )

    def test_strips_tz_before_compare(self):
        aware = datetime(2022, 1, 1, tzinfo=timezone.utc)
        naive = dt(2020, 1, 1)
        result = loader._intersect((naive, None), (aware, None))
        # tz stripped internally; earliest common start is the aware value w/o tz
        self.assertEqual(result[0], dt(2022, 1, 1))
        self.assertIsNone(result[1])


class ExtractPointTest(TestCase):
    def test_point_shape(self):
        lon, lat, elev = loader._extract_point(
            {'type': 'Point', 'coordinates': [-83.5, 35.68, 573.32]}
        )
        self.assertEqual((lon, lat, elev), (-83.5, 35.68, 573.32))

    def test_multipoint_unwrap_one_level(self):
        lon, lat, elev = loader._extract_point(
            {'type': 'MultiPoint', 'coordinates': [[-83.5, 35.68, 573.32]]}
        )
        self.assertEqual((lon, lat, elev), (-83.5, 35.68, 573.32))

    def test_missing_elevation(self):
        lon, lat, elev = loader._extract_point(
            {'type': 'Point', 'coordinates': [-83.5, 35.68]}
        )
        self.assertEqual(lon, -83.5)
        self.assertEqual(lat, 35.68)
        self.assertIsNone(elev)

    def test_none_and_empty(self):
        self.assertEqual(loader._extract_point(None), (None, None, None))
        self.assertEqual(
            loader._extract_point({'type': 'Point', 'coordinates': []}),
            (None, None, None),
        )


class MergeTimeRangesTest(TestCase):
    """`_merge_time_ranges` is the piece that keeps the CSV from listing every
    install/reinstall boundary as its own row when the physical position hasn't
    changed. The behavior we want is *extent-based*: min(start), max(end)
    across all rows in a group."""

    @staticmethod
    def _base_row(**overrides):
        row = {
            'hor': '004', 'ver': '501',
            '_win_start': None, '_win_end': None,
            'x_offset': 0.0, 'y_offset': 0.0, 'z_offset': -0.045,
            'pitch': 1.0, 'roll': 0.0, 'azimuth': 130.0,
            'reference_location_id': 'SOILPL999',
            'reference_location_start_date': None,
            'reference_location_end_date': None,
            'reference_location_latitude': 35.68,
            'reference_location_longitude': -83.50,
            'reference_location_elevation': 573.32,
            'asset_uid': 1,
            'cvald1_cm': 4.5,
            'source_stream': 'rawVSWC0',
            'cert_filename': 'A.xml',
        }
        row.update(overrides)
        return row

    def test_two_rows_same_position_collapse_across_gap(self):
        rows = [
            self._base_row(_win_start=dt(2020, 1, 1), _win_end=dt(2021, 6, 30)),
            self._base_row(_win_start=dt(2021, 7, 1), _win_end=dt(2024, 1, 1)),
        ]
        merged = loader._merge_time_ranges(rows)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]['position_start_date'], '2020-01-01T00:00:00Z')
        self.assertEqual(merged[0]['position_end_date'], '2024-01-01T00:00:00Z')

    def test_different_z_offset_stays_separate(self):
        rows = [
            self._base_row(_win_start=dt(2020, 1, 1), _win_end=dt(2021, 6, 30), z_offset=-0.045),
            self._base_row(_win_start=dt(2021, 7, 1), _win_end=dt(2024, 1, 1), z_offset=-0.055),
        ]
        merged = loader._merge_time_ranges(rows)
        self.assertEqual(len(merged), 2)
        z_values = sorted(r['z_offset'] for r in merged)
        self.assertEqual(z_values, [-0.055, -0.045])

    def test_open_start_and_open_end_map_to_blank(self):
        rows = [
            self._base_row(_win_start=None, _win_end=None),
        ]
        merged = loader._merge_time_ranges(rows)
        self.assertEqual(merged[0]['position_start_date'], '')
        self.assertEqual(merged[0]['position_end_date'], '')

    def test_provenance_from_earliest_sub_interval(self):
        # A merged row spans multiple asset installs; the provenance fields we
        # carry (asset_uid, cvald1_cm, cert_filename) come from the earliest row.
        # If we ever want per-window provenance, this test will catch the change.
        rows = [
            self._base_row(_win_start=dt(2020, 1, 1), _win_end=dt(2021, 6, 30),
                           asset_uid=100, cert_filename='OLD.xml'),
            self._base_row(_win_start=dt(2021, 7, 1), _win_end=dt(2024, 1, 1),
                           asset_uid=200, cert_filename='NEW.xml'),
        ]
        merged = loader._merge_time_ranges(rows)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]['asset_uid'], 100)
        self.assertEqual(merged[0]['cert_filename'], 'OLD.xml')

    def test_finalize_row_field_order(self):
        # position_start_date and position_end_date must come right after hor/ver
        # so the emitted JSON reads naturally. Everything else preserves input order.
        row = self._base_row(_win_start=dt(2020, 1, 1), _win_end=dt(2021, 6, 30))
        merged = loader._merge_time_ranges([row])
        keys = list(merged[0].keys())
        self.assertEqual(keys[:4], ['hor', 'ver', 'position_start_date', 'position_end_date'])
        self.assertNotIn('_win_start', keys)
        self.assertNotIn('_win_end', keys)


class BuildRowsTest(TestCase):
    """`_build_rows` is the (install × geo) intersection + cvald1-depth-adjust
    + future-date collapse. We stub out streams_by_asset so this exercises the
    row construction logic without needing the full PDR fake."""

    @staticmethod
    def _install(asset_uid=1, install_date=dt(2020, 1, 1), remove_date=None):
        return AssetInstall(
            cfgloc='CFGLOC999999',
            cfgloc_description='XYZ SP1',
            nam_locn_id=1,
            asset_uid=asset_uid,
            install_date=install_date,
            remove_date=remove_date,
        )

    @staticmethod
    def _geo(start=dt(2010, 1, 1), end=None, z=0.005):
        return {
            'start_date': start, 'end_date': end,
            'x_offset': -0.46, 'y_offset': -1.09, 'z_offset': z,
            'pitch': 1.0, 'roll': 0.0, 'azimuth': 130.0,
            'reference_location_id': 'SOILPL999',
            'reference_location_start_date': dt(2010, 1, 1),
            'reference_location_end_date': None,
            'reference_location_latitude': 35.68,
            'reference_location_longitude': -83.50,
            'reference_location_elevation': 573.32,
        }

    @staticmethod
    def _cal(asset_uid=1, cid=100, stream='rawVSWC0', cvald1=5.0):
        return Cvald1Calibration(
            asset_uid=asset_uid,
            calibration_id=cid,
            sensor_stream_num=0,
            schema_field_name=stream,
            valid_start_time=dt(2010, 1, 1),
            valid_end_time=None,
            cert_filename='CERT.xml',
            cvald1_cm=cvald1,
        )

    def test_z_offset_adjusted_by_cvald1(self):
        install = self._install(remove_date=dt(2024, 1, 1))
        geo = self._geo(z=0.008)
        rows = loader._build_rows(
            cfgloc_installs=[install],
            geolocations=[geo],
            streams_by_asset={1: [self._cal(cvald1=7.0)]},
            allowed_vers={'501'},
            hor='004',
            now_naive=dt(2026, 7, 8),
        )
        # 0.008 + 7/-100 = -0.062
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['z_offset'], -0.062)

    def test_future_end_date_collapses_to_blank(self):
        # PDR placeholder future dates (2031-12-31) should render as open-ended.
        install = self._install(remove_date=dt(2031, 12, 31))
        geo = self._geo(end=dt(2031, 12, 31))
        rows = loader._build_rows(
            cfgloc_installs=[install],
            geolocations=[geo],
            streams_by_asset={1: [self._cal()]},
            allowed_vers={'501'},
            hor='004',
            now_naive=dt(2026, 7, 8),
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['position_end_date'], '')

    def test_ver_not_in_allowed_set_is_skipped(self):
        install = self._install()
        geo = self._geo()
        rows = loader._build_rows(
            cfgloc_installs=[install],
            geolocations=[geo],
            streams_by_asset={1: [self._cal(stream='rawVSWC7')]},   # -> VER 508
            allowed_vers={'501', '502'},                             # 508 not allowed
            hor='004',
            now_naive=dt(2026, 7, 8),
        )
        self.assertEqual(rows, [])

    def test_asset_with_no_streams_is_skipped(self):
        install = self._install(asset_uid=999)
        rows = loader._build_rows(
            cfgloc_installs=[install],
            geolocations=[self._geo()],
            streams_by_asset={1: [self._cal()]},   # no key for 999
            allowed_vers={'501'},
            hor='004',
            now_naive=dt(2026, 7, 8),
        )
        self.assertEqual(rows, [])

    def test_disjoint_install_and_geo_produces_no_rows(self):
        install = self._install(install_date=dt(2020, 1, 1), remove_date=dt(2020, 6, 30))
        geo = self._geo(start=dt(2021, 1, 1), end=dt(2022, 1, 1))
        rows = loader._build_rows(
            cfgloc_installs=[install],
            geolocations=[geo],
            streams_by_asset={1: [self._cal()]},
            allowed_vers={'501'},
            hor='004',
            now_naive=dt(2026, 7, 8),
        )
        self.assertEqual(rows, [])

    def test_two_geos_two_streams_produces_four_rows(self):
        # Two geolocation windows × two allowed VSWC streams = four output rows,
        # each with its own z_offset depending on cvald1.
        install = self._install(remove_date=dt(2030, 1, 1))
        geos = [
            self._geo(start=dt(2010, 1, 1), end=dt(2023, 1, 1), z=0.005),
            self._geo(start=dt(2023, 1, 1), end=None,           z=0.008),
        ]
        streams = [
            self._cal(cid=1, stream='rawVSWC0', cvald1=5.0),
            self._cal(cid=2, stream='rawVSWC1', cvald1=15.0),  # VER 502
        ]
        rows = loader._build_rows(
            cfgloc_installs=[install],
            geolocations=geos,
            streams_by_asset={1: streams},
            allowed_vers={'501', '502'},
            hor='004',
            now_naive=dt(2026, 7, 8),
        )
        # 2 geos × 2 streams = 4 unique (position, ver) tuples -> 4 merged groups
        self.assertEqual(len(rows), 4)
        vers = sorted(r['ver'] for r in rows)
        self.assertEqual(vers, ['501', '501', '502', '502'])


class AuthoritativeCalSelectionTest(TestCase):
    """`write_files` reduces multiple Cvald1Calibration rows for the same
    (asset_uid, schema_field_name) to the single row with the highest
    calibration_id. This is Option B: the newer-inserted cert wins across the
    whole install window; older/phantom records don't slice output."""

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/out')
        self.err_path = Path('/err')
        self.fs.create_dir(self.out_path)
        self.fs.create_dir(self.err_path)

    def _run_loader(self, calibrations):
        installs = [AssetInstall(
            cfgloc='CFGLOC105360', cfgloc_description='BONA SP1',
            nam_locn_id=105360, asset_uid=46446,
            install_date=dt(2020, 3, 11), remove_date=None,
        )]
        cfgloc_vers = [CfglocVer(cfgloc='CFGLOC105360', hor='004', ver='501',
                                 group_name='conc-h2o-soil-salinity-split_BONA004501')]
        geolocations = _feature_collection([_feature(x_offset=0.0, y_offset=0.0, z_offset=0.005,
                                                     start_date='2020-03-11T00:00:00Z')])
        loader.write_files(
            get_asset_installs=lambda: installs,
            get_calibrations=lambda: calibrations,
            get_cfgloc_vers=lambda: cfgloc_vers,
            get_geolocations=lambda _nam_id: geolocations,
            get_parents=lambda _nam_id: {'site': (1, 'BONA')},
            out_path=self.out_path,
            err_path=self.err_path,
            generated_at=datetime(2026, 7, 8, tzinfo=timezone.utc),
        )
        json_path = self.out_path / 'enviroscan' / 'CFGLOC105360' / 'position_history' / 'CFGLOC105360_history.json'
        with open(json_path) as fp:
            return json.load(fp)

    def test_highest_calibration_id_wins(self):
        # Real scenario from CFGLOC105360: two cal records for the same
        # (asset, stream). The older one has cvald1=5.0 (correct value), the newer
        # one has cvald1=99.0. Highest calibration_id must win.
        calibrations = [
            Cvald1Calibration(asset_uid=46446, calibration_id=100,
                              sensor_stream_num=0, schema_field_name='rawVSWC0',
                              valid_start_time=dt(2020, 2, 19), valid_end_time=dt(2021, 4, 14),
                              cert_filename='OLD.xml', cvald1_cm=5.0),
            Cvald1Calibration(asset_uid=46446, calibration_id=200,
                              sensor_stream_num=0, schema_field_name='rawVSWC0',
                              valid_start_time=dt(2021, 10, 13), valid_end_time=dt(2031, 12, 31),
                              cert_filename='NEW.xml', cvald1_cm=99.0),
        ]
        payload = self._run_loader(calibrations)
        # 0.005 + 99/-100 = -0.985
        self.assertEqual(len(payload['rows']), 1)
        self.assertEqual(payload['rows'][0]['cvald1_cm'], 99.0)
        self.assertEqual(payload['rows'][0]['z_offset'], -0.985)
        self.assertEqual(payload['rows'][0]['cert_filename'], 'NEW.xml')

    def test_single_row_per_stream_produces_one_span(self):
        # Two cal records with the same cvald1 value must not slice the output.
        # Pre-Option-B this would produce two rows split on the cal boundary.
        calibrations = [
            Cvald1Calibration(asset_uid=46446, calibration_id=100,
                              sensor_stream_num=0, schema_field_name='rawVSWC0',
                              valid_start_time=dt(2020, 2, 19), valid_end_time=dt(2021, 4, 14),
                              cert_filename='OLD.xml', cvald1_cm=5.0),
            Cvald1Calibration(asset_uid=46446, calibration_id=200,
                              sensor_stream_num=0, schema_field_name='rawVSWC0',
                              valid_start_time=dt(2021, 10, 13), valid_end_time=dt(2031, 12, 31),
                              cert_filename='NEW.xml', cvald1_cm=5.0),
        ]
        payload = self._run_loader(calibrations)
        self.assertEqual(len(payload['rows']), 1)
        self.assertEqual(payload['rows'][0]['position_end_date'], '')  # future end -> blank


class EndToEndWriteFilesTest(TestCase):
    """Full loader run over a synthetic CFGLOC with two geolocation windows
    and one asset install. Verifies the emitted JSON has the shape pub_files
    expects and matches the design doc's worked scenario."""

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/out')
        self.err_path = Path('/err')
        self.fs.create_dir(self.out_path)
        self.fs.create_dir(self.err_path)

    def test_writes_one_json_per_cfgloc_with_expected_structure(self):
        installs = [AssetInstall(
            cfgloc='CFGLOC999999', cfgloc_description='XYZ SP1 EnviroSCAN',
            nam_locn_id=42, asset_uid=12294,
            install_date=dt(2010, 1, 1), remove_date=None,
        )]
        cfgloc_vers = [
            CfglocVer(cfgloc='CFGLOC999999', hor='004', ver='501',
                      group_name='conc-h2o-soil-salinity-split_XYZ004501'),
        ]
        calibrations = [
            Cvald1Calibration(asset_uid=12294, calibration_id=1,
                              sensor_stream_num=0, schema_field_name='rawVSWC0',
                              valid_start_time=dt(2010, 1, 1), valid_end_time=None,
                              cert_filename='A.xml', cvald1_cm=5.0),
        ]
        geolocations = _feature_collection([
            _feature(start_date='2010-01-01T00:00:00Z',
                     end_date='2023-01-19T00:00:00Z',
                     x_offset=-0.46, y_offset=-1.09, z_offset=0.005),
            _feature(start_date='2023-01-19T00:00:00Z',
                     end_date=None,
                     x_offset=2.36, y_offset=-1.13, z_offset=0.008),
        ])
        loader.write_files(
            get_asset_installs=lambda: installs,
            get_calibrations=lambda: calibrations,
            get_cfgloc_vers=lambda: cfgloc_vers,
            get_geolocations=lambda _id: geolocations,
            get_parents=lambda _id: {'site': (1, 'XYZ')},
            out_path=self.out_path,
            err_path=self.err_path,
            generated_at=datetime(2026, 7, 8, tzinfo=timezone.utc),
        )

        json_path = self.out_path / 'enviroscan' / 'CFGLOC999999' / 'position_history' / 'CFGLOC999999_history.json'
        self.assertTrue(json_path.is_file())
        payload = json.loads(json_path.read_text())

        self.assertEqual(payload['source_type'], 'enviroscan')
        self.assertEqual(payload['cfgloc'], 'CFGLOC999999')
        self.assertEqual(payload['site'], 'XYZ')
        self.assertEqual(payload['generated_at'], '2026-07-08T00:00:00Z')
        # Two distinct z-offset positions -> two rows after merge
        rows = payload['rows']
        self.assertEqual(len(rows), 2)
        z_values = sorted(r['z_offset'] for r in rows)
        # 0.005 + 5/-100 = -0.045; 0.008 + 5/-100 = -0.042
        self.assertEqual(z_values, [-0.045, -0.042])
        for row in rows:
            self.assertEqual(row['hor'], '004')
            self.assertEqual(row['ver'], '501')
            self.assertIn('position_start_date', row)
            self.assertIn('position_end_date', row)

    def test_cfgloc_with_no_configured_ver_is_skipped(self):
        installs = [AssetInstall(
            cfgloc='CFGLOC_NOVER', cfgloc_description='decommissioned',
            nam_locn_id=99, asset_uid=1,
            install_date=dt(2010, 1, 1), remove_date=dt(2015, 1, 1),
        )]
        calibrations = [Cvald1Calibration(
            asset_uid=1, calibration_id=1, sensor_stream_num=0,
            schema_field_name='rawVSWC0', valid_start_time=dt(2010, 1, 1),
            valid_end_time=None, cert_filename='A.xml', cvald1_cm=5.0,
        )]
        loader.write_files(
            get_asset_installs=lambda: installs,
            get_calibrations=lambda: calibrations,
            get_cfgloc_vers=lambda: [],   # nothing configured -> skip
            get_geolocations=lambda _id: _feature_collection([]),
            get_parents=lambda _id: {'site': (1, 'XYZ')},
            out_path=self.out_path,
            err_path=self.err_path,
            generated_at=datetime(2026, 7, 8, tzinfo=timezone.utc),
        )
        # No JSON should be written for a CFGLOC missing from Q4
        self.assertFalse(
            (self.out_path / 'enviroscan' / 'CFGLOC_NOVER').exists()
        )


# --- helpers -----------------------------------------------------------------

def _feature(*, start_date, end_date=None, x_offset=0.0, y_offset=0.0, z_offset=0.0,
             alpha=1.0, beta=0.0, gamma=130.0):
    """Build a Feature dict shaped like get_named_location_geolocations' output."""
    return {
        'type': 'Feature',
        'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0, 0.0]},
        'properties': {
            'start_date': start_date, 'end_date': end_date,
            'alpha': alpha, 'beta': beta, 'gamma': gamma,
            'x_offset': x_offset, 'y_offset': y_offset, 'z_offset': z_offset,
            'reference_location': {
                'type': 'Feature', 'geometry': None,
                'properties': {
                    'name': 'SOILPL999',
                    'locations': {
                        'type': 'FeatureCollection',
                        'features': [{
                            'type': 'Feature',
                            'geometry': {'type': 'Point', 'coordinates': [-83.5, 35.68, 573.32]},
                            'properties': {'start_date': '2010-01-01T00:00:00Z', 'end_date': None},
                        }],
                    },
                },
            },
        },
    }


def _feature_collection(features):
    return {'type': 'FeatureCollection', 'features': features}
