#!/usr/bin/env python3
"""
Unit tests for concH2oSoilSalinity_position_history_loader.

Cover the pieces the loader logic hinges on:
  - stream -> VER mapping
  - date-range intersection
  - MultiPoint / Point unwrap for reference-location geometry
  - install-window-scoped cal selection (overlap between cal valid period and
    this install; in-force-at-install-start preferred, tiebreak by cal_id)
  - future-date placeholder collapse
  - adjacency-based row merge (consecutive same-position rows collapse;
    an intervening different-position row breaks the run)
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
    """`_merge_time_ranges` walks each (HOR, VER) chronologically and collapses
    consecutive same-position rows; a different-position row breaks the run.
    This preserves the sensor_positions invariant that per-VER windows don't
    overlap, and that an ABA sequence (A, then B, then A again) surfaces as
    three rows instead of one merged A."""

    @staticmethod
    def _base_row(**overrides):
        row = {
            'hor': '004', 'ver': '501',
            '_eff_start': None, '_eff_end': None,
            '_pos_start': None, '_pos_end': None,
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
        # Default position window to match effective when not explicitly overridden.
        for eff_key, pos_key in (('_eff_start', '_pos_start'), ('_eff_end', '_pos_end')):
            if pos_key not in overrides and eff_key in overrides:
                overrides[pos_key] = overrides[eff_key]
        row.update(overrides)
        return row

    def test_two_rows_same_position_collapse_across_gap(self):
        rows = [
            self._base_row(_eff_start=dt(2020, 1, 1), _eff_end=dt(2021, 6, 30)),
            self._base_row(_eff_start=dt(2021, 7, 1), _eff_end=dt(2024, 1, 1)),
        ]
        merged = loader._merge_time_ranges(rows)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]['effective_start_date'], '2020-01-01T00:00:00Z')
        self.assertEqual(merged[0]['effective_end_date'], '2024-01-01T00:00:00Z')
        self.assertEqual(merged[0]['position_start_date'], '2020-01-01T00:00:00Z')
        self.assertEqual(merged[0]['position_end_date'], '2024-01-01T00:00:00Z')

    def test_different_z_offset_stays_separate(self):
        rows = [
            self._base_row(_eff_start=dt(2020, 1, 1), _eff_end=dt(2021, 6, 30), z_offset=-0.045),
            self._base_row(_eff_start=dt(2021, 7, 1), _eff_end=dt(2024, 1, 1), z_offset=-0.055),
        ]
        merged = loader._merge_time_ranges(rows)
        self.assertEqual(len(merged), 2)
        z_values = sorted(r['z_offset'] for r in merged)
        self.assertEqual(z_values, [-0.055, -0.045])

    def test_aba_sequence_stays_three_rows(self):
        # Position A -> B -> A must NOT collapse into a single A row spanning
        # the whole timeline; the intermediate B breaks the run, so we emit
        # three chronologically ordered rows. This is what protects the
        # cross-CFGLOC GRSM case: a stale cvald1 that briefly picks a different
        # z_offset can't retroactively fuse the neighboring true-A periods.
        rows = [
            self._base_row(_eff_start=dt(2020, 1, 1), _eff_end=dt(2021, 1, 1), z_offset=-0.045),
            self._base_row(_eff_start=dt(2021, 1, 1), _eff_end=dt(2022, 1, 1), z_offset=-0.145),
            self._base_row(_eff_start=dt(2022, 1, 1), _eff_end=dt(2023, 1, 1), z_offset=-0.045),
        ]
        merged = loader._merge_time_ranges(rows)
        self.assertEqual(len(merged), 3)
        z_values = [r['z_offset'] for r in merged]
        self.assertEqual(z_values, [-0.045, -0.145, -0.045])
        # Verify chronological order in output
        starts = [r['effective_start_date'] for r in merged]
        self.assertEqual(starts, ['2020-01-01T00:00:00Z',
                                  '2021-01-01T00:00:00Z',
                                  '2022-01-01T00:00:00Z'])

    def test_open_start_and_open_end_map_to_blank(self):
        rows = [
            self._base_row(_eff_start=None, _eff_end=None),
        ]
        merged = loader._merge_time_ranges(rows)
        self.assertEqual(merged[0]['effective_start_date'], '')
        self.assertEqual(merged[0]['effective_end_date'], '')
        self.assertEqual(merged[0]['position_start_date'], '')
        self.assertEqual(merged[0]['position_end_date'], '')

    def test_provenance_from_earliest_sub_interval(self):
        # A merged row spans multiple asset installs; the provenance fields we
        # carry (asset_uid, cvald1_cm, cert_filename) come from the earliest row.
        # If we ever want per-window provenance, this test will catch the change.
        rows = [
            self._base_row(_eff_start=dt(2020, 1, 1), _eff_end=dt(2021, 6, 30),
                           asset_uid=100, cert_filename='OLD.xml'),
            self._base_row(_eff_start=dt(2021, 7, 1), _eff_end=dt(2024, 1, 1),
                           asset_uid=200, cert_filename='NEW.xml'),
        ]
        merged = loader._merge_time_ranges(rows)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]['asset_uid'], 100)
        self.assertEqual(merged[0]['cert_filename'], 'OLD.xml')

    def test_finalize_row_field_order(self):
        # The three date sets come right after hor/ver in effective/position order so
        # the emitted JSON reads naturally. Everything else preserves input order.
        row = self._base_row(_eff_start=dt(2020, 1, 1), _eff_end=dt(2021, 6, 30))
        merged = loader._merge_time_ranges([row])
        keys = list(merged[0].keys())
        self.assertEqual(keys[:6], ['hor', 'ver',
                                     'effective_start_date', 'effective_end_date',
                                     'position_start_date', 'position_end_date'])
        self.assertNotIn('_eff_start', keys)
        self.assertNotIn('_eff_end', keys)
        self.assertNotIn('_pos_start', keys)
        self.assertNotIn('_pos_end', keys)

    def test_ref_location_change_splits_row_but_shares_position_dates(self):
        # Same install/position at same offsets, but the reference location has two
        # time slices (e.g. elevation shifted a few cm at 2024-06-25). The merge must
        # emit two rows keyed on ref-locn identity, each carrying its own effective
        # and refLocn dates. Both rows share the same position dates — the sensor
        # never moved. This is the enviroscan analogue of the RMNP tempSoil case
        # Teresa's boss designed the effective schema around.
        rows = [
            self._base_row(_eff_start=dt(2022, 5, 5), _eff_end=dt(2024, 6, 25),
                           _pos_start=dt(2022, 5, 5), _pos_end=None,
                           reference_location_start_date='2010-01-01T00:00:00Z',
                           reference_location_end_date='2024-06-25T00:00:00Z',
                           reference_location_elevation=100.00),
            self._base_row(_eff_start=dt(2024, 6, 25), _eff_end=None,
                           _pos_start=dt(2022, 5, 5), _pos_end=None,
                           reference_location_start_date='2024-06-25T00:00:00Z',
                           reference_location_end_date=None,
                           reference_location_elevation=100.05),
        ]
        merged = loader._merge_time_ranges(rows)
        self.assertEqual(len(merged), 2)
        # Both rows show the same position dates — the position never changed
        self.assertEqual(merged[0]['position_start_date'], '2022-05-05T00:00:00Z')
        self.assertEqual(merged[0]['position_end_date'], '')
        self.assertEqual(merged[1]['position_start_date'], '2022-05-05T00:00:00Z')
        self.assertEqual(merged[1]['position_end_date'], '')
        # Effective and refLocn dates split at the elevation-change boundary
        self.assertEqual(merged[0]['effective_start_date'], '2022-05-05T00:00:00Z')
        self.assertEqual(merged[0]['effective_end_date'],   '2024-06-25T00:00:00Z')
        self.assertEqual(merged[1]['effective_start_date'], '2024-06-25T00:00:00Z')
        self.assertEqual(merged[1]['effective_end_date'],   '')
        self.assertEqual(merged[0]['reference_location_elevation'], 100.00)
        self.assertEqual(merged[1]['reference_location_elevation'], 100.05)

    def test_same_key_extends_position_across_install_gap(self):
        # Two rows same offsets and same ref-locn identity but with an install gap
        # between them (adjacent installs of the same asset). The merged row's
        # position dates extend from the earliest install start to the latest end,
        # matching effective. This is how a consecutive-install run collapses.
        rows = [
            self._base_row(_eff_start=dt(2020, 1, 1), _eff_end=dt(2021, 6, 30),
                           _pos_start=dt(2020, 1, 1), _pos_end=dt(2021, 6, 30)),
            self._base_row(_eff_start=dt(2021, 7, 1), _eff_end=dt(2024, 1, 1),
                           _pos_start=dt(2021, 7, 1), _pos_end=dt(2024, 1, 1)),
        ]
        merged = loader._merge_time_ranges(rows)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]['position_start_date'], '2020-01-01T00:00:00Z')
        self.assertEqual(merged[0]['position_end_date'],   '2024-01-01T00:00:00Z')
        self.assertEqual(merged[0]['effective_start_date'], '2020-01-01T00:00:00Z')
        self.assertEqual(merged[0]['effective_end_date'],   '2024-01-01T00:00:00Z')


class BuildRowsTest(TestCase):
    """`_build_rows` is the (install × geo) intersection + per-install cvald1
    selection + depth-adjust + future-date collapse. We pass explicit cal maps
    so each test can exercise a different selection scenario."""

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
    def _cal(asset_uid=1, cid=100, stream='rawVSWC0', cvald1=5.0,
             valid_start=dt(2010, 1, 1), valid_end=None):
        return Cvald1Calibration(
            asset_uid=asset_uid,
            calibration_id=cid,
            sensor_stream_num=0,
            schema_field_name=stream,
            valid_start_time=valid_start,
            valid_end_time=valid_end,
            cert_filename='CERT.xml',
            cvald1_cm=cvald1,
        )

    @classmethod
    def _cal_map(cls, cals, streams_by_asset=None):
        """Group a flat cal list into cals_by_asset_stream + streams_by_asset."""
        from collections import defaultdict
        cals_by_asset_stream = defaultdict(list)
        for c in cals:
            cals_by_asset_stream[(c.asset_uid, c.schema_field_name)].append(c)
        if streams_by_asset is None:
            streams_by_asset = defaultdict(list)
            for (asset_uid, stream_name) in cals_by_asset_stream.keys():
                streams_by_asset[asset_uid].append(stream_name)
        return dict(cals_by_asset_stream), dict(streams_by_asset)

    def test_z_offset_adjusted_by_cvald1(self):
        install = self._install(remove_date=dt(2024, 1, 1))
        geo = self._geo(z=0.008)
        cbas, sba = self._cal_map([self._cal(cvald1=7.0)])
        rows = loader._build_rows(
            cfgloc='CFGLOC999999',
            cfgloc_installs=[install],
            geolocations=[geo],
            cals_by_asset_stream=cbas,
            streams_by_asset=sba,
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
        cbas, sba = self._cal_map([self._cal()])
        rows = loader._build_rows(
            cfgloc='CFGLOC999999',
            cfgloc_installs=[install],
            geolocations=[geo],
            cals_by_asset_stream=cbas,
            streams_by_asset=sba,
            allowed_vers={'501'},
            hor='004',
            now_naive=dt(2026, 7, 8),
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['position_end_date'], '')

    def test_ver_not_in_allowed_set_is_skipped(self):
        install = self._install()
        geo = self._geo()
        cbas, sba = self._cal_map([self._cal(stream='rawVSWC7')])   # -> VER 508
        rows = loader._build_rows(
            cfgloc='CFGLOC999999',
            cfgloc_installs=[install],
            geolocations=[geo],
            cals_by_asset_stream=cbas,
            streams_by_asset=sba,
            allowed_vers={'501', '502'},                             # 508 not allowed
            hor='004',
            now_naive=dt(2026, 7, 8),
        )
        self.assertEqual(rows, [])

    def test_asset_with_no_streams_is_skipped(self):
        install = self._install(asset_uid=999)
        cbas, sba = self._cal_map([self._cal()])
        rows = loader._build_rows(
            cfgloc='CFGLOC999999',
            cfgloc_installs=[install],
            geolocations=[self._geo()],
            cals_by_asset_stream=cbas,
            streams_by_asset=sba,   # no key for 999
            allowed_vers={'501'},
            hor='004',
            now_naive=dt(2026, 7, 8),
        )
        self.assertEqual(rows, [])

    def test_disjoint_install_and_geo_produces_no_rows(self):
        install = self._install(install_date=dt(2020, 1, 1), remove_date=dt(2020, 6, 30))
        geo = self._geo(start=dt(2021, 1, 1), end=dt(2022, 1, 1))
        cbas, sba = self._cal_map([self._cal()])
        rows = loader._build_rows(
            cfgloc='CFGLOC999999',
            cfgloc_installs=[install],
            geolocations=[geo],
            cals_by_asset_stream=cbas,
            streams_by_asset=sba,
            allowed_vers={'501'},
            hor='004',
            now_naive=dt(2026, 7, 8),
        )
        self.assertEqual(rows, [])

    def test_install_with_no_overlapping_cal_produces_no_rows(self):
        # This is the cross-CFGLOC bug scenario in miniature: install period
        # (2017 -> 2018) and only cal record is valid 2020 -> 2021. No overlap
        # -> skip that install/stream. Previously Option B would silently apply
        # the mismatched cal.
        install = self._install(install_date=dt(2017, 4, 24), remove_date=dt(2018, 7, 5))
        geo = self._geo(start=dt(2010, 1, 1))
        cbas, sba = self._cal_map([self._cal(valid_start=dt(2020, 11, 4),
                                              valid_end=dt(2021, 12, 29),
                                              cvald1=166.0)])
        rows = loader._build_rows(
            cfgloc='CFGLOC999999',
            cfgloc_installs=[install],
            geolocations=[geo],
            cals_by_asset_stream=cbas,
            streams_by_asset=sba,
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
        cbas, sba = self._cal_map([
            self._cal(cid=1, stream='rawVSWC0', cvald1=5.0),
            self._cal(cid=2, stream='rawVSWC1', cvald1=15.0),  # VER 502
        ])
        rows = loader._build_rows(
            cfgloc='CFGLOC999999',
            cfgloc_installs=[install],
            geolocations=geos,
            cals_by_asset_stream=cbas,
            streams_by_asset=sba,
            allowed_vers={'501', '502'},
            hor='004',
            now_naive=dt(2026, 7, 8),
        )
        # 2 geos × 2 streams = 4 unique (position, ver) tuples -> 4 merged groups
        self.assertEqual(len(rows), 4)
        vers = sorted(r['ver'] for r in rows)
        self.assertEqual(vers, ['501', '501', '502', '502'])


class SelectCalForInstallTest(TestCase):
    """Direct tests for `_select_cal_for_install`: the install-window-scoped
    picker that replaced Option B. The rule is: pick the cal whose valid
    period overlaps [install_start, install_end], preferring the one in
    force at install_start (latest valid_start ≤ install_start), then
    falling back to earliest-overlapping. Tiebreak by highest calibration_id.
    Return None if no overlap — the caller then skips that install/stream.
    """

    @staticmethod
    def _cal(cid, valid_start, valid_end, cvald1=86.0):
        return Cvald1Calibration(
            asset_uid=1, calibration_id=cid, sensor_stream_num=0,
            schema_field_name='rawVSWC0',
            valid_start_time=valid_start, valid_end_time=valid_end,
            cert_filename=f'CERT_{cid}.xml', cvald1_cm=cvald1,
        )

    def test_no_overlap_returns_none(self):
        # Cross-CFGLOC scenario: install (2017 -> 2018), cal (2020 -> 2021).
        # This is exactly what let asset 40784's San Joaquin cert leak into
        # its earlier SP5 install under Option B.
        cals = [self._cal(1113348, dt(2020, 11, 4), dt(2021, 12, 29), cvald1=166.0)]
        result = loader._select_cal_for_install(cals, dt(2017, 4, 24), dt(2018, 7, 5))
        self.assertIsNone(result)

    def test_single_overlapping_cal_wins(self):
        # Real 40784 SP5 case: only cal 1080335 (valid 2017-03 -> 2018-11)
        # overlaps install (2017-04 -> 2018-07). Others don't.
        cals = [
            self._cal(1018291, dt(2018, 9, 13), dt(2020, 5, 14), cvald1=186.0),
            self._cal(1080335, dt(2017, 3, 31), dt(2018, 11, 30), cvald1=86.0),
            self._cal(1113348, dt(2020, 11, 4), dt(2021, 12, 29), cvald1=166.0),
        ]
        result = loader._select_cal_for_install(cals, dt(2017, 4, 24), dt(2018, 7, 5))
        self.assertIsNotNone(result)
        self.assertEqual(result.calibration_id, 1080335)
        self.assertEqual(result.cvald1_cm, 86.0)

    def test_in_force_at_install_start_beats_later_overlapping(self):
        # 46446 SP5 case: install (2020-03-11 -> open), three cals overlap
        # (one starts before install, two after). The one in force at install
        # start wins regardless of calibration_id.
        cals = [
            self._cal(1096994, dt(2020, 2, 19), dt(2021, 4, 14), cvald1=86.0),  # in force
            self._cal(1140848, dt(2021, 10, 13), dt(2031, 12, 31), cvald1=86.0),
            self._cal(1201723, dt(2021, 10, 13), dt(2031, 12, 31), cvald1=86.0),
        ]
        result = loader._select_cal_for_install(cals, dt(2020, 3, 11), None)
        self.assertEqual(result.calibration_id, 1096994)

    def test_tiebreak_by_highest_calibration_id_when_multiple_in_force(self):
        # 42820 SP5 case: two cals with the same valid_start both in force at
        # install start. Tiebreak picks the higher calibration_id.
        cals = [
            self._cal(1028951, dt(2019, 4, 4), dt(2020, 5, 28), cvald1=86.0),
            self._cal(1034725, dt(2019, 4, 4), dt(2020, 4, 2), cvald1=86.0),
            self._cal(1080563, dt(2017, 6, 20), dt(2019, 2, 19), cvald1=136.0),  # no overlap
        ]
        result = loader._select_cal_for_install(cals, dt(2019, 4, 22), dt(2020, 3, 11))
        self.assertEqual(result.calibration_id, 1034725)

    def test_no_in_force_falls_back_to_earliest_overlapping(self):
        # Install starts before any cal, but a cal starts inside the install
        # window. Nothing is "in force at install start" -> earliest-overlapping.
        cals = [
            self._cal(20, dt(2022, 6, 1), dt(2023, 6, 1), cvald1=86.0),  # earliest overlap
            self._cal(30, dt(2022, 9, 1), dt(2024, 1, 1), cvald1=86.0),
        ]
        result = loader._select_cal_for_install(cals, dt(2022, 1, 1), dt(2024, 1, 1))
        self.assertEqual(result.calibration_id, 20)


class WriteFilesCalSelectionTest(TestCase):
    """End-to-end verification that `write_files` threads install-window-scoped
    cal selection all the way through: given multiple cals for the same
    (asset, stream), the output row's cvald1 comes from the one whose valid
    period overlaps the install period, NOT the globally highest calibration_id."""

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/out')
        self.err_path = Path('/err')
        self.fs.create_dir(self.out_path)
        self.fs.create_dir(self.err_path)

    def _run_loader(self, calibrations, install_start, install_end=None):
        installs = [AssetInstall(
            cfgloc='CFGLOC105360', cfgloc_description='GRSM SP5',
            nam_locn_id=105360, asset_uid=40784,
            install_date=install_start, remove_date=install_end,
        )]
        cfgloc_vers = [CfglocVer(cfgloc='CFGLOC105360', hor='005', ver='501',
                                 group_name='conc-h2o-soil-salinity-split_GRSM005501')]
        geolocations = _feature_collection([_feature(x_offset=0.0, y_offset=0.0, z_offset=0.005,
                                                     start_date='2010-01-01T00:00:00Z')])
        loader.write_files(
            get_asset_installs=lambda: installs,
            get_calibrations=lambda: calibrations,
            get_cfgloc_vers=lambda: cfgloc_vers,
            get_geolocations=lambda _nam_id: geolocations,
            get_parents=lambda _nam_id: {'site': (1, 'GRSM')},
            out_path=self.out_path,
            err_path=self.err_path,
            generated_at=datetime(2026, 7, 8, tzinfo=timezone.utc),
        )
        json_path = self.out_path / 'enviroscan' / 'CFGLOC105360' / 'position_history' / 'CFGLOC105360_history.json'
        if not json_path.exists():
            return None
        with open(json_path) as fp:
            return json.load(fp)

    def test_cross_deployment_cal_is_not_picked(self):
        # Faithful reproduction of the GRSM asset 40784 bug: only cal in PDR
        # is from its LATER San Joaquin deployment (2020-11 -> 2021-12), but
        # the install at SP5 is 2017-04 -> 2018-07. Under the fix, no cal
        # overlaps the SP5 install -> no output row (safer than emitting the
        # physically impossible z_offset=-1.66 the old Option B produced).
        cals = [Cvald1Calibration(
            asset_uid=40784, calibration_id=1113348, sensor_stream_num=0,
            schema_field_name='rawVSWC0',
            valid_start_time=dt(2020, 11, 4), valid_end_time=dt(2021, 12, 29),
            cert_filename='SANJOAQUIN.xml', cvald1_cm=166.0,
        )]
        payload = self._run_loader(cals,
                                    install_start=dt(2017, 4, 24),
                                    install_end=dt(2018, 7, 5))
        # Loader skipped this install/stream -> no rows -> no JSON emitted
        self.assertIsNone(payload)

    def test_in_force_cal_beats_higher_id_out_of_window(self):
        # Two cals for the same (asset, stream). The older one is in force at
        # install start; the newer one has a higher calibration_id but is from
        # a later deployment. The in-force cal must win.
        cals = [
            Cvald1Calibration(
                asset_uid=40784, calibration_id=1080335, sensor_stream_num=0,
                schema_field_name='rawVSWC0',
                valid_start_time=dt(2017, 3, 31), valid_end_time=dt(2018, 11, 30),
                cert_filename='SP5.xml', cvald1_cm=86.0,
            ),
            Cvald1Calibration(
                asset_uid=40784, calibration_id=1113348, sensor_stream_num=0,
                schema_field_name='rawVSWC0',
                valid_start_time=dt(2020, 11, 4), valid_end_time=dt(2021, 12, 29),
                cert_filename='SANJOAQUIN.xml', cvald1_cm=166.0,
            ),
        ]
        payload = self._run_loader(cals,
                                    install_start=dt(2017, 4, 24),
                                    install_end=dt(2018, 7, 5))
        self.assertIsNotNone(payload)
        self.assertEqual(len(payload['rows']), 1)
        # 0.005 + 86/-100 = -0.855 — the SP5-appropriate value, not -1.655
        self.assertEqual(payload['rows'][0]['cvald1_cm'], 86.0)
        self.assertEqual(payload['rows'][0]['z_offset'], -0.855)
        self.assertEqual(payload['rows'][0]['cert_filename'], 'SP5.xml')


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

    def test_multi_ref_feat_splits_effective_but_shares_position(self):
        # One install, one cfgloc-geo, two ref-locn time slices (e.g. an elevation
        # revision at 2024-06-25). Loader must emit two rows: same position dates
        # (the sensor never moved), different effective + refLocn dates.
        installs = [AssetInstall(
            cfgloc='CFGLOC777777', cfgloc_description='XYZ SP2 EnviroSCAN',
            nam_locn_id=77, asset_uid=99,
            install_date=dt(2022, 5, 5), remove_date=None,
        )]
        cfgloc_vers = [
            CfglocVer(cfgloc='CFGLOC777777', hor='004', ver='508',
                      group_name='conc-h2o-soil-salinity-split_XYZ004508'),
        ]
        calibrations = [
            Cvald1Calibration(asset_uid=99, calibration_id=1,
                              sensor_stream_num=7, schema_field_name='rawVSWC7',
                              valid_start_time=dt(2020, 1, 1), valid_end_time=None,
                              cert_filename='CERT.xml', cvald1_cm=86.0),
        ]
        geolocations = _feature_collection([
            _feature(start_date='2010-01-01T00:00:00Z', end_date=None,
                     x_offset=0.0, y_offset=0.0, z_offset=0.005,
                     ref_feats=[
                         {'start_date': '2010-01-01T00:00:00Z',
                          'end_date':   '2024-06-25T00:00:00Z',
                          'lon': -105.545955, 'lat': 40.275903, 'elev': 2741.57},
                         {'start_date': '2024-06-25T00:00:00Z',
                          'end_date':   None,
                          'lon': -105.545955, 'lat': 40.275903, 'elev': 2741.62},
                     ]),
        ])
        loader.write_files(
            get_asset_installs=lambda: installs,
            get_calibrations=lambda: calibrations,
            get_cfgloc_vers=lambda: cfgloc_vers,
            get_geolocations=lambda _id: geolocations,
            get_parents=lambda _id: {'site': (1, 'XYZ')},
            out_path=self.out_path,
            err_path=self.err_path,
            generated_at=datetime(2026, 7, 15, tzinfo=timezone.utc),
        )
        json_path = (self.out_path / 'enviroscan' / 'CFGLOC777777'
                     / 'position_history' / 'CFGLOC777777_history.json')
        payload = json.loads(json_path.read_text())
        rows = payload['rows']
        self.assertEqual(len(rows), 2)
        # Row order after merge is chronological on effective_start
        r_before, r_after = rows[0], rows[1]
        # Position dates identical across both rows — the sensor never moved
        self.assertEqual(r_before['position_start_date'], '2022-05-05T00:00:00Z')
        self.assertEqual(r_before['position_end_date'],   '')
        self.assertEqual(r_after['position_start_date'],  '2022-05-05T00:00:00Z')
        self.assertEqual(r_after['position_end_date'],    '')
        # Effective and refLocn dates split at the elevation revision boundary
        self.assertEqual(r_before['effective_start_date'], '2022-05-05T00:00:00Z')
        self.assertEqual(r_before['effective_end_date'],   '2024-06-25T00:00:00Z')
        self.assertEqual(r_after['effective_start_date'],  '2024-06-25T00:00:00Z')
        self.assertEqual(r_after['effective_end_date'],    '')
        self.assertEqual(r_before['reference_location_elevation'], 2741.57)
        self.assertEqual(r_after['reference_location_elevation'],  2741.62)

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
             alpha=1.0, beta=0.0, gamma=130.0, ref_feats=None, ref_name='SOILPL999'):
    """Build a Feature dict shaped like get_named_location_geolocations' output.

    `ref_feats` is a list of dicts describing each reference-location time slice; each
    dict may set `start_date`, `end_date`, `lon`, `lat`, `elev`. Defaults to a single
    open-ended slice — matching the shape most existing tests expect.
    """
    if ref_feats is None:
        ref_feats = [{'start_date': '2010-01-01T00:00:00Z', 'end_date': None,
                      'lon': -83.5, 'lat': 35.68, 'elev': 573.32}]
    ref_feat_features = [
        {
            'type': 'Feature',
            'geometry': {'type': 'Point',
                         'coordinates': [r.get('lon', -83.5), r.get('lat', 35.68), r.get('elev', 573.32)]},
            'properties': {'start_date': r.get('start_date'), 'end_date': r.get('end_date')},
        }
        for r in ref_feats
    ]
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
                    'name': ref_name,
                    'locations': {
                        'type': 'FeatureCollection',
                        'features': ref_feat_features,
                    },
                },
            },
        },
    }


def _feature_collection(features):
    return {'type': 'FeatureCollection', 'features': features}
