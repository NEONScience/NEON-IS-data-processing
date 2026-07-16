#!/usr/bin/env python3
"""
concH2oSoilSalinity position-history loader.

Queries PDR for the full CFGLOC × asset × calibration × geolocation history for
every enviroscan probe. For each install period at each CFGLOC, picks the cvald1
record whose validity window overlaps that install (asset+stream+install-scoped,
not global per asset+stream) — cvald1 depends on physical install geometry and
can differ between the same sensor's deployments at different CFGLOCs, so a
newer cert from a later deployment must not shadow the cert that was in force
at THIS install. Then stitches (install × geolocation) intersections and applies
the CVALD1 depth correction (depth_m = z_offset + CVALD1_cm / -100) per VER.
Consecutive same-position rows collapse into one; any intervening position
change breaks the run. Writes one JSON file per CFGLOC. Consumers (pub_files
sensor_positions) read these files instead of hitting the DB per publish month,
so every downloaded month contains the complete position history — including
moves that happened outside that month's sensor operation window.
"""
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import structlog

import common.date_formatter as date_formatter
from common.err_datum import err_datum_path
from data_access.types.asset_install import AssetInstall
from data_access.types.cfgloc_ver import CfglocVer
from data_access.types.cvald1_calibration import Cvald1Calibration

log = structlog.get_logger()

DateRange = Tuple[Optional[datetime], Optional[datetime]]


def write_files(*,
                get_asset_installs: Callable[[], List[AssetInstall]],
                get_calibrations:   Callable[[], List[Cvald1Calibration]],
                get_cfgloc_vers:    Callable[[], List[CfglocVer]],
                get_geolocations:   Callable[[int], Any],
                get_parents:        Callable[[int], Optional[Dict[str, Tuple[int, str]]]],
                out_path: Path,
                err_path: Path,
                generated_at: Optional[datetime] = None) -> None:
    """
    Build and write one position-history JSON per enviroscan CFGLOC.

    :param get_asset_installs: Callable returning Q1 rows (CFGLOC × asset install).
    :param get_calibrations:   Callable returning Q2 rows (CVALD1 per asset × period × VSWC stream).
    :param get_cfgloc_vers:    Callable returning Q4 rows (VERs actually configured per CFGLOC).
    :param get_geolocations:   Callable returning the FeatureCollection of geolocations for a nam_locn_id
                               (reuses data_access.get_named_location_geolocations).
    :param get_parents:        Callable returning the parents dict for a nam_locn_id
                               (reuses data_access.get_named_location_parents).
    :param out_path:           Base output path.
    :param err_path:           Error-routing base path.
    :param generated_at:       Timestamp stamped into every JSON. Defaults to now (UTC).
    """
    if generated_at is None:
        generated_at = datetime.now(tz=timezone.utc)
    generated_at_str = _fmt_dt(generated_at)
    now_naive = _naive(generated_at)

    installs = get_asset_installs()
    calibrations = get_calibrations()
    cfgloc_vers = get_cfgloc_vers()

    installs_by_cfgloc: Dict[str, List[AssetInstall]] = defaultdict(list)
    for install in installs:
        installs_by_cfgloc[install.cfgloc].append(install)

    # Keep ALL cvald1 records per (asset_uid, stream); the per-install selection
    # in _build_rows filters to the cal whose valid period overlaps THIS install.
    # A globally "latest calibration_id per asset+stream" pick is wrong because
    # cvald1 depends on the physical install (pipe depth, ground reference), so
    # the newest cert from a later deployment at a different CFGLOC would shadow
    # the cert that was actually in force during this install here.
    cals_by_asset_stream: Dict[Tuple[int, str], List[Cvald1Calibration]] = defaultdict(list)
    for cal in calibrations:
        cals_by_asset_stream[(cal.asset_uid, cal.schema_field_name)].append(cal)

    streams_by_asset: Dict[int, List[str]] = defaultdict(list)
    for (asset_uid, stream_name) in cals_by_asset_stream.keys():
        streams_by_asset[asset_uid].append(stream_name)

    allowed_vers_by_cfgloc: Dict[str, Set[str]] = defaultdict(set)
    hor_by_cfgloc: Dict[str, str] = {}
    for cv in cfgloc_vers:
        allowed_vers_by_cfgloc[cv.cfgloc].add(cv.ver)
        hor_by_cfgloc[cv.cfgloc] = cv.hor  # HOR is per-CFGLOC (all VERs share the same HOR)

    for cfgloc, cfgloc_installs in installs_by_cfgloc.items():
        allowed_vers = allowed_vers_by_cfgloc.get(cfgloc)
        if not allowed_vers:
            # CFGLOC has enviroscan installs but is not configured in the split group
            # (e.g. sensor decommissioned before concH2oSoilSalinity DP existed). Skip.
            log.debug(f'Skipping {cfgloc}: no configured VERs in split group')
            continue

        hor = hor_by_cfgloc[cfgloc]
        nam_locn_id = cfgloc_installs[0].nam_locn_id
        cfgloc_description = cfgloc_installs[0].cfgloc_description

        parents = get_parents(nam_locn_id) or {}
        site = parents.get('site', (None, None))[1]

        geo_collection = get_geolocations(nam_locn_id)
        geolocations = _flatten_geolocations(geo_collection)

        rows = _build_rows(
            cfgloc=cfgloc,
            cfgloc_installs=cfgloc_installs,
            geolocations=geolocations,
            cals_by_asset_stream=cals_by_asset_stream,
            streams_by_asset=streams_by_asset,
            allowed_vers=allowed_vers,
            hor=hor,
            now_naive=now_naive,
        )

        if not rows:
            log.debug(f'{cfgloc}: no rows produced (no valid intersections)')
            continue

        out_obj = {
            'source_type': 'enviroscan',
            'cfgloc': cfgloc,
            'cfgloc_description': cfgloc_description,
            'site': site,
            'generated_at': generated_at_str,
            'rows': rows,
        }
        _write_json(cfgloc=cfgloc, obj=out_obj, out_path=out_path, err_path=err_path)


def _build_rows(*,
                cfgloc:          str,
                cfgloc_installs: List[AssetInstall],
                geolocations:    List[Dict[str, Any]],
                cals_by_asset_stream: Dict[Tuple[int, str], List[Cvald1Calibration]],
                streams_by_asset: Dict[int, List[str]],
                allowed_vers:    Set[str],
                hor:             str,
                now_naive:       datetime) -> List[Dict[str, Any]]:
    """
    For a single CFGLOC, produce the (install × geolocation × VER) intersection rows.
    For each (install, stream), pick the cvald1 record whose valid period overlaps
    that install; if none overlaps, skip that stream for that install and log.

    :param cals_by_asset_stream: (asset_uid, stream_name) -> all cvald1 rows for
                                 that asset+stream. Selection happens per install.
    :param streams_by_asset: asset_uid -> list of stream names seen in the cal set.
    :param now_naive: 'Generated-at' timestamp (tz-naive). Any window end that
                      falls after this is treated as open-ended — PDR uses
                      placeholder future dates (e.g. 2031-12-31) for still-valid
                      records, and downstream consumers expect a blank end.
    """
    rows: List[Dict[str, Any]] = []
    for install in cfgloc_installs:
        stream_names = streams_by_asset.get(install.asset_uid, [])
        if not stream_names:
            continue
        for geo in geolocations:
            # Position window is the install × cfgloc-geo intersection — the true period
            # the sensor was physically at these offsets, independent of ref-location
            # time slicing. Effective window narrows that with the ref_feat's own dates.
            pos_window = _intersect(
                (install.install_date, install.remove_date),
                (geo['start_date'], geo['end_date']),
            )
            if pos_window is None:
                continue
            ref_start_dt = _parse_dt(geo.get('reference_location_start_date'))
            ref_end_dt = _parse_dt(geo.get('reference_location_end_date'))
            eff_window = _intersect(
                pos_window,
                (ref_start_dt, ref_end_dt),
            )
            if eff_window is None:
                continue
            pos_start, pos_end = pos_window
            eff_start, eff_end = eff_window
            # Collapse placeholder-future end dates to open-ended for both windows.
            if pos_end is not None and pos_end > now_naive:
                pos_end = None
            if eff_end is not None and eff_end > now_naive:
                eff_end = None
            for stream_name in stream_names:
                ver = _stream_to_ver(stream_name)
                if ver is None or str(ver) not in allowed_vers:
                    continue
                cal_candidates = cals_by_asset_stream.get((install.asset_uid, stream_name), [])
                stream = _select_cal_for_install(cal_candidates,
                                                 install.install_date,
                                                 install.remove_date)
                if stream is None:
                    log.warning(
                        'no cvald1 record overlaps install period; skipping row',
                        cfgloc=cfgloc, asset_uid=install.asset_uid,
                        stream=stream_name, ver=str(ver),
                        install_start=install.install_date,
                        install_end=install.remove_date,
                    )
                    continue
                z_offset_adjusted = round(geo['z_offset'] + stream.cvald1_cm / -100.0, 4)
                rows.append({
                    'hor': hor,
                    'ver': str(ver),
                    # Keep raw datetimes for the merge pass below; formatted later.
                    '_eff_start': eff_start,
                    '_eff_end':   eff_end,
                    '_pos_start': pos_start,
                    '_pos_end':   pos_end,
                    'x_offset': geo['x_offset'],
                    'y_offset': geo['y_offset'],
                    'z_offset': z_offset_adjusted,
                    'pitch':    geo['pitch'],
                    'roll':     geo['roll'],
                    'azimuth':  geo['azimuth'],
                    'reference_location_id':         geo['reference_location_id'],
                    'reference_location_start_date': geo['reference_location_start_date'],
                    'reference_location_end_date':   geo['reference_location_end_date'],
                    'reference_location_latitude':   geo['reference_location_latitude'],
                    'reference_location_longitude':  geo['reference_location_longitude'],
                    'reference_location_elevation':  geo['reference_location_elevation'],
                    'asset_uid':       install.asset_uid,
                    'cvald1_cm':       stream.cvald1_cm,
                    'source_stream':   stream.schema_field_name,
                    'cert_filename':   stream.cert_filename,
                })
    return _merge_time_ranges(rows)


def _select_cal_for_install(cals: List[Cvald1Calibration],
                            install_start: Optional[datetime],
                            install_end: Optional[datetime]) -> Optional[Cvald1Calibration]:
    """
    Pick the cvald1 record whose validity window overlaps this install period at
    this CFGLOC. Prefer the cal in force at install start (latest valid_start ≤
    install_start); if none, prefer the earliest-starting overlapping cal.
    Tiebreak by highest calibration_id.

    Returns None if no cal overlaps the install — the caller skips that install/
    stream rather than apply a cross-deployment cal (e.g. asset 40784's cert
    measured at CFGLOC113339 must NOT be used for its earlier CFGLOC105360 stint).
    """
    install_start_n = _naive(install_start) if install_start is not None else None
    install_end_n = _naive(install_end) if install_end is not None else None

    def overlaps(cal: Cvald1Calibration) -> bool:
        return _intersect(
            (install_start_n, install_end_n),
            (cal.valid_start_time, cal.valid_end_time),
        ) is not None

    candidates = [c for c in cals if overlaps(c)]
    if not candidates:
        return None

    if install_start_n is not None:
        in_force = [c for c in candidates
                    if _naive(c.valid_start_time) <= install_start_n]
        if in_force:
            return max(in_force, key=lambda c: (_naive(c.valid_start_time),
                                                c.calibration_id))
    return min(candidates,
               key=lambda c: (_naive(c.valid_start_time), -c.calibration_id))


def _merge_time_ranges(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Consolidate rows chronologically per (HOR, VER) by effective start.

    Three date sets are tracked per merged row:
      - effective (`_eff_start`/`_eff_end`)  — the 3-way intersect of install × cfgloc-geo
        × ref_feat. Merged rows form a non-overlapping timeline; consumers key on this.
      - position  (`_pos_start`/`_pos_end`)  — install × cfgloc-geo. Reflects when the
        physical sensor position existed regardless of ref-location time slicing.
      - refLocn                              — the ref_feat's own start/end (in the key
        fields already, so consistent within a merged run).

    Two passes:

      Phase A — merge on the FULL identity key (position offsets + ref-location identity).
      Rows sharing the same full key collapse when adjacent in effective time.

      Phase B — reunion position dates across ref-locn splits at the same position. Rows
      sharing the same position-offsets identity, contiguous in effective time (no gap
      to the previous row, no intervening different-offsets row), share the same
      position_start/end. Without this, two rows differing only in ref-locn slice would
      show different position dates depending on which installs each slice happened to
      intersect — even though the physical position never moved.

    An A-B-A sequence (position A, then B, then A again) stays as three rows — the
    intermediate B breaks BOTH the full-key run and the position-only run, matching the
    physical reality that the sensor was moved between two same-position deployments.
    """
    key_fields = (
        'x_offset', 'y_offset', 'z_offset',
        'pitch', 'roll', 'azimuth',
        'reference_location_id',
        'reference_location_start_date', 'reference_location_end_date',
        'reference_location_latitude', 'reference_location_longitude',
        'reference_location_elevation',
    )
    pos_only_fields = ('x_offset', 'y_offset', 'z_offset', 'pitch', 'roll', 'azimuth')
    NEG_INF = datetime.min
    POS_INF = datetime.max

    def norm_start(dt_val: Any) -> datetime:
        return _naive(dt_val) if dt_val is not None else NEG_INF

    def norm_end(dt_val: Any) -> datetime:
        return _naive(dt_val) if dt_val is not None else POS_INF

    def eff_start(r: Dict[str, Any]) -> datetime:
        return norm_start(r.get('_eff_start'))

    def eff_end(r: Dict[str, Any]) -> datetime:
        return norm_end(r.get('_eff_end'))

    def pos_start(r: Dict[str, Any]) -> datetime:
        val = r.get('_pos_start', r.get('_eff_start'))
        return norm_start(val)

    def pos_end(r: Dict[str, Any]) -> datetime:
        val = r.get('_pos_end', r.get('_eff_end'))
        return norm_end(val)

    def key(r: Dict[str, Any]) -> Tuple:
        return tuple(r.get(f) for f in key_fields)

    def pos_key(r: Dict[str, Any]) -> Tuple:
        return tuple(r.get(f) for f in pos_only_fields)

    by_ver: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_ver[(row['hor'], row['ver'])].append(row)

    merged: List[Dict[str, Any]] = []
    for _, ver_rows in by_ver.items():
        ver_rows.sort(key=eff_start)

        # Phase A: collapse full-key adjacent runs.
        phase_a: List[Dict[str, Any]] = []
        cur = ver_rows[0]
        cur_eff_s = eff_start(cur)
        cur_eff_e = eff_end(cur)
        cur_pos_s = pos_start(cur)
        cur_pos_e = pos_end(cur)
        cur_key = key(cur)
        for r in ver_rows[1:]:
            r_key = key(r)
            if r_key == cur_key:
                cur_eff_e = max(cur_eff_e, eff_end(r))
                cur_pos_s = min(cur_pos_s, pos_start(r))
                cur_pos_e = max(cur_pos_e, pos_end(r))
            else:
                phase_a.append({'row': cur, 'eff_s': cur_eff_s, 'eff_e': cur_eff_e,
                                'pos_s': cur_pos_s, 'pos_e': cur_pos_e})
                cur = r
                cur_eff_s = eff_start(r)
                cur_eff_e = eff_end(r)
                cur_pos_s = pos_start(r)
                cur_pos_e = pos_end(r)
                cur_key = r_key
        phase_a.append({'row': cur, 'eff_s': cur_eff_s, 'eff_e': cur_eff_e,
                        'pos_s': cur_pos_s, 'pos_e': cur_pos_e})

        # Phase B: within same position-offsets identity, contiguous in effective time
        # (no gap, no intervening different-offsets row), reunion position_start/end so
        # ref-locn splits at the same position surface consistent position dates.
        run: List[Dict[str, Any]] = []
        prev_pos_key: Optional[Tuple] = None
        prev_eff_e: Optional[datetime] = None

        def close_run() -> None:
            if not run:
                return
            run_pos_s = min(e['pos_s'] for e in run)
            run_pos_e = max(e['pos_e'] for e in run)
            for e in run:
                merged.append(_finalize_row(e['row'], e['eff_s'], e['eff_e'],
                                            run_pos_s, run_pos_e, NEG_INF, POS_INF))

        for entry in phase_a:
            pk = pos_key(entry['row'])
            same_offsets = (prev_pos_key is not None and pk == prev_pos_key)
            no_gap = (prev_eff_e is not None and entry['eff_s'] <= prev_eff_e)
            if same_offsets and no_gap:
                run.append(entry)
            else:
                close_run()
                run = [entry]
            prev_pos_key = pk
            prev_eff_e = entry['eff_e']
        close_run()

    return merged


def _finalize_row(row: Dict[str, Any],
                  eff_s: datetime, eff_e: datetime,
                  pos_s: datetime, pos_e: datetime,
                  neg_inf: datetime, pos_inf: datetime) -> Dict[str, Any]:
    """Format the merged interval back into ISO strings; preserve field order.

    Emits three date sets: effective (non-overlapping timeline consumers key on),
    position (install × cfgloc-geo), and reference-location dates (already in `row`).
    """
    out: Dict[str, Any] = {
        'hor': row['hor'],
        'ver': row['ver'],
        'effective_start_date': '' if eff_s == neg_inf else _fmt_dt(eff_s),
        'effective_end_date':   '' if eff_e == pos_inf else _fmt_dt(eff_e),
        'position_start_date':  '' if pos_s == neg_inf else _fmt_dt(pos_s),
        'position_end_date':    '' if pos_e == pos_inf else _fmt_dt(pos_e),
    }
    for k, v in row.items():
        if k in ('hor', 'ver') or k.startswith('_'):
            continue
        out[k] = v
    # Provenance (asset_uid, cvald1_cm, cal_valid_start/end, source_stream,
    # cert_filename) is taken from the FIRST sub-interval — a merged row spans
    # multiple asset installs / cal periods, so a single value here isn't
    # authoritative. Consumers care about position, not provenance.
    return out


def _flatten_geolocations(feature_collection: Any) -> List[Dict[str, Any]]:
    """
    Unpack the FeatureCollection returned by get_named_location_geolocations into a flat list of dicts.

    Each feature in the top-level collection is one row from `locn` for this CFGLOC. Its
    `properties.reference_location` carries a nested Feature whose `locations` are the
    reference-location's own time-sliced geolocations (lat/lon/elevation per time slice).

    Emits one flat entry per (cfgloc-geo × ref_feat) pair — a ref location that changed
    (e.g. elevation shifted) surfaces as multiple entries so the caller can split rows on
    the ref_feat boundary. Falls back to one entry with null ref fields when a cfgloc-geo
    has no reference feature or the reference has no time slices.
    """
    flat: List[Dict[str, Any]] = []
    if feature_collection is None:
        return flat
    features = feature_collection.get('features') if isinstance(feature_collection, dict) else getattr(feature_collection, 'features', [])
    for feature in features or []:
        props = feature.get('properties') if isinstance(feature, dict) else feature.properties
        if props is None:
            continue
        base = {
            'start_date': _parse_dt(props.get('start_date')),
            'end_date':   _parse_dt(props.get('end_date')),
            'x_offset':   props.get('x_offset'),
            'y_offset':   props.get('y_offset'),
            'z_offset':   props.get('z_offset'),
            'pitch':      props.get('alpha'),
            'roll':       props.get('beta'),
            'azimuth':    props.get('gamma'),
        }
        ref_slices = _ref_feat_slices(props.get('reference_location'))
        for ref in ref_slices:
            entry = dict(base)
            entry.update(ref)
            flat.append(entry)
    return flat


def _ref_feat_slices(ref_feature: Any) -> List[Dict[str, Any]]:
    """
    Return one dict per reference-location time slice (ref_feat). Each dict carries
    the ref name, that slice's start/end dates, and its lat/lon/elevation.

    A CFGLOC with no ref feature (or a ref feature with no time slices) still produces
    one entry with all fields None so `_build_rows` can still emit a row.
    """
    if ref_feature is None:
        return [_null_ref_slice()]
    ref_props = ref_feature.get('properties') if isinstance(ref_feature, dict) else ref_feature.properties
    if ref_props is None:
        return [_null_ref_slice()]
    ref_name = ref_props.get('name')
    ref_locations = ref_props.get('locations') or {}
    ref_feats = ref_locations.get('features') if isinstance(ref_locations, dict) else getattr(ref_locations, 'features', [])
    if not ref_feats:
        return [_null_ref_slice(ref_name=ref_name)]
    slices: List[Dict[str, Any]] = []
    for ref_geo_feat in ref_feats:
        ref_geo_props = ref_geo_feat.get('properties') if isinstance(ref_geo_feat, dict) else ref_geo_feat.properties
        ref_geometry = ref_geo_feat.get('geometry') if isinstance(ref_geo_feat, dict) else ref_geo_feat.geometry
        ref_lon, ref_lat, ref_elev = _extract_point(ref_geometry)
        ref_start = ref_geo_props.get('start_date') if ref_geo_props is not None else None
        ref_end = ref_geo_props.get('end_date') if ref_geo_props is not None else None
        slices.append({
            'reference_location_id':         ref_name,
            'reference_location_start_date': ref_start,
            'reference_location_end_date':   ref_end,
            'reference_location_latitude':   ref_lat,
            'reference_location_longitude':  ref_lon,
            'reference_location_elevation':  ref_elev,
        })
    return slices


def _null_ref_slice(ref_name: Optional[str] = None) -> Dict[str, Any]:
    return {
        'reference_location_id':         ref_name,
        'reference_location_start_date': None,
        'reference_location_end_date':   None,
        'reference_location_latitude':   None,
        'reference_location_longitude':  None,
        'reference_location_elevation':  None,
    }


def _extract_point(geometry: Any) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Return (lon, lat, elevation) from a geojson Point-like structure.

    PDR reference-location geometries sometimes come back as MultiPoint
    ([[lon, lat, elev]]) instead of Point ([lon, lat, elev]); unwrap one level.
    """
    if geometry is None:
        return None, None, None
    coords = geometry.get('coordinates') if isinstance(geometry, dict) else getattr(geometry, 'coordinates', None)
    if not coords:
        return None, None, None
    if isinstance(coords[0], (list, tuple)):
        coords = coords[0]
    if len(coords) < 2:
        return None, None, None
    lon = float(coords[0])
    lat = float(coords[1])
    elev = float(coords[2]) if len(coords) >= 3 else None
    return lon, lat, elev


def _intersect(*periods: DateRange) -> Optional[DateRange]:
    """
    Intersect any number of half-open date ranges. `None` on either side means open-ended.
    Returns None if the ranges do not overlap.

    Strips tzinfo before comparing to avoid crashes when psycopg2 returns tz-aware
    datetimes for some columns while string-parsed dates come back tz-naive.
    """
    starts = [_naive(p[0]) for p in periods if p[0] is not None]
    ends = [_naive(p[1]) for p in periods if p[1] is not None]
    win_start = max(starts) if starts else None
    win_end = min(ends) if ends else None
    if win_start is not None and win_end is not None and win_start >= win_end:
        return None
    return (win_start, win_end)


def _naive(dt: datetime) -> datetime:
    """Drop tzinfo so tz-aware and tz-naive datetimes can be compared consistently."""
    if dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt


def _stream_to_ver(schema_field_name: Optional[str]) -> Optional[int]:
    """rawVSWC0 -> 501, rawVSWC7 -> 508. Returns None if the pattern doesn't match."""
    if not schema_field_name or not schema_field_name.startswith('rawVSWC'):
        return None
    suffix = schema_field_name[len('rawVSWC'):]
    if not suffix.isdigit():
        return None
    return 501 + int(suffix)


def _fmt_dt(value: Any) -> str:
    """
    Return an ISO-8601 UTC string for a datetime, an empty string for None,
    and passthrough for pre-formatted strings.
    """
    if value is None:
        return ''
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        return date_formatter.to_string(value)
    return str(value)


def _parse_dt(value: Any) -> Optional[datetime]:
    """
    Return a datetime for either a datetime input or a formatted string.
    Empty/None returns None (open-ended).
    """
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return date_formatter.to_datetime(value)
        except (ValueError, AttributeError):
            return None
    return None


def _write_json(*, cfgloc: str, obj: Dict[str, Any], out_path: Path, err_path: Path) -> None:
    """
    Write the position-history JSON for one CFGLOC. Path shape matches how pub_files
    joins the loader output: <out_path>/enviroscan/<cfgloc>/position_history/<cfgloc>_history.json
    """
    file_path = Path(out_path, 'enviroscan', cfgloc, 'position_history', f'{cfgloc}_history.json')
    file_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(file_path, 'w') as f:
            json.dump(obj, f, indent=2, default=str, sort_keys=False)
    except Exception:
        err_datum_path(err=sys.exc_info(), DirDatm=str(file_path.parent), DirErrBase=Path(err_path),
                       RmvDatmOut=True, DirOutBase=out_path)
