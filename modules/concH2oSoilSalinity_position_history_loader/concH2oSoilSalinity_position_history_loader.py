#!/usr/bin/env python3
"""
concH2oSoilSalinity position-history loader.

Queries PDR for the full CFGLOC × asset × calibration × geolocation history for
every enviroscan probe, stitches the intersections, applies the CVALD1 depth
correction (depth_m = z_offset + CVALD1_cm / -100) per VER, and writes one JSON
file per CFGLOC. Consumers (pub_files sensor_positions) read these files instead
of hitting the DB per publish month, so every downloaded month contains the
complete position history — including moves that happened outside that month's
sensor operation window.
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

    # (asset_uid, valid_start, valid_end) -> [stream calibrations at that period]
    cals_by_asset_period: Dict[Tuple[int, Optional[datetime], Optional[datetime]], List[Cvald1Calibration]] = defaultdict(list)
    for cal in calibrations:
        cals_by_asset_period[(cal.asset_uid, cal.valid_start_time, cal.valid_end_time)].append(cal)

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
            cfgloc_installs=cfgloc_installs,
            geolocations=geolocations,
            cals_by_asset_period=cals_by_asset_period,
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
                cfgloc_installs: List[AssetInstall],
                geolocations:    List[Dict[str, Any]],
                cals_by_asset_period: Dict[Tuple[int, Optional[datetime], Optional[datetime]], List[Cvald1Calibration]],
                allowed_vers:    Set[str],
                hor:             str,
                now_naive:       datetime) -> List[Dict[str, Any]]:
    """
    For a single CFGLOC, produce the full list of (install × geolocation × cal-period × VER)
    intersection rows.

    :param now_naive: 'Generated-at' timestamp (tz-naive). Any window end that
                      falls after this is treated as open-ended — PDR uses
                      placeholder future dates (e.g. 2031-12-31) for still-valid
                      records, and downstream consumers expect a blank end.
    """
    rows: List[Dict[str, Any]] = []
    for install in cfgloc_installs:
        asset_periods = [(period, streams)
                         for period, streams in cals_by_asset_period.items()
                         if period[0] == install.asset_uid]
        for geo in geolocations:
            for (_, cal_start, cal_end), streams in asset_periods:
                window = _intersect(
                    (install.install_date, install.remove_date),
                    (geo['start_date'], geo['end_date']),
                    (cal_start, cal_end),
                )
                if window is None:
                    continue
                win_start, win_end = window
                # Collapse placeholder-future end dates to open-ended.
                if win_end is not None and win_end > now_naive:
                    win_end = None
                for stream in streams:
                    ver = _stream_to_ver(stream.schema_field_name)
                    if ver is None or str(ver) not in allowed_vers:
                        continue
                    z_offset_adjusted = round(geo['z_offset'] + stream.cvald1_cm / -100.0, 4)
                    rows.append({
                        'hor': hor,
                        'ver': str(ver),
                        # Keep raw datetimes for the merge pass below; formatted later.
                        '_win_start': win_start,
                        '_win_end':   win_end,
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
                        'cal_valid_start': _fmt_dt(cal_start),
                        'cal_valid_end':   _fmt_dt(cal_end),
                        'source_stream':   stream.schema_field_name,
                        'cert_filename':   stream.cert_filename,
                    })
    return _merge_time_ranges(rows)


def _merge_time_ranges(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Consolidate rows that differ only in time range into one row per unique
    physical position (HOR/VER, offsets, orientation, reference-location).
    Each group emits a single row spanning the earliest start to the latest
    end across all its sub-windows — even across removal/reinstall gaps,
    since a re-installed sensor at the same offsets is the same position
    from a sensor_positions.csv consumer's point of view.

    Rows in DIFFERENT groups (e.g. a brief cvald1 anomaly that put a depth at
    z=-0.36 for 20 days while the rest of the timeline had z=-0.46) stay
    separate, so real position changes still surface.
    """
    key_fields = (
        'hor', 'ver',
        'x_offset', 'y_offset', 'z_offset',
        'pitch', 'roll', 'azimuth',
        'reference_location_id',
        'reference_location_start_date', 'reference_location_end_date',
        'reference_location_latitude', 'reference_location_longitude',
        'reference_location_elevation',
    )
    NEG_INF = datetime.min
    POS_INF = datetime.max

    groups: Dict[Tuple, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row.get(f) for f in key_fields)].append(row)

    merged: List[Dict[str, Any]] = []
    for _, group_rows in groups.items():
        earliest_start = POS_INF
        latest_end = NEG_INF
        earliest_row = group_rows[0]
        for r in group_rows:
            s = _naive(r['_win_start']) if r['_win_start'] is not None else NEG_INF
            e = _naive(r['_win_end'])   if r['_win_end']   is not None else POS_INF
            if s < earliest_start:
                earliest_start = s
                earliest_row = r
            if e > latest_end:
                latest_end = e
        merged.append(_finalize_row(earliest_row, earliest_start, latest_end, NEG_INF, POS_INF))
    return merged


def _finalize_row(row: Dict[str, Any], start: datetime, end: datetime,
                  neg_inf: datetime, pos_inf: datetime) -> Dict[str, Any]:
    """Format the merged interval back into ISO strings; preserve field order."""
    out: Dict[str, Any] = {
        'hor': row['hor'],
        'ver': row['ver'],
        'position_start_date': '' if start == neg_inf else _fmt_dt(start),
        'position_end_date':   '' if end   == pos_inf else _fmt_dt(end),
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

    Each feature in the top-level collection is one row from `locn` for this CFGLOC.
    `properties.reference_location` is a nested Feature carrying the reference (e.g. SOILPL...)
    nam_locn_name and its own recursive geolocations. The reference's first geolocation
    row is where lat/lon/elevation live.
    """
    flat: List[Dict[str, Any]] = []
    if feature_collection is None:
        return flat
    features = feature_collection.get('features') if isinstance(feature_collection, dict) else getattr(feature_collection, 'features', [])
    for feature in features or []:
        props = feature.get('properties') if isinstance(feature, dict) else feature.properties
        if props is None:
            continue
        ref_feature = props.get('reference_location')
        ref_name = ref_lat = ref_lon = ref_elev = None
        ref_start = ref_end = None
        if ref_feature is not None:
            ref_props = ref_feature.get('properties') if isinstance(ref_feature, dict) else ref_feature.properties
            if ref_props is not None:
                ref_name = ref_props.get('name')
                ref_locations = ref_props.get('locations') or {}
                ref_feats = ref_locations.get('features') if isinstance(ref_locations, dict) else getattr(ref_locations, 'features', [])
                if ref_feats:
                    ref_geo_feat = ref_feats[0]
                    ref_geo_props = ref_geo_feat.get('properties') if isinstance(ref_geo_feat, dict) else ref_geo_feat.properties
                    ref_geometry = ref_geo_feat.get('geometry') if isinstance(ref_geo_feat, dict) else ref_geo_feat.geometry
                    ref_lon, ref_lat, ref_elev = _extract_point(ref_geometry)
                    if ref_geo_props is not None:
                        ref_start = ref_geo_props.get('start_date')
                        ref_end = ref_geo_props.get('end_date')
        flat.append({
            'start_date': _parse_dt(props.get('start_date')),
            'end_date':   _parse_dt(props.get('end_date')),
            'x_offset':   props.get('x_offset'),
            'y_offset':   props.get('y_offset'),
            'z_offset':   props.get('z_offset'),
            'pitch':      props.get('alpha'),
            'roll':       props.get('beta'),
            'azimuth':    props.get('gamma'),
            'reference_location_id':         ref_name,
            'reference_location_start_date': ref_start,
            'reference_location_end_date':   ref_end,
            'reference_location_latitude':   ref_lat,
            'reference_location_longitude':  ref_lon,
            'reference_location_elevation':  ref_elev,
        })
    return flat


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
