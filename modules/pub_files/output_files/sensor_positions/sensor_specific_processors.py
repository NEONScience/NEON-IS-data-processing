"""
Sensor-specific processing module.
Handles thermistor depth processing for tchain sensors and per-VER depth
overrides for EnviroSCAN (concH2oSoilSalinity).
"""
from collections import defaultdict
from typing import List, Dict, Optional
from pub_files.output_files.sensor_positions.sensor_position import get_property
from pub_files.output_files.sensor_positions.sensor_positions_database import SensorPositionsDatabase

def is_tchain_sensor(location) -> bool:
    """Check if this location represents a tchain sensor by looking for thermistor depth properties."""
    return any(get_property(location.properties, f'ThermistorDepth{i}') is not None 
               for i in range(501, 512))

def get_thermistor_depths(location) -> Dict[str, Optional[float]]:
    """Extract thermistor depths for tchain sensors."""
    thermistor_depths = {}
    for i in range(501, 512):
        key = str(i)
        thermistor_depths[key] = get_property(location.properties, f'ThermistorDepth{key}')
    return thermistor_depths


def create_tchain_rows(database: SensorPositionsDatabase, location, geolocation, 
                      row_hor_ver: str, row_location_id: str, row_description: str,
                      create_base_row_data_func, add_reference_position_data_func) -> List[List]:
    """Create multiple rows for tchain sensor, one for each thermistor depth."""
    base_data = create_base_row_data_func(database, geolocation, row_hor_ver, row_location_id, row_description)
    complete_rows = add_reference_position_data_func(database, base_data, geolocation, geolocation.offset_name)
    
    thermistor_depths = get_thermistor_depths(location)
    tchain_rows = []
    
    for complete_row_data in complete_rows:
        for thermistor_id, depth in thermistor_depths.items():
            if depth is not None:
                # Modify HOR.VER for this thermistor
                hor_ver_split = complete_row_data['row_hor_ver'].split('.')
                hor_ver_split[1] = thermistor_id
                modified_hor_ver = ".".join(hor_ver_split)

                # Adjust z_offset for thermistor depth
                modified_z_offset = round(complete_row_data['row_z_offset'] - depth, 2)

                tchain_row = [
                    modified_hor_ver,
                    complete_row_data['row_location_id'],
                    complete_row_data['row_description'],
                    complete_row_data['row_position_start_date'],
                    complete_row_data['row_position_end_date'],
                    complete_row_data['row_reference_location_id'],
                    complete_row_data['row_reference_location_description'],
                    complete_row_data['row_reference_location_start_date'],
                    complete_row_data['row_reference_location_end_date'],
                    complete_row_data['row_x_offset'],
                    complete_row_data['row_y_offset'],
                    modified_z_offset,
                    complete_row_data['row_pitch'],
                    complete_row_data['row_roll'],
                    complete_row_data['row_azimuth'],
                    complete_row_data['row_reference_location_latitude'],
                    complete_row_data['row_reference_location_longitude'],
                    complete_row_data['row_reference_location_elevation'],
                    complete_row_data['row_east_offset'],
                    complete_row_data['row_north_offset'],
                    complete_row_data['row_x_azimuth'],
                    complete_row_data['row_y_azimuth']
                ]
                tchain_rows.append(tchain_row)

    return tchain_rows


def is_enviroscan_sensor(location_json: Optional[dict]) -> bool:
    """Check if this location JSON was synthesized by group_split for EnviroSCAN.

    The R wrapper wrap.concH2oSalinity.grp.split.R writes one JSON per (CFGLOC,
    VER) with override_source == 'enviroscan'. The shared CFGLOC has one DB row
    so DB values alone would collapse the 8 depths to one; the JSON supplies
    per-VER HOR/VER and depth (z-offset).
    """
    if not location_json:
        return False
    return location_json.get('override_source') == 'enviroscan'


def collect_enviroscan_features(location_json: dict, cfgloc: str,
                                row_location_id: str, row_description: str,
                                geolocations, sink: List[Dict]) -> None:
    """Append each feature in the JSON to a shared collection pool for
    later aggregation across days. group_split emits one JSON per (CFGLOC,
    VER) per day with N features covering N depth segments within that day;
    pub_files sees all monthly datum days at once and merges day-adjacent
    same-(cfgloc, HOR, VER, depth) features into single rows."""
    for feature in location_json.get('features', []) or []:
        if not feature:
            continue
        depth = feature.get('depth')
        if depth is None:
            continue
        sink.append({
            'cfgloc': cfgloc,
            'hor': feature.get('HOR', ''),
            'ver': feature.get('VER', ''),
            'depth': float(depth),
            'position_start_date': feature.get('positionStartDateTime', ''),
            'position_end_date': feature.get('positionEndDateTime', ''),
            'row_location_id': row_location_id,
            'row_description': row_description,
            'geolocations': geolocations,
        })


def aggregate_enviroscan_rows(collected: List[Dict],
                              database: SensorPositionsDatabase,
                              create_base_row_data_func,
                              add_reference_position_data_func) -> List[List]:
    """Merge day-adjacent same-(cfgloc, HOR, VER, depth) features into
    contiguous ranges, emit one row per merged range per DB geolocation.
    Adjacent day-runs are considered contiguous when prev.end == next.start
    (group_split ensures this: day N ends at UTC midnight, day N+1 starts
    at the same instant; transition-day crossovers fall in the middle of
    the ambiguous zone). z_offset overridden as DB_z + segment depth."""
    if not collected:
        return []

    # Group by (cfgloc, HOR, VER, rounded depth). Depths compared to 4
    # decimals so cal-file jitter doesn't fragment same-depth runs.
    groups: Dict[tuple, List[Dict]] = defaultdict(list)
    for f in collected:
        key = (f['cfgloc'], f['hor'], f['ver'], round(f['depth'], 4))
        groups[key].append(f)

    rows: List[List] = []
    for (cfgloc, hor, ver, depth), feats in groups.items():
        feats.sort(key=lambda x: x['position_start_date'])

        # Merge contiguous ISO-Z strings (group_split guarantees exact match
        # at day boundaries — no timezone or precision drift on the seam).
        merged: List[Dict] = [dict(feats[0])]
        for f in feats[1:]:
            if merged[-1]['position_end_date'] == f['position_start_date']:
                merged[-1]['position_end_date'] = f['position_end_date']
            else:
                merged.append(dict(f))

        override_hor_ver = f'{hor}.{ver}'
        for m in merged:
            for geolocation in m['geolocations']:
                base_data = create_base_row_data_func(
                    database, geolocation, override_hor_ver,
                    m['row_location_id'], m['row_description'])
                base_data['row_z_offset'] = round(
                    base_data['row_z_offset'] + float(depth), 2)
                # Segment range overrides DB geolocation dates for the
                # positionStart/EndDateTime CSV columns.
                base_data['row_position_start_date'] = m['position_start_date']
                base_data['row_position_end_date'] = m['position_end_date']

                complete_rows = add_reference_position_data_func(
                    database, base_data, geolocation, geolocation.offset_name)

                for row_data in complete_rows:
                    rows.append([
                        row_data['row_hor_ver'],
                        row_data['row_location_id'],
                        row_data['row_description'],
                        row_data['row_position_start_date'],
                        row_data['row_position_end_date'],
                        row_data['row_reference_location_id'],
                        row_data['row_reference_location_description'],
                        row_data['row_reference_location_start_date'],
                        row_data['row_reference_location_end_date'],
                        row_data['row_x_offset'],
                        row_data['row_y_offset'],
                        row_data['row_z_offset'],
                        row_data['row_pitch'],
                        row_data['row_roll'],
                        row_data['row_azimuth'],
                        row_data['row_reference_location_latitude'],
                        row_data['row_reference_location_longitude'],
                        row_data['row_reference_location_elevation'],
                        row_data['row_east_offset'],
                        row_data['row_north_offset'],
                        row_data['row_x_azimuth'],
                        row_data['row_y_azimuth']
                    ])
    return rows
