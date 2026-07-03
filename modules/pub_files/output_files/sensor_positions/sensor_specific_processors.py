"""
Sensor-specific processing module.
Handles thermistor depth processing for tchain sensors and per-VER depth
overrides for EnviroSCAN (concH2oSoilSalinity).
"""
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


def create_enviroscan_row(location_json: dict, geolocation,
                          row_location_id: str, row_description: str,
                          create_base_row_data_func, add_reference_position_data_func,
                          database: SensorPositionsDatabase) -> List[List]:
    """Build one row per DB geolocation history entry, with HOR/VER/z_offset
    overridden from the synthesized location JSON."""
    feature = location_json.get('features', [{}])[0] or {}
    hor = feature.get('HOR', '')
    ver = feature.get('VER', '')
    override_hor_ver = f'{hor}.{ver}'
    depth = feature.get('depth')

    base_data = create_base_row_data_func(database, geolocation, override_hor_ver,
                                          row_location_id, row_description)
    if depth is not None:
        base_data['row_z_offset'] = round(base_data['row_z_offset'] + float(depth), 2)

    complete_rows = add_reference_position_data_func(database, base_data, geolocation,
                                                     geolocation.offset_name)

    rows = []
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
