import math
from typing import List, Optional, NamedTuple

import structlog

from data_access.types.property import Property
from pub_files.database.geolocations import GeoLocation

log = structlog.get_logger()


class SensorPosition(NamedTuple):
    north_offset: Optional[float]
    east_offset: Optional[float]
    x_azimuth: Optional[float]
    y_azimuth: Optional[float]


def get_position(g: GeoLocation) -> SensorPosition:
    log.debug(f'properties: {g.properties}')
    x_azimuth = get_property(g.properties, 'x Azimuth Angle')
    y_azimuth = get_property(g.properties, 'y Azimuth Angle')
    log.debug(f'x_azimuth: {x_azimuth} y_azimuth: {y_azimuth}')
    radius = math.sqrt((g.x_offset * g.x_offset) + (g.y_offset * g.y_offset))
    theta = get_theta(g.x_offset, g.y_offset)
    cardinal_theta = get_cardinal_theta(x_azimuth, y_azimuth, theta)
    if not cardinal_theta:
        north_offset = None
        east_offset = None
    else:
        north_offset = get_north_offset(radius, cardinal_theta)
        east_offset = get_east_offset(radius, cardinal_theta)
    return SensorPosition(north_offset=north_offset,
                          east_offset=east_offset,
                          x_azimuth=x_azimuth,
                          y_azimuth=y_azimuth)


def get_north_offset(radius, cardinal_theta) -> Optional[float]:
    return radius * math.sin(math.radians(cardinal_theta))


def get_east_offset(radius, cardinal_theta) -> Optional[float]:
    return radius * math.cos(math.radians(cardinal_theta))


def get_cardinal_theta(x_azimuth, y_azimuth, theta) -> Optional[float]:
    if (x_azimuth or y_azimuth) and (x_azimuth != 0 and y_azimuth != 0):
        if x_azimuth and not y_azimuth:
            y_azimuth = 0
        if y_azimuth and not x_azimuth:
            x_azimuth = 0
        corrected_y_azimuth = correct_y_azimuth(x_azimuth, y_azimuth)
        return theta - corrected_y_azimuth
    return None


def correct_y_azimuth(x_azimuth, y_azimuth) -> float:
    corrected_y_azimuth = y_azimuth
    if y_azimuth < x_azimuth:
        diff = x_azimuth - y_azimuth
    else:
        diff = 360 - x_azimuth + y_azimuth
    if diff > 90:
        delta = diff - 90
        corrected_y_azimuth = (0.5 * delta) + y_azimuth
        if corrected_y_azimuth >= 360:
            corrected_y_azimuth -= 360
    if diff < 90:
        delta = 90 - diff
        corrected_y_azimuth = y_azimuth - (0.5 * delta)
        if corrected_y_azimuth < 0:
            corrected_y_azimuth += 360
    return corrected_y_azimuth


def get_theta(x_offset, y_offset) -> float:
    if x_offset == 0:
        theta = 90.
    else:
        theta = math.degrees(math.atan(y_offset / x_offset))
    # quadrant correction
    if x_offset < 0:
        theta += 180
    if x_offset > 0 > y_offset:
        theta += 360
    return theta


def get_property(properties: List[Property], property_name: str) -> Optional[float]:
    for prop in properties:
        if prop.name == property_name:
            return float(prop.value)
    return None
