import math
from decimal import Decimal, ROUND_UP
from typing import List, Optional, NamedTuple, Tuple

import structlog

from data_access.types.property import Property
from pub_files.database.geolocations import GeoLocation

log = structlog.get_logger()


class SensorPosition(NamedTuple):
    north_offset: Optional[Decimal]
    east_offset: Optional[Decimal]
    x_azimuth: Optional[float]
    y_azimuth: Optional[float]


def get_position(g: GeoLocation) -> SensorPosition:
    x_azimuth = get_property(g.properties, 'x Azimuth Angle')
    y_azimuth = get_property(g.properties, 'y Azimuth Angle')
    log.debug(f'x_offset: {g.x_offset} y_offset: {g.y_offset}')
    log.debug(f'x_azimuth: {x_azimuth} y_azimuth: {y_azimuth}')
    (east_offset, north_offset) = get_cardinal_offsets(x_azimuth, y_azimuth, g.x_offset, g.y_offset)
    if north_offset == -0:
        north_offset = abs(north_offset)
    if east_offset == -0:
        east_offset = abs(east_offset)
    return SensorPosition(north_offset=north_offset,
                          east_offset=east_offset,
                          x_azimuth=x_azimuth,
                          y_azimuth=y_azimuth)


def get_cardinal_offsets(x_azimuth, y_azimuth, x_offset, y_offset) -> Tuple[Decimal, Decimal]:
    diff = Decimal(0)
    delta = Decimal(0)
    corrected_y_azimuth = Decimal(y_azimuth)
    if y_azimuth < x_azimuth:
        diff = x_azimuth - y_azimuth
    else:
        diff = 360 - y_azimuth + x_azimuth
    if diff > 90:
        delta = diff - 90
        corrected_y_azimuth = Decimal(0.5 * delta + y_azimuth)
        if corrected_y_azimuth >= 360:
            corrected_y_azimuth -= 360
    if diff < 90:
        delta = 90 - diff
        corrected_y_azimuth = Decimal(y_azimuth - 0.5 * delta)
        if corrected_y_azimuth < 0:
            corrected_y_azimuth += 360
    # convert to polar coordinates
    radius = Decimal(math.sqrt(x_offset * x_offset + y_offset * y_offset))
    theta = Decimal(0)
    if x_offset == 0:
        theta = Decimal(90)
    else:
        theta = Decimal(math.degrees(math.atan(y_offset/x_offset)))
    # quadrant correction
    if x_offset < 0:
        theta += 180
    if x_offset > 0 and y_offset < 0:
        theta += 360
    # rotate by azimuth
    cardinal_theta = theta - corrected_y_azimuth
    east_offset = Decimal(radius * Decimal(math.cos((math.radians(cardinal_theta)))))
    north_offset = Decimal(radius * Decimal(math.sin(math.radians(cardinal_theta))))
    return round_up(east_offset), round_up(north_offset)


def get_property(properties: List[Property], property_name: str) -> Optional[float]:
    for prop in properties:
        if prop.name == property_name:
            return float(prop.value)
    return None


def round_up(value):
    two_places = Decimal('1e-2')
    return Decimal(value).quantize(two_places, rounding=ROUND_UP)


if __name__ == '__main__':
    (east_offset, north_offset) = get_cardinal_offsets(45., 315., .25, .42)
    print(f'east: {east_offset} north: {north_offset}')
