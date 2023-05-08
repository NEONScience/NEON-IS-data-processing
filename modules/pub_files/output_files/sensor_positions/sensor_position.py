import math
from decimal import Decimal, ROUND_UP
from typing import List, Optional, NamedTuple, Tuple

import structlog

from data_access.types.property import Property
from pub_files.database.geolocations import GeoLocation

log = structlog.get_logger()


class SensorPosition(NamedTuple):
    """The calculated sensor position."""
    north_offset: Optional[Decimal]
    east_offset: Optional[Decimal]
    x_azimuth: Optional[float]
    y_azimuth: Optional[float]


def get_position(g: GeoLocation, x_offset, y_offset) -> SensorPosition:
    """
    Return the sensor position.

    :param g: The reference geolocation data.
    :param x_offset: The x offset of the sensor from the reference location.
    :param y_offset: The y offset of the sensor from the reference location.
    """
    x_azimuth = get_property(g.properties, 'x Azimuth Angle')
    y_azimuth = get_property(g.properties, 'y Azimuth Angle')
    azimuth_is_zero = False
    if (x_azimuth is None or x_azimuth == 0) or (y_azimuth is None or y_azimuth == 0):
        azimuth_is_zero = True
    if not azimuth_is_zero:
        if x_azimuth is None:
            x_azimuth = Decimal(0)
        if y_azimuth is None:
            y_azimuth = Decimal(0)
        (east_offset, north_offset) = get_cardinal_offsets(x_azimuth, y_azimuth, x_offset, y_offset)
        if north_offset == -0:
            north_offset = abs(north_offset)
        if east_offset == -0:
            east_offset = abs(east_offset)
        # return calculated values
        return SensorPosition(north_offset=north_offset,
                              east_offset=east_offset,
                              x_azimuth=x_azimuth,
                              y_azimuth=y_azimuth)
    # return defaults
    return SensorPosition(north_offset=y_offset,
                          east_offset=x_offset,
                          x_azimuth=0,
                          y_azimuth=0)


def get_cardinal_offsets(x_azimuth, y_azimuth, x_offset, y_offset) -> Tuple[Decimal, Decimal]:
    """
    Return the cardinal offset.

    :param x_azimuth: The geolocation x azimuth.
    :param y_azimuth: The geolocation y azimuth.
    :param x_offset: The sensor x offset from the reference location.
    :param y_offset: The sensor y offset from the reference location.
    """
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
    return east_offset, north_offset


def get_property(properties: List[Property], property_name: str) -> Optional[float]:
    """Get a property by name from the property list."""
    for prop in properties:
        if prop.name == property_name:
            return float(prop.value)
    return None


def round_up_two_places(value):
    """Round up with two signification digits after the decimal."""
    return Decimal(value).quantize(Decimal('1e-2'), rounding=ROUND_UP)


def test():
    (east_offset, north_offset) = get_cardinal_offsets(45., 315., .25, .42)
    print(f'east: {east_offset} north: {north_offset}')
    (east_offset, north_offset) = get_cardinal_offsets(45., 315., 0, 0)
    print(f'east: {east_offset} north: {north_offset}')
