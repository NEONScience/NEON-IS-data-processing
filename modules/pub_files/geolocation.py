import math
import dataclasses
from datetime import datetime
from typing import List, Tuple

import common.date_formatter
from data_access.types.property import Property


@dataclasses.dataclass(frozen=True)
class GeoLocation:
    location_id: int
    geometry: str
    start_date: datetime
    end_date: datetime
    alpha: float
    beta: float
    gamma: float
    x_offset: float
    y_offset: float
    z_offset: float
    offset_id: int
    offset_name: str
    offset_description: str
    properties: List[Property]

    def get_dates(self) -> Tuple[str, str]:
        start_date = self.start_date
        if start_date is not None:
            start = common.date_formatter.to_string(start_date)
        else:
            start = ''
        end_date = self.end_date
        if end_date is not None:
            end = common.date_formatter.to_string(end_date)
        else:
            end = ''
        return start, end

    def get_offsets(self) -> Tuple[float, float]:
        """Calculate east and north offsets."""
        # convert to polar coordinates
        radius = math.sqrt(self.x_offset * self.x_offset + self.y_offset * self.y_offset)
        theta = self._get_theta()
        corrected_y_azimuth = self._correct_y_azimuth()
        cardinal_theta = theta - corrected_y_azimuth
        east_offset = radius * math.cos(math.radians(cardinal_theta))
        north_offset = radius * math.sin(math.radians(cardinal_theta))
        return east_offset, north_offset

    def get_azimuth_values(self) -> Tuple[float, float]:
        x_azimuth = 0
        y_azimuth = 0
        for prop in self.properties:
            if prop.name == 'x Azimuth Angle':
                x_azimuth = float(prop.value)
            if prop.name == 'y Azimuth Angle':
                y_azimuth = float(prop.value)
        return x_azimuth, y_azimuth

    def _correct_y_azimuth(self) -> float:
        (x_azimuth, y_azimuth) = self.get_azimuth_values()
        corrected_y_azimuth = y_azimuth
        if y_azimuth < x_azimuth:
            diff = x_azimuth - y_azimuth
        else:
            diff = 360 - x_azimuth + y_azimuth
        if diff > 90:
            delta = diff - 90
            corrected_y_azimuth = 0.5 * delta + y_azimuth
            if corrected_y_azimuth >= 360:
                corrected_y_azimuth -= 360
        if diff < 90:
            delta = 90 - diff
            corrected_y_azimuth = y_azimuth - 0.5 * delta
            if corrected_y_azimuth < 0:
                corrected_y_azimuth += 360
        return corrected_y_azimuth

    def _get_theta(self) -> float:
        if self.x_offset == 0:
            theta = 90.
        else:
            theta = math.degrees(math.atan(self.y_offset / self.x_offset))
        # quadrant correction
        if self.x_offset < 0:
            theta += 180
        if self.x_offset > 0 > self.y_offset:
            theta += 360
        return theta
