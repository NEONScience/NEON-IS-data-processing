from typing import Optional

import eml.eml_2_2_0 as eml

from pub_files.geometry import Geometry
from pub_files.input_files.file_metadata import FileMetadata


def get_geographic_coverage(geometry: Geometry, metadata: FileMetadata, unit_name: str) -> eml.GeographicCoverage:
    """Returns the populated geographic coverage object."""

    def get_bounding_coordinates():
        """Returns the geographic bounding coordinates."""
        bounding_coordinates = eml.GeographicCoverageBoundingCoordinates()
        bounding_coordinates.east_bounding_coordinate = None
        bounding_coordinates.west_bounding_coordinate = None
        bounding_coordinates.north_bounding_coordinate = None
        bounding_coordinates.south_bounding_coordinate = None
        if geometry.longitude is not None:
            bounding_coordinates.east_bounding_coordinate = round(geometry.longitude, 6)
            bounding_coordinates.west_bounding_coordinate = round(geometry.longitude, 6)
        if geometry.latitude is not None:
            bounding_coordinates.north_bounding_coordinate = round(geometry.latitude, 6)
            bounding_coordinates.south_bounding_coordinate = round(geometry.latitude, 6)
        bounding_coordinates.bounding_altitudes = get_bounding_altitudes()
        return bounding_coordinates

    def get_bounding_altitudes():
        """Returns the geographic bounding altitudes."""
        bounding_altitudes = eml.GeographicCoverageBoundingCoordinatesBoundingAltitudes()
        bounding_altitudes.altitude_minimum = None
        bounding_altitudes.altitude_maximum = None
        if geometry.elevation is not None:
            bounding_altitudes.altitude_minimum = round(geometry.elevation, 6)
            bounding_altitudes.altitude_maximum = round(geometry.elevation, 6)
        bounding_altitudes.altitude_units = get_unit(unit_name)
        return bounding_altitudes

    domain = metadata.path_elements.domain
    site = metadata.path_elements.site
    geographic_coverage = eml.GeographicCoverage()
    geographic_coverage.id.append(site)
    geographic_coverage.geographic_description = f'{domain}, {site}'
    geographic_coverage.bounding_coordinates = get_bounding_coordinates()
    return geographic_coverage


def get_unit(unit_name: str) -> Optional[eml.LengthUnitType]:
    """Returns the unit name for the given spatial reference identifier."""
    if unit_name is not None and (unit_name.lower() == 'metre' or unit_name.lower() == 'meter'):
        return eml.LengthUnitType.METER
    return None
