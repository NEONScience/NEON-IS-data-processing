from typing import Optional

from eml.eml_2_2_0 import GeographicCoverageBoundingCoordinatesBoundingAltitudes, \
    GeographicCoverageBoundingCoordinates, GeographicCoverage, LengthUnitType

from pub_files.output_files.eml.eml_database import EmlDatabase
from pub_files.geometry import Geometry
from pub_files.input_files.file_metadata import FileMetadata


class EmlCoverage:

    def __init__(self, geometry: Geometry, metadata: FileMetadata, database: EmlDatabase):
        self.geometry = geometry
        self.metadata = metadata
        self.database = database

    def get_coverage(self) -> GeographicCoverage:
        domain = self.metadata.path_elements.domain
        site = self.metadata.path_elements.site
        geographic_coverage = GeographicCoverage()
        geographic_coverage.id.append(site)
        geographic_coverage.geographic_description = f'{domain}, {site}'
        geographic_coverage.bounding_coordinates = self.get_bounding_coordinates()
        return geographic_coverage

    def get_bounding_coordinates(self):
        bounding_coordinates = GeographicCoverageBoundingCoordinates()
        bounding_coordinates.east_bounding_coordinate = round(self.geometry.longitude, 6)
        bounding_coordinates.west_bounding_coordinate = round(self.geometry.longitude, 6)
        bounding_coordinates.north_bounding_coordinate = round(self.geometry.latitude, 6)
        bounding_coordinates.south_bounding_coordinate = round(self.geometry.latitude, 6)
        bounding_coordinates.bounding_altitudes = self.get_bounding_altitudes()
        return bounding_coordinates

    def get_bounding_altitudes(self):
        bounding_altitudes = GeographicCoverageBoundingCoordinatesBoundingAltitudes()
        bounding_altitudes.altitude_minimum = round(self.geometry.elevation, 6)
        bounding_altitudes.altitude_maximum = round(self.geometry.elevation, 6)
        bounding_altitudes.altitude_units = self.get_unit(self.geometry.srid)
        return bounding_altitudes

    def get_unit(self, srid: int) -> Optional[LengthUnitType]:
        unit_name = self.database.get_spatial_unit(srid)
        if unit_name.lower() == 'metre' or unit_name.lower() == 'meter':
            return LengthUnitType.METER
        return None
