from functools import partial

from data_access.db_connector import DbConnector
from data_access.get_thresholds import get_thresholds
from pub_files.database.queries.geolocation_geometry import get_geometry
from pub_files.database.queries.named_locations import get_named_location
from pub_files.database.queries.spatial_units import get_spatial_unit
from pub_files.database.queries.units import get_unit_eml_type
from pub_files.database.queries.value_list import get_value_list
from pub_files.output_files.eml.eml_database import EmlDatabase


class EmlData:

    def __init__(self, connector: DbConnector) -> None:
        self.get_geometry = partial(get_geometry, connector)
        self.get_named_location = partial(get_named_location, connector)
        self.get_spatial_unit = partial(get_spatial_unit, connector)
        self.get_value_list = partial(get_value_list, connector)
        self.get_thresholds = partial(get_thresholds, connector)
        self.get_unit_eml_type = partial(get_unit_eml_type, connector)

    def get_database(self) -> EmlDatabase:
        return EmlDatabase(get_named_location=self.get_named_location,
                           get_geometry=self.get_geometry,
                           get_spatial_unit=self.get_spatial_unit,
                           get_value_list=self.get_value_list,
                           get_thresholds=self.get_thresholds,
                           get_unit_eml_type=self.get_unit_eml_type)
