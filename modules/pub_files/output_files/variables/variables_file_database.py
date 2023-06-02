from functools import partial
from typing import NamedTuple, Callable, List

import pub_files.database.file_variables as queries
from data_access.db_connector import DbConnector
from pub_files.database.file_variables import FileVariables


class VariablesDatabase(NamedTuple):
    """Class to consolidate functions for reading needed data for the variables file from the database."""
    get_sensor_positions: Callable[[], List[FileVariables]]
    get_is_science_review: Callable[[], List[FileVariables]]
    get_sae_science_review: Callable[[], List[FileVariables]]


def get_variables_database(connector: DbConnector) -> VariablesDatabase:
    """Populate the database object with functions hiding the database connection from calling clients."""
    get_sensor_positions = partial(queries.get_sensor_positions, connector)
    get_is_science_review = partial(queries.get_is_science_review, connector)
    get_sae_science_review = partial(queries.get_sae_science_review, connector)
    return VariablesDatabase(get_sensor_positions=get_sensor_positions,
                             get_is_science_review=get_is_science_review,
                             get_sae_science_review=get_sae_science_review)
