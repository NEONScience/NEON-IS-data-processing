from typing import NamedTuple, Callable, List

from data_access.db_connector import DbConnector
from pub_files.database.file_variables import make_get_sensor_position_variables, FileVariables
from pub_files.database.term_variables import make_get_term_variables, TermVariables


class VariablesDatabase(NamedTuple):
    get_sensor_position_variables: Callable[[], List[FileVariables]]
    get_term_variables: Callable[[str, str], TermVariables]


def get_variables_database(connector: DbConnector) -> VariablesDatabase:
    return VariablesDatabase(get_sensor_position_variables=make_get_sensor_position_variables(connector),
                             get_term_variables=make_get_term_variables(connector))
