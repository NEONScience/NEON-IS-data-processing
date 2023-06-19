from datetime import datetime
from typing import NamedTuple, Callable

from data_access.db_connector import DbConnector
from pub_files.database.file_variables import FileVariables, make_get_is_science_review_variables
from pub_files.database.science_review_flags import ScienceReviewFlag, make_get_flags
from pub_files.database.terms import make_get_term_name, make_get_term_number


class ScienceReviewDatabase(NamedTuple):
    get_flags: Callable[[str, str, datetime, datetime], list[ScienceReviewFlag]]
    get_variables: Callable[[], list[FileVariables]]
    get_term_name: Callable[[str], str]
    get_term_number: Callable[[str], str]


def get_science_review_database(connector: DbConnector) -> ScienceReviewDatabase:
    return ScienceReviewDatabase(get_flags=make_get_flags(connector),
                                 get_variables=make_get_is_science_review_variables(connector),
                                 get_term_name=make_get_term_name(connector),
                                 get_term_number=make_get_term_number(connector))
