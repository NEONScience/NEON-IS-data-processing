from collections import defaultdict

import structlog

from data_access.db_connector import DbConnector


log = structlog.get_logger()


def write_results(connector: DbConnector, paths_by_repo: defaultdict[str, list[str]]):
    # TODO: implement when the database table becomes available.
    for key in paths_by_repo.keys():
        log.debug(f'repo: {key} error file count: {len(paths_by_repo[key])}')
