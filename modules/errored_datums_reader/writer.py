from collections import defaultdict

import structlog

from data_access.db_connector import DbConnector


log = structlog.get_logger()


def write_results(connector: DbConnector, files_by_pipeline: defaultdict[str, list[str]]):
    # TODO: implement when the database table becomes available.
    for pipeline_name in files_by_pipeline.keys():
        file_count = len(files_by_pipeline[pipeline_name])
        log.debug(f'pipeline: {pipeline_name} error file count: {file_count}')
