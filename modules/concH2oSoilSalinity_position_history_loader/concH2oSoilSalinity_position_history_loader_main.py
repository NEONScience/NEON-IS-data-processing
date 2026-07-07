#!/usr/bin/env python3
from contextlib import closing
from functools import partial
from pathlib import Path

import environs
import structlog

import common.log_config as log_config
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
from data_access.get_enviroscan_asset_installs import get_enviroscan_asset_installs
from data_access.get_enviroscan_cvald1_calibrations import get_enviroscan_cvald1_calibrations
from data_access.get_enviroscan_cfgloc_vers import get_enviroscan_cfgloc_vers
from data_access.get_named_location_geolocations import get_named_location_geolocations
from data_access.get_named_location_parents import get_named_location_parents

import concH2oSoilSalinity_position_history_loader.concH2oSoilSalinity_position_history_loader as loader

log = structlog.get_logger()


def main() -> None:
    env = environs.Env()
    out_path: Path = env.path('OUT_PATH')
    err_path: Path = env.path('ERR_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    log.debug(f'out_path: {out_path}')

    db_config = read_from_mount(Path('/var/db_secret'))
    with closing(DbConnector(db_config)) as connector:
        loader.write_files(
            get_asset_installs=partial(get_enviroscan_asset_installs, connector),
            get_calibrations=partial(get_enviroscan_cvald1_calibrations, connector),
            get_cfgloc_vers=partial(get_enviroscan_cfgloc_vers, connector),
            get_geolocations=partial(get_named_location_geolocations, connector),
            get_parents=partial(get_named_location_parents, connector),
            out_path=out_path,
            err_path=err_path,
        )


if __name__ == '__main__':
    main()
