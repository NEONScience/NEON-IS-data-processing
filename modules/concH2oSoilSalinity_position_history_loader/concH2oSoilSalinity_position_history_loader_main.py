#!/usr/bin/env python3
import os
from contextlib import closing
from functools import partial
from pathlib import Path
from typing import List, Optional

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
    # Optional inputs — when the loader runs inline inside a per-site pub pipeline
    # (folded into pub_format_and_package), IN_PATH points at the datum tree so
    # we can derive the site(s) present and site-scope the PDR queries. When the
    # loader runs standalone (cron), these env vars are unset and we fall back to
    # a system-wide query (no site filter).
    in_path: Optional[Path] = env.path('IN_PATH', None)
    relative_path_index: Optional[int] = env.int('RELATIVE_PATH_INDEX', None)
    log_config.configure(log_level)
    log.debug(f'out_path: {out_path}')

    sites: Optional[List[str]] = None
    if in_path is not None and relative_path_index is not None:
        sites = _collect_sites(in_path, relative_path_index)
        if sites:
            log.info(f'site-scoped run: {sites}')
        else:
            log.warning(
                'IN_PATH set but no site directories found at expected depth; '
                'falling back to system-wide query',
                in_path=str(in_path), relative_path_index=relative_path_index,
            )

    db_config = read_from_mount(Path('/var/db_secret'))
    with closing(DbConnector(db_config)) as connector:
        loader.write_files(
            get_asset_installs=partial(get_enviroscan_asset_installs, connector, sites),
            get_calibrations=partial(get_enviroscan_cvald1_calibrations, connector, sites),
            get_cfgloc_vers=partial(get_enviroscan_cfgloc_vers, connector, sites),
            get_geolocations=partial(get_named_location_geolocations, connector),
            get_parents=partial(get_named_location_parents, connector),
            out_path=out_path,
            err_path=err_path,
        )


def _collect_sites(in_path: Path, relative_path_index: int) -> List[str]:
    """
    Walk IN_PATH and extract the set of NEON site codes present. Path shape is
    <prefix>/<DP>/<YEAR>/<MONTH>/<SITE>/..., where <prefix> is `relative_path_index`
    parts deep. Under the folded-in pub_format_and_package model each Pachyderm
    datum is scoped to one site, so this returns a 1-element list — but the set
    shape lets a multi-site datum work too.
    """
    site_index = relative_path_index + 3
    sites = set()
    for root, _, _ in os.walk(in_path):
        parts = Path(root).parts
        if len(parts) == site_index + 1:
            sites.add(parts[site_index])
    return sorted(sites)


if __name__ == '__main__':
    main()
