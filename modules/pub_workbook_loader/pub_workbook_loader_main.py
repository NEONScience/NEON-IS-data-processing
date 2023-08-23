#!/usr/bin/env python3
""" Publication workbook loader module
This module looks loads one or more publication workbooks from the database and writes 
them to tab-delimited files. 

Input parameters are specified in environment variables as follows:
    OUT_PATH_WORKBOOK: The parent path to place the publication workbook files. This code
        will write the publication workbook to the file:
            <OUT_PATH_WORKBOOK>/publication_workbook_<PRODUCT>.txt
        where PRODUCT is an individual data product ID in input parameter PRODUCTS
    PRODUCTS: A comma-separated list (no spaces) of data product identifiers to 
        evaluate in existing publication records. This should include any and all 
        products that the current processing is expected to control 
        publication visibility for. 
            Example: NEON.DOM.SITE.DP1.20015.001,NEON.DOM.SITE.DP1.00066.001
    LOG_LEVEL: The logging level to report at. Options are: 'DEBUG','INFO',
        'WARN','ERROR','FATAL'
""" 
# ---------------------------------------------------------------------------
import os
import environs
import structlog
from pathlib import Path
from contextlib import closing
from functools import partial

import common.log_config as log_config
from data_access.db_connector import DbConnector
from data_access.db_config_reader import read_from_mount
from data_access.get_pub_workbook import get_pub_workbook
from pub_workbook_loader.pub_workbook_loader import load_pub_workbook

log = structlog.get_logger()


def main() -> None:
    # Parse input parameters and initialize
    env = environs.Env()
    log_level: str = os.environ['LOG_LEVEL']
    log_config.configure(log_level)
    db_config = read_from_mount(Path('/var/db_secret'))

    dp_ids = env.list('PRODUCTS')
    out_path: Path = Path(os.environ['OUT_PATH_WORKBOOK'])
    out_path.mkdir(parents=True, exist_ok=True)

    with closing(DbConnector(db_config)) as connector:
        get_pub_workbook_partial = partial(get_pub_workbook,connector=connector)
        load_pub_workbook(get_pub_workbook = get_pub_workbook_partial,
                          out_path = out_path,
                          dp_ids = dp_ids)

if __name__ == "__main__":
    main()
