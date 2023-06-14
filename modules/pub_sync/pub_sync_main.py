#!/usr/bin/env python3
""" Publication sync module
This module looks for publication records in the database (within time and site constraints)
for which current processing has not output any data, indicating that the associated 
pub packages should not be accessible in LATEST data. Any LATEST records are removed
and replaced with new LATEST records with a status of NODATA.

Input parameters are specified in environment variables as follows:
    DATE_PATH: The path to the repository that shows the date range of publication
        records to evaluate. Example of the path structure:
            /date-repo/2023/01
        The dates in this repository structure represent the dataIntervalStart in 
        publication records. 
        Note that the parent of the lowest of (DATE_PATH_YEAR_INDEX,
        DATE_PATH_MONTH_INDEX) will serve as the starting point to determine the
        total date range to evaluate.
    DATE_PATH_YEAR_INDEX: The path index to the year field in DATE_PATH. In the 
        DATE_PATH example above, the DATE_PATH_YEAR_INDEX = 2
    DATE_PATH_MONTH_INDEX: The path index to the month field in DATE_PATH. In the 
        DATE_PATH example above, the DATE_PATH_MONTH_INDEX = 3
    DATA_PATH: The path to the repository that shows what data have egressed to 
        the publication bucket. This repo does not need to have any data in it, 
        simply the path structure that reflects the product-site-month-packages 
        that have been published. Example path structure:
            /data-repo/NEON.DOM.SITE.DP1.00066.001/CPER/20230201T000000-20230301T000000/basic
        Note that the parent of the lowest of (DATA_PATH_PRODUCT_INDEX,
        DATA_PATH_SITE_INDEX,DATA_PATH_DATE_INDEX,DATA_PATH_PACKAGE_INDEX) will 
        serve as the starting point to determine the product-site-month-packages
        output by current processing.
    DATA_PATH_PRODUCT_INDEX: The path index to the product field in DATA_PATH. 
        In the DATA_PATH example above, DATA_PATH_PRODUCT_INDEX = 2
    DATA_PATH_SITE_INDEX: The path index to the product field in DATA_PATH. 
        In the DATA_PATH example above, DATA_PATH_SITE_INDEX = 3
    DATA_PATH_DATE_INDEX: The path index to the product field in DATA_PATH. 
        In the DATA_PATH example above, DATA_PATH_DATE_INDEX = 4
    DATA_PATH_PACKAGE_INDEX: The path index to the product field in DATA_PATH. 
        In the DATA_PATH example above, DATA_PATH_PACKAGE_INDEX = 5
    PRODUCTS: A comma-separated list (no spaces) of data product identifiers to 
        evaluate in existing publication records. This should include any and all 
        products that the current processing is expected to control 
        publication visibility for. 
            Example: NEON.DOM.SITE.DP1.20016.001,NEON.DOM.SITE.DP1.20008.001
    SITES: A comma-separated list (no spaces) of site-codes to evaluate in existing 
        publication records. This should include any and all sites that the current
        processing is expected to control publication visibility for. 
            Example: CPER,HARV,BARC
        To evaluate all sites for which publication records exist, set SITES="all"
    CHANGE_BY: A string of the user implementing changes in the database. Will be 
        added to any inserted publication records
    LOG_LEVEL: The logging level to report at. Options are: 'DEBUG','INFO',
        'WARN','ERROR','FATAL'
""" 
# ---------------------------------------------------------------------------
from pathlib import Path
import environs
import structlog
from contextlib import closing
import os
import datetime
from dateutil.relativedelta import relativedelta
import common.log_config as log_config
from common.get_path_key import get_path_key
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
from data_access.get_dp_pub_records import get_dp_pub_records
from data_access.remove_pub import remove_pub

log = structlog.get_logger()

def main() -> None:
    # Parse input parameters and initialize
    env = environs.Env()
    log_level: str = os.environ['LOG_LEVEL']
    log_config.configure(log_level)
    db_config = read_from_mount(Path('/var/db_secret'))
    connector = DbConnector(db_config)
    
    date_path: Path = Path(os.environ['DATE_PATH'])
    if 'DATA_PATH' in os.environ:
        data_path: Path = Path(os.environ['DATA_PATH'])
    else:
        # It is possible that data path will not exist if there are no current publications for the date range
        data_path = None
    date_path_year_index = env.int('DATE_PATH_YEAR_INDEX')
    date_path_month_index = env.int('DATE_PATH_MONTH_INDEX')
    data_path_product_index = env.int('DATA_PATH_PRODUCT_INDEX')
    data_path_site_index = env.int('DATA_PATH_SITE_INDEX')
    data_path_date_index = env.int('DATA_PATH_DATE_INDEX')
    data_path_package_index = env.int('DATA_PATH_PACKAGE_INDEX')
    
    dp_ids = env.list('PRODUCTS')
    sites = env.list('SITES')
    change_by = env.str('CHANGE_BY')

    with closing(DbConnector(db_config)) as connector:
        get_srfs_partial = partial(get_srf_loaders, connector=connector)
        load_srfs(out_path=out_path, get_srfs=get_srfs_partial, group_prefix=group_prefix)

    pub_sync(connector=connector,
             data_path = data_path,
             date_path_year_index = date_path_year_index,
             date_path_month_index = date_path_month_index,
             data_path_product_index = data_path_product_index,
             data_path_site_index = data_path_site_index,
             data_path_date_index = data_path_date_index,
             data_path_package_index = data_path_package_index,
             dp_ids = dp_ids,
             sites = sites,
             change_by = change_by)

  
if __name__ == "__main__":
    main()
