#!/usr/bin/env python3
import os
import environs
import structlog
from pathlib import Path
from datetime import date,timedelta,datetime

from common import log_config as log_config
from cron_daily_and_date_control.cron_daily_and_date_control_config import Config
from cron_daily_and_date_control.cron_daily_and_date_control import DateControl


def main() -> None:
    env = environs.Env()
    site_file_path: Path = env.path('SITE_FILE')
    out_path: Path = env.path('OUT_PATH')
    source_type: str = os.environ['SOURCE_TYPE']
    start_date: str = os.getenv('START_DATE')
    end_date: str = os.getenv('END_DATE')
    lag_days_end: int = env.int('LAG_DAYS_END',2)
    log_level: str = os.getenv('LOG_LEVEL','INFO')
    log_config.configure(log_level)
    log = structlog.get_logger()
    
    
    log.info(f'source Type: {source_type}')
    log.debug(f'site_file: {site_file_path}')
    log.debug(f'out_path: {out_path}')
    
    
    # If global END_DATE unset, set to lag_days_end days ago (default 2)
    log.info(f'Global start date: {start_date}')
    if start_date is not None:
        start_date=datetime.strptime(f"{start_date}", "%Y-%m-%d")
    
    if end_date is None:
        end_date=date.today() - timedelta(days=lag_days_end)
        log.info(f'Input END_DATE is unset. Using {lag_days_end} days previous.')
    else:
        end_date=datetime.strptime(f"{end_date}", "%Y-%m-%d")
    
    log.info(f'Global end date: {end_date.strftime("%Y-%m-%d")}')
    
    config = Config(site_file_path=site_file_path,
                    out_path=out_path,
                    source_type=source_type,
                    start_date=start_date,
                    end_date=end_date)
    date_control = DateControl(config)
    date_control.populate_site_dates()


if __name__ == '__main__':
    main()
