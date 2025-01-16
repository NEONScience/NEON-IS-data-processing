#!usr/bin/env python3
import os
import environs
from pathlib import Path
from structlog import get_logger
import requests
import common.log_config as log_config
from datetime import date,timedelta,datetime
import json
import pandas as pd

log = get_logger()


def populate_site_dates() -> None:
  env = environs.Env()
  log_level: str = os.getenv('LOG_LEVEL','INFO')
  log_config.configure(log_level)
  out_path: Path = env.path('OUT_PATH')
  source_type: str = os.environ['SOURCE_TYPE']
  lag_days_end: str = os.getenv('LAG_DAYS_END','2')
  start_date: str = os.getenv('START_DATE')
  end_date: str = os.getenv('END_DATE')
  site_file_path: Path = env.path('SITE_FILE')
  
  log.info(f'source Type: {source_type}')
  log.debug(f'site_file: {site_file_path}')
  log.debug(f'out_path: {out_path}')
  
  
  # If global END_DATE unset, set to X days ago (default 2)
  log.info(f'Global start date: {start_date}')
  if start_date is not None:
      start_date=datetime.strptime(f"{start_date}", "%Y-%m-%d")
  
  if end_date is None:
      lag_days_end=int(lag_days_end)
      end_date=date.today() - timedelta(days=lag_days_end)
      log.info(f'Input END_DATE is unset. Using {lag_days_end} days previous.')
  else:
      end_date=datetime.strptime(f"{end_date}", "%Y-%m-%d")
  
  log.info(f'Global end date: {end_date.strftime("%Y-%m-%d")}')


  # Load the site list
  with open(site_file_path, 'r') as site_file_json:
    site_list=json.load(site_file_json)
    for entry in site_list:
      keys=entry.keys()
      site=entry['site']
      site_start_date=None
      kafka_site_start_date=None
      site_end_date=None
      if 'start_date' in keys:
        site_start_date=entry['start_date']
        site_start_date=datetime.strptime(f"{site_start_date}", "%Y-%m-%d")
      if 'kafka_start_date' in keys:
        kafka_site_start_date=entry['kafka_start_date']
        kafka_site_start_date=datetime.strptime(f"{kafka_site_start_date}", "%Y-%m-%d")
      if 'end_date' in keys:
        site_end_date=entry['end_date']
        site_end_date=datetime.strptime(f"{site_end_date}", "%Y-%m-%d")
      
      # Reinitialize
      start_date_trino=None
      end_date_trino=None
      start_date_kafka=None
      end_date_kafka=None
      
      # Preliminary logic for start and end dates
      if (start_date is None) & (site_start_date is None) & (kafka_site_start_date is None):
        # Nothing to do
        log.warn(f'No global START_DATE and no site-specific or kafka start dates indicated for {site}. Skipping."')
        continue
      
      # Make sure site start date is never None
      if (start_date is None) & (site_start_date is None):
        site_start_date=kafka_site_start_date
      elif site_start_date is None:
        site_start_date=start_date
        log.debug(f'Setting site start date for {site} to global start date {start_date.strftime("%Y-%m-%d")}')
          
      # Make sure site start date is on or after global start date
      if (start_date is not None) & (site_start_date < start_date):
        site_start_date=start_date
          
      # Make sure kafka site start date is on or after global start date
      if (start_date is not None) & (kafka_site_start_date is not None) & (kafka_site_start_date < start_date):
        kafka_site_start_date=start_date
          
      # Make sure site end date is never null
      if site_end_date is None:
        site_end_date=end_date
          
      # Make sure site end date is on or before global end date
      if site_end_date > end_date:
        site_end_date=end_date
      
      # Now we can ignore global start and end dates, paying attention only to site start/end and kafka start dates
      
      # Make sure site end date is on or after site start date
      if site_end_date < site_start_date:
        # Nothing to do
        log.info(f'Start date {site_start_date.strftime("%Y-%m-%d")} for {site} is after end date {site_end_date.strftime("%Y-%m-%d")} for {site}. Skipping.')
        continue
      
      # Make sure kafka start date is on or after site start date 
      if (kafka_site_start_date is not None) & (kafka_site_start_date < site_start_date):
        kafka_site_start_date=site_start_date;
        log.debug(f'Adjusting Kafka start date for {site} to {site_start_date.strftime("%Y-%m-%d")}.')
      
      # Determine dates for kafka data vs. trino data
      if (kafka_site_start_date is None) | (kafka_site_start_date > site_end_date):
        # Trino only
        start_date_trino=site_start_date
        end_date_trino=site_end_date
      elif (kafka_site_start_date == site_start_date):
        # Kafka only
        start_date_kafka=kafka_site_start_date
        end_date_kafka=site_end_date
      else:
        start_date_trino=site_start_date
        end_date_trino=kafka_site_start_date - timedelta(days=1)
        start_date_kafka=kafka_site_start_date
        end_date_kafka=site_end_date
      log.info(f'Import triggers for {site}: Trino start: {start_date_trino}; Trino end: {end_date_trino}; Kafka start: {start_date_kafka}; Kafka end: {end_date_kafka}')
      
      # Create daily folder structure for trino data
      log.debug(f'Creating import trigger folder structure for {site}')
      if start_date_trino is not None:
        trino_date_range=pd.date_range(start=start_date_trino,end=end_date_trino)
        for d in trino_date_range:
          date_path=Path(out_path,source_type,d.strftime("%Y/%m/%d"))
          date_path.mkdir(parents=True, exist_ok=True)
          site_date_path = Path(date_path,site)
          with site_date_path.open('w') as file:
            rpt=file.write(site)
         
      # Create daily folder structure for kafka data
      if start_date_kafka is not None:
        kafka_date_range=pd.date_range(start=start_date_kafka,end=end_date_kafka)
        for d in kafka_date_range:
          date_path=Path(out_path,source_type,d.strftime("%Y/%m/%d"))
          date_path.mkdir(parents=True, exist_ok=True)
          site_date_file_path = Path(date_path,site+'.kafka')
          with site_date_file_path.open('w') as file:
            rpt=file.write(site+'.kafka')
         
      # Finally, create a file listing the data years (for the metadata assignment pipelines)      
      years=os.listdir(Path(out_path,source_type)) 
      for year in years:
        year_file_path=Path(out_path,'data_year_'+year+'.txt')
        with year_file_path.open('w') as file:
          rpt=file.write(year)
        

if __name__ == '__main__':
    populate_site_dates()
