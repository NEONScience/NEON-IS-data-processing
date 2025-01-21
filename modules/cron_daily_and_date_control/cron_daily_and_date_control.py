#!usr/bin/env python3
import os
from pathlib import Path
from structlog import get_logger
from datetime import date,timedelta,datetime
import json
import pandas as pd
from cron_daily_and_date_control.cron_daily_and_date_control_config import Config

log = get_logger()


class DateControl:

  def __init__(self, config: Config) -> None:
    self.site_file_path = config.site_file_path
    self.out_path = config.out_path
    self.source_type = config.source_type
    self.start_date = config.start_date
    self.end_date = config.end_date


  def populate_site_dates(self) -> None:
  
    # Load the site list
    with open(self.site_file_path, 'r') as site_file_json:
      site_list=json.load(site_file_json)
      for entry in site_list:
        # Reinitialize
        start_date_trino=None
        end_date_trino=None
        start_date_kafka=None
        end_date_kafka=None
        site_start_date=None
        kafka_site_start_date=None
        
        # Extract info from entry
        keys=entry.keys()
        site=entry['site']
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
        
        # Preliminary logic for start and end dates
        if (self.start_date is None) & (site_start_date is None) & (kafka_site_start_date is None):
          # Nothing to do
          log.warn(f'No global START_DATE and no site-specific or kafka start dates indicated for {site}. Skipping."')
          continue
        
        # Make sure site start date is never None
        if (self.start_date is None) & (site_start_date is None):
          site_start_date=kafka_site_start_date
        elif site_start_date is None:
          site_start_date=self.start_date
          log.debug(f'Setting site start date for {site} to global start date {self.start_date.strftime("%Y-%m-%d")}')
            
        # Make sure site start date is on or after global start date
        if (self.start_date is not None) and (site_start_date < self.start_date):
          site_start_date=self.start_date
            
        # Make sure kafka site start date is on or after global start date
        if (self.start_date is not None) and (kafka_site_start_date is not None) and (kafka_site_start_date < self.start_date):
          kafka_site_start_date=self.start_date
            
        # Make sure site end date is never null
        if site_end_date is None:
          site_end_date=self.end_date
            
        # Make sure site end date is on or before global end date
        if site_end_date > self.end_date:
          site_end_date=self.end_date
        
        # Now we can ignore global start and end dates, paying attention only to site start/end and kafka start dates
        
        # Make sure site end date is on or after site start date
        if site_end_date < site_start_date:
          # Nothing to do
          log.info(f'Start date {site_start_date.strftime("%Y-%m-%d")} for {site} is after end date {site_end_date.strftime("%Y-%m-%d")} for {site}. Skipping.')
          continue
        
        # Make sure kafka start date is on or after site start date 
        if (kafka_site_start_date is not None) and (kafka_site_start_date < site_start_date):
          kafka_site_start_date=site_start_date;
          log.debug(f'Adjusting Kafka start date for {site} to {site_start_date.strftime("%Y-%m-%d")}.')
        
        # Determine dates for kafka data vs. trino data
        if (kafka_site_start_date is None) or (kafka_site_start_date > site_end_date):
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
            date_path=Path(self.out_path,self.source_type,d.strftime("%Y/%m/%d"))
            date_path.mkdir(parents=True, exist_ok=True)
            site_date_path = Path(date_path,site)
            with site_date_path.open('w') as file:
              rpt=file.write(site)
           
        # Create daily folder structure for kafka data
        if start_date_kafka is not None:
          kafka_date_range=pd.date_range(start=start_date_kafka,end=end_date_kafka)
          for d in kafka_date_range:
            date_path=Path(self.out_path,self.source_type,d.strftime("%Y/%m/%d"))
            date_path.mkdir(parents=True, exist_ok=True)
            site_date_file_path = Path(date_path,site+'.kafka')
            with site_date_file_path.open('w') as file:
              rpt=file.write(site+'.kafka')
           
        # Finally, create a file listing the data years (for the metadata assignment pipelines)      
        years=os.listdir(Path(self.out_path,self.source_type)) 
        for year in years:
          year_file_path=Path(self.out_path,'data_year_'+year+'.txt')
          with year_file_path.open('w') as file:
            rpt=file.write(year)
          
