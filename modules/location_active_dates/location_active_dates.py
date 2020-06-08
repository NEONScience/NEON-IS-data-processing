#!/usr/bin/env python3
from pathlib import Path
from datetime import datetime

import geojson
import environs
import structlog

import common.date_formatter as date_formatter
import common.log_config as log_config
from common.file_crawler import crawl
from common.file_linker import link


log = structlog.get_logger()


def link_files(location_path: Path, out_path: Path, schema_index: int):
    """
    Link a location file for each active date with path '/<schema>/yyyy/mm/dd/<location name>/<filename>'.

    :param location_path: The location file path.
    :param out_path: The output directory root path.
    :param schema_index: The file path index of the schema name.
    """
    for path in crawl(location_path):
        parts = path.parts
        schema_name = parts[schema_index]
        with open(path, 'r') as file:
            geojson_data = geojson.load(file)
            features = geojson_data['features']
            properties = features[0]['properties']
            location_name = properties['name']
            active_periods = properties['active_periods']
            # link file for each active date
            for period in active_periods:
                start_date = period['start_date']
                end_date = period['end_date']
                log.debug(f'start_date: {start_date} end_date: {end_date}')
                if start_date is not None:
                    start_date = date_formatter.parse(start_date)
                if end_date is not None:
                    end_date = date_formatter.parse(end_date)
                for date in date_formatter.dates_between(start_date, end_date):
                    dt = datetime(date.year, date.month, date.day)
                    year, month, day = date_formatter.parse_date(dt)
                    link_path = Path(out_path, schema_name, year, month, location_name, path.name)
                    link(path, link_path)


def main():
    env = environs.Env()
    location_path = env.path('LOCATION_PATH')
    out_path = env.path('OUT_PATH')
    schema_index = env.int('SCHEMA_INDEX')
    log_level = env.log_level('LOG_LEVEL')
    log_config.configure(log_level)
    log.info('Processing.')
    link_files(location_path, out_path, schema_index)


if __name__ == "__main__":
    main()
