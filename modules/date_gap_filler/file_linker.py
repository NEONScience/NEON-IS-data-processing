#!/usr/bin/env python3
from pathlib import Path
from calendar import monthrange
import structlog
import shutil

from date_gap_filler.dates_between import date_is_between
from date_gap_filler.date_gap_filler_config import DateGapFillerConfig
from date_gap_filler.location_path_parser import LocationPathParser
from date_gap_filler.data_path_parser import DataPathParser
import date_gap_filler.empty_files as empty_files

log = structlog.get_logger()


class FileLinker:
    """
    Class to link location files, empty_files, and data files from the input repositories into the output
    repository. 
    """

    def __init__(self, config: DateGapFillerConfig) -> None:
        self.config = config
        self.out_path = config.out_path
        self.location_path = config.location_path
        self.location_path_parser = LocationPathParser(config)
        self.data_path = config.data_path
        self.data_path_parser = DataPathParser(config)
        self.start_date = config.start_date
        self.end_date = config.end_date
        self.location_dir = config.location_dir
        self.symlink = config.symlink

    def link_files(self) -> None:
        """Process and link the location files, link available data files, and fill date gaps with empty files."""
        for path in self.location_path.rglob('*'):
            if path.is_file():
                log.debug(f'processing location file: {path}')
                source_type, year, month, day, location = self.location_path_parser.parse(path)
                if not date_is_between(year=int(year), month=int(month), day=int(day),
                                       start_date=self.start_date, end_date=self.end_date):
                    continue
                root_link_path = Path(self.out_path, source_type, year, month, day, location)
                
                # Link location file
                self.link_location(root_link_path, path)

                # Link any data files available for this location
                sub_data_path_count = 0
                log.debug(f'Data path: {self.data_path}')
                if self.data_path is not None:
                    repo = Path(*self.data_path.parts[0:3])
                    root_data_path = self.get_data_path(repo, source_type, year, month, day, location)
                    for sub_data_path in root_data_path.rglob('*'):
                        if sub_data_path.is_file():
                            self.link_data(root_link_path, sub_data_path)
                            sub_data_path_count += 1

                
                # If no data has been linked from the data_path input, link the empty files
                if sub_data_path_count == 0:
                    empty_files.link_files(self.config, root_link_path, location, year, month, day)

    def link_location(self, root_link_path: Path, path: Path) -> None:
        location_link = Path(root_link_path, self.location_dir, path.name)
        location_link.parent.mkdir(parents=True, exist_ok=True)
        if not location_link.exists():
            if self.symlink:
                log.debug(f'Linking path {location_link} to {path}.')
                location_link.symlink_to(path)
            else:
                log.debug(f'Copying {path} to {location_link}.')
                shutil.copy2(path,location_link)


    def link_data(self, root_link_path: Path, sub_data_path: Path) -> None:
        source_type, year, month, day, location, data_type = self.data_path_parser.parse(sub_data_path)
        data_link = Path(root_link_path, data_type, sub_data_path.name)
        data_link.parent.mkdir(parents=True, exist_ok=True)
        if not data_link.exists():
            if self.symlink:
                log.debug(f'Linking path {data_link} to {sub_data_path}.')
                data_link.symlink_to(sub_data_path)
            else:
                log.debug(f'Copying {sub_data_path} to {data_link}.')
                shutil.copy2(sub_data_path,data_link)


    def get_data_path(self,repo:Path,source_type:str,year:str,month:str,day:str,location:str) -> Path:
        # Construct the path to locations in the DATA_PATH input repo
        source_type_index=self.config.data_source_type_index
        year_index=self.config.data_year_index
        month_index=self.config.data_month_index
        day_index=self.config.data_day_index
        location_index=self.config.data_location_index
        path_parts_tuple=[
            (source_type,source_type_index),
            (year,year_index), 
            (month,month_index),
            (day, day_index),
            (location,location_index)
            ]
        path_parts_tuple.sort(key=self.takeSecond)
        data_path=Path(repo,*list(zip(*path_parts_tuple))[0])
        return data_path

    # take second element for sort
    def takeSecond(self,elem):
        return elem[1]
