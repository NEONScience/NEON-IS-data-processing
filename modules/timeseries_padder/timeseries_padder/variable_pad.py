#!/usr/bin/env python3
import datetime
import json
import os
import sys
from pathlib import Path
from typing import Dict, Union, Optional, Any, List

from structlog import get_logger

import timeseries_padder.timeseries_padder.pad_calculator as pad_calculator
import timeseries_padder.timeseries_padder.file_writer as file_writer
from timeseries_padder.timeseries_padder.timeseries_padder_config import Config
from timeseries_padder.timeseries_padder.data_path_parser import DataPathParser


log = get_logger()


class VariablePad:
    """Class to pad data with a variable window."""

    def __init__(self, config: Config) -> None:
        self.data_path = config.data_path
        self.out_path = config.out_path
        self.pad_dirs = config.pad_dirs
        self.copy_dirs = config.copy_dirs
        self.data_file_path = DataPathParser(config)
        self.out_dir_parts = list(self.out_path.parts)
        self.relative_path_index = config.relative_path_index

    def pad(self) -> None:
        """Pad the data with the calculated window size."""
        try:
            # date_path should be based on year/month/day
            active_periods = self.check_active_periods_flag()

            date_and_location_max_window_size: Dict[str, Union[int, float]] = {}
            date_and_location_min_data_rate: Dict[str, float] = {}
            for path in self.data_path.rglob('*'):
                if path.is_file():
                    parts = path.parts
                    year, month, day, location, data_type = self.data_file_path.parse(path)
                    if data_type in self.pad_dirs:
                        date_location_key = year+month+day+location
                        config_location_path = Path(*parts[:self.data_file_path.location_index + 1])
                        # get min of all data rates (to ensure adequate window coverage)
                        if date_location_key not in date_and_location_min_data_rate:
                            location_path = Path(config_location_path, Config.location_dir)
                            location_files = [f for f in os.listdir(location_path)
                                            if f.endswith(Config.location_file_extension)]
                            location_file = Path(location_path, location_files[0])
                            date_and_location_min_data_rate[date_location_key] = \
                                pad_calculator.get_data_rate(str(location_file))
                        data_rate = date_and_location_min_data_rate[date_location_key]
                        # get max of all window sizes
                        if date_location_key not in date_and_location_max_window_size:
                            threshold_path = Path(config_location_path, Config.threshold_dir)
                            threshold_files = [f for f in os.listdir(threshold_path)
                                            if f.endswith(Config.threshold_file_extension)]
                            threshold_file = Path(threshold_path, threshold_files[0])
                            log.debug(f'threshold file: {threshold_file}')
                            date_and_location_max_window_size[date_location_key] = \
                                pad_calculator.get_max_window_size(str(threshold_file), data_rate)
                        window_size = date_and_location_max_window_size[date_location_key]
                        data_date = datetime.date(int(year), int(month), int(day))
                        # calculate pad size
                        pad_size = pad_calculator.calculate_pad_size(window_size)
                        padded_dates = pad_calculator.get_padded_dates(data_date, pad_size)
                        # link data file into each date in the padded range
                        link_parts = list(parts)
                        for index in range(1, len(self.out_dir_parts)):
                            link_parts[index] = self.out_dir_parts[index]
                        for date in padded_dates:
                            if any(pad_dir in str(path) for pad_dir in self.pad_dirs):
                                link_parts[self.data_file_path.year_index] = str(date.year)
                                link_parts[self.data_file_path.month_index] = str(date.month).zfill(2)
                                link_parts[self.data_file_path.day_index] = str(date.day).zfill(2)
                                link_path = Path(*link_parts)
                                log.debug(f'file: {path} link: {link_path}')
                                link_path.parent.mkdir(parents=True, exist_ok=True)
                                if not link_path.exists():
                                    link_path.symlink_to(path)
                            # write manifest and thresholds
                            if date == data_date:
                                # link thresholds
                                file_writer.link_thresholds(config_location_path, link_path)
                                manifest_path = Path(link_path.parent, Config.manifest_filename)
                                padded_dates = self.recheck_padded_dates(padded_dates, active_periods)
                                file_writer.write_manifest(padded_dates, manifest_path)
                    elif data_type in self.copy_dirs:
                        link_path = Path(self.out_path, *parts[self.relative_path_index:])
                        link_path.parent.mkdir(parents=True, exist_ok=True)
                        log.debug(f'file: {path} link: {link_path}')
                        if not link_path.exists():
                            link_path.symlink_to(path)
        except Exception:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            log.error("Exception at line " + str(exc_tb.tb_lineno) + ": " + str(sys.exc_info()))

    def check_active_periods_flag(self) -> Optional[Dict[str, Any]]:
        for root, dirs, files in os.walk(self.data_path):
            if Path(root).parts[-1] != Config.location_dir:
                continue

            for fname in files:
                if not fname.lower().startswith("cfgloc") or not fname.lower().endswith("json"):
                    continue

                loc_json = Path(root) / fname
                try:
                    with loc_json.open("r", encoding="utf-8") as f:
                        doc = json.load(f)
                except Exception as e:
                    log.warning(f"Skipping {loc_json}: {e}")
                    continue

                features = doc.get("features", [])
                if not isinstance(features, list):
                    continue

                for feature in features:
                    props = feature.get("properties", {})
                    periods = props.get("active_periods", [])
                    if not isinstance(periods, list):
                        continue
                    for period in periods:
                        if isinstance(period, dict) and period.get("active_periods_flag"):
                            return period

        return None

    @staticmethod
    def recheck_padded_dates(padded_dates: List[datetime.datetime], active_periods: Optional[Dict[str, Any]]) -> List[datetime.datetime]:
        if not active_periods:
            return padded_dates

        date_str = active_periods.get("start_date")
        pivot = datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").date()

        flag = active_periods.get("active_periods_flag".lower())
        if flag == "both":
            return [dt for dt in padded_dates if dt == pivot]
        elif flag == "start":
            return [dt for dt in padded_dates if dt >= pivot]
        elif flag == "end":
            date_str = active_periods.get("end_date")
            pivot = datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").date()
            return [dt for dt in padded_dates if dt <= pivot]
        else:
            return padded_dates
