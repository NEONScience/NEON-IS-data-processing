#!/usr/bin/env python3
from pathlib import Path
import datetime

from structlog import get_logger

import timeseries_padder.timeseries_padder.pad_calculator as pad_calculator
import timeseries_padder.timeseries_padder.file_writer as file_writer
from timeseries_padder.timeseries_padder.pad_config import PadConfig
from timeseries_padder.timeseries_padder.data_file_path import DataFilePath

log = get_logger()


class ConstantWindowPad(object):
    """Class to pad data with a fixed window size."""

    def __init__(self, *, data_path: Path, out_path: Path, relative_path_index: int, window_size: int,
                 data_file_path: DataFilePath):
        """
        Constructor.

        :param data_path: The data path.
        :param out_path:  The path for writing results.
        :param relative_path_index: Trim file paths to this index.
        :param window_size: The window size.
        """
        self.data_path = data_path
        self.out_path = out_path
        self.relative_path_index = relative_path_index
        self.window_size = window_size
        self.data_file_path = data_file_path
        self.types_to_process = [PadConfig.data_dir]
        self.out_path_parts = list(out_path.parts)

    def pad(self):
        """Pad the data using the given window size."""
        manifests = {}
        manifest_file_names = {}
        for path in self.data_path.rglob('*'):
            if path.is_file():
                self.process_file(path, manifests, manifest_file_names)
        file_writer.write_manifests(manifests, manifest_file_names)

    def process_file(self, path: Path, manifests: dict, manifest_file_names: dict):
        """
        Process each data file by linking into each date in the padded time range,
        linking the threshold file, and the manifest file containing the padded dates.

        :param path: The data file path.
        :param manifests: The manifest dates organized by location.
        :param manifest_file_names:  The manifest file names organized by location.
        """
        parts = path.parts
        year, month, day, location, data_type = self.data_file_path.parse(path)
        if data_type in self.types_to_process:
            location_path = Path(*parts[:self.data_file_path.location_index + 1])
            if location not in manifests:
                manifests[location] = []
            data_date = datetime.date(int(year), int(month), int(day))
            padded_range_dates = pad_calculator.get_dates_in_padded_range(data_date, self.window_size)
            # link file into each date in the padded range
            link_parts = list(parts)
            for index in range(1, len(self.out_path_parts)):
                link_parts[index] = self.out_path_parts[index]
            for padded_range_date in padded_range_dates:
                link_parts[self.data_file_path.year_index] = str(padded_range_date.year)
                link_parts[self.data_file_path.month_index] = str(padded_range_date.month).zfill(2)
                link_parts[self.data_file_path.day_index] = str(padded_range_date.day).zfill(2)
                link_path = Path(*link_parts)
                log.debug(f'file: {path} link: {link_path}')
                link_path.parent.mkdir(parents=True, exist_ok=True)
                if not link_path.exists():
                    link_path.symlink_to(path)
                manifests[location].append(padded_range_date)
                if padded_range_date == data_date:
                    manifest_file_names[location] = Path(link_path.parent, PadConfig.manifest_filename)
                file_writer.link_thresholds(location_path, link_path)
        else:
            link_path = Path(self.out_path, *parts[self.relative_path_index:])
            link_path.parent.mkdir(parents=True, exist_ok=True)
            if not link_path.exists():
                link_path.symlink_to(path)
