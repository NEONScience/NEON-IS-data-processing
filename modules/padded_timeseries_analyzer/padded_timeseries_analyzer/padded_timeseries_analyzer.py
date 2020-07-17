#!/usr/bin/env python3
import os
import sys
from pathlib import Path

from structlog import get_logger

from padded_timeseries_analyzer.padded_timeseries_analyzer.analyzer_config import AnalyzerConfig

log = get_logger()


class PaddedTimeSeriesAnalyzer:

    def __init__(self, data_path: Path, out_path: Path, relative_path_index: int):
        """
        Constructor.

        :param data_path: The data directory.
        :param out_path: The output directory.
        :param relative_path_index: Trim input file paths to this index.
        """
        self.data_path = data_path
        self.out_path = out_path
        self.relative_path_index = relative_path_index

    def analyze(self):
        """Analyze time series data to calculate additional time padding
        required for processing with thresholds."""
        out_path_parts = list(self.out_path.parts)
        manifest_file = AnalyzerConfig.manifest_filename
        try:
            for root, directories, files in os.walk(str(self.data_path)):
                for filename in files:
                    if filename == manifest_file:
                        # read manifest
                        dates = [date.rstrip() for date in open(Path(root, filename))]
                        # check for existence of complete manifest
                        dates_not_found = []
                        for date in dates:
                            dates_not_found.append(date)
                        for date in dates:
                            for data_file in os.listdir(root):
                                log.debug(f'data_file: {data_file}')
                                if data_file != manifest_file:
                                    data_file_date = self.get_data_file_date(data_file)
                                    log.debug(f'checking data file date: {data_file_date} and '
                                              f'manifest date {date} in {dates_not_found}')
                                    if date in data_file_date and date in dates_not_found:
                                        log.debug(f'found data for: {date}')
                                        dates_not_found.remove(date)
                        # if complete link to output
                        if not dates_not_found:
                            for data_file in os.listdir(root):
                                if data_file != manifest_file:
                                    file_path = Path(root, data_file)
                                    link_parts = Path(file_path).parts
                                    link_parts = list(link_parts)
                                    for index in range(1, len(out_path_parts)):
                                        link_parts[index] = out_path_parts[index]
                                    link_path = Path(*link_parts)
                                    log.debug(f'linking {file_path} to {link_path}')
                                    link_path.parent.mkdir(parents=True, exist_ok=True)
                                    if not link_path.exists():
                                        link_path.symlink_to(file_path)
                                    self.link_thresholds(file_path, link_path)
                            # go up one directory and get any ancillary files to link
                            self.link_ancillary_files(Path(root))
        except Exception:
            exception_type, exception_obj, exception_tb = sys.exc_info()
            log.error("Exception at line " + str(exception_tb.tb_lineno) + ": " + str(sys.exc_info()))

    @staticmethod
    def link_thresholds(data_file_path: Path, data_file_link_path: Path):
        """
        Write thresholds if they exist in the source repository.

        :param data_file_path: The source path for the threshold file.
        :param data_file_link_path: The destination path to write results.
        """
        file_root = data_file_path.parent.parent
        link_root = data_file_link_path.parent.parent
        file_path = Path(file_root, AnalyzerConfig.threshold_dir, AnalyzerConfig.threshold_filename)
        if file_path.exists():
            link_path = Path(link_root, AnalyzerConfig.threshold_dir, AnalyzerConfig.threshold_filename)
            log.debug(f'linking {file_path} to {link_path}')
            link_path.parent.mkdir(parents=True, exist_ok=True)
            if not link_path.exists():
                link_path.symlink_to(file_path)

    def link_ancillary_files(self, root: Path):
        """
        Write any files outside of data and thresholds in the input path
        into the output path.

        :param root: The threshold root directory.
        """
        for file_path in root.parent.rglob('*'):
            if file_path.is_file():
                file_path = str(file_path)
                if AnalyzerConfig.data_dir not in file_path and AnalyzerConfig.threshold_dir not in file_path:
                    file_path = Path(file_path)
                    link_path = Path(self.out_path, *file_path.parts[self.relative_path_index:])
                    link_path.parent.mkdir(parents=True, exist_ok=True)
                    if not link_path.exists():
                        link_path.symlink_to(file_path)

    @staticmethod
    def get_data_file_date(filename: str) -> str:
        """Parse the date from data file names in format source_location_yyyy-mm-dd.xxx"""
        return filename.split('.')[0].split('_')[2]
