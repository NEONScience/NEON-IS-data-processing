#!/usr/bin/env python3
import os
import sys
from pathlib import Path

#from structlog import get_logger
import structlog

from padded_timeseries_analyzer.padded_timeseries_analyzer.analyzer_config import AnalyzerConfig
from common.err_datum import err_datum_path

#log = get_logger()
log = structlog.get_logger()


class PaddedTimeSeriesAnalyzer:

    def __init__(self, data_path: Path, out_path: Path, err_path: Path, relative_path_index: int) -> None:
        """
        Constructor.

        :param data_path: The data directory, i.e., /tmp/in/prt/2018/01/03.
        :param out_path: The output directory, i.e., /tmp/out.
        :param err_path: The error directory, i.e., errored.
        :param relative_path_index: Trim input file paths to this index.
        """
        self.data_path = data_path
        self.out_path = out_path
        self.DirErrBase = Path(self.out_path, err_path)
        # DirErrBase: the user specified error directory, i.e., /tmp/out/errored
        self.relative_path_index = relative_path_index

    def analyze(self) -> None:
        """Verify all necessary data files are present in the input."""
        dataDir_routed = Path("")
        manifest_file = AnalyzerConfig.manifest_filename
        for root,directories,files in os.walk(str(self.data_path)):
            for filename in files:
                if filename != manifest_file:
                    try:
                        if (Path(root,filename).parent) == 'data':
                            dataDir_routed = Path(root,filename).parent
                            log.info(f'Inside inner try, data_path directory {dataDir_routed}')
                            log.info(f'No manifest_file found in data_path directory {dataDir_routed}')
                    except:
                        err_msg = "No manifest_file found in data path directory"
                        err_datum_path(err=err_msg,DirDatm=str(Path(dataDir_routed)),DirErrBase=self.DirErrBase,
                                       RmvDatmOut=True,DirOutBase=self.out_path)
                else:
                    # read manifest
                    dates = [date.rstrip() for date in open(Path(root,filename))]
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
                                file_path = Path(root,data_file)
                                link_path = Path(self.out_path,*file_path.parts[self.relative_path_index:])
                                log.debug(f'linking {file_path} to {link_path}')
                                link_path.parent.mkdir(parents=True,exist_ok=True)
                                if not link_path.exists():
                                    link_path.symlink_to(file_path)
                                self.link_thresholds(file_path,link_path)
                        # go up one directory to find any ancillary files to link
                        self.link_ancillary_files(Path(root))

    @staticmethod
    def link_thresholds(data_file_path: Path, data_file_link_path: Path) -> None:
        """
        Link the threshold file.

        :param data_file_path: The source path for the data file.
        :param data_file_link_path: The link path for the data file.
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

    def link_ancillary_files(self, root: Path) -> None:
        """
        Link any files beyond data and thresholds.

        :param root: The root directory to find files.
        """
        for file_path in root.parent.rglob('*'):
            if file_path.is_file():
                if '/'+AnalyzerConfig.data_dir+'/' not in str(file_path) and AnalyzerConfig.threshold_dir not in str(file_path):
                    link_path = Path(self.out_path, *file_path.parts[self.relative_path_index:])
                    link_path.parent.mkdir(parents=True, exist_ok=True)
                    if not link_path.exists():
                        link_path.symlink_to(file_path)

    @staticmethod
    def get_data_file_date(filename: str) -> str:
        """Parse the date from file names in format source_location_yyyy-mm-dd.xxx"""
        return filename.split('.')[0].split('_')[2]
