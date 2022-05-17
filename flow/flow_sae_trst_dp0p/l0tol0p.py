#!/usr/bin/env python3
from pathlib import Path
from structlog import get_logger

import environs
import os
import pandas as pd

from flow_sae_trst_dp0p.cal_flags import get_cal_val_flags
from flow_sae_trst_dp0p import log_config
from typing import List, Dict, Optional

log = get_logger()


class L0toL0p:

    def __init__(self, cal_term_map: Optional[Dict] = None, calibrated_qf_list: Optional[List] = None,
                 target_qf_cal_list: Optional[List] = None):
        """
        :param cal_term_map: map for calibrated variables between field names from avro schema and term names from ATBD
        :param calibrated_qf_list: list of quality flag names that were calibrated on site
        :param target_qf_cal_list: list of quality flag names that will take flags from calibrated_qf_list
       """
        self.cal_term_map = cal_term_map or {}
        self.calibrated_qf_list = calibrated_qf_list or []
        self.target_qf_cal_list = target_qf_cal_list or []
        env = environs.Env()
        self.in_path: Path = env.str('IN_PATH')
        self.out_path: Path = env.path('OUT_PATH')
        self.file_dirs: list = env.list('FILE_DIR')
        self.relative_path_index: int = env.int('RELATIVE_PATH_INDEX')
        log_level: str = env.log_level('LOG_LEVEL', 'INFO')
        log_config.configure(log_level)

    def data_conversion(self, filename) -> pd.DataFrame:
        pass

    def get_combined_qfcal(self, outputdf: pd) -> None:
        if len(self.calibrated_qf_list) == 0:
            return
        elif len(self.calibrated_qf_list) == 1:
            self.assign_qf_cal(outputdf[self.calibrated_qf_list[0]], outputdf)
        else:
            qf = 0
            for qfcal in self.calibrated_qf_list:
                if qfcal == 1:
                    qf = 1
                    break
                elif qfcal == -1:
                    qf = -1
            self.assign_qf_cal(qf, outputdf)

    def assign_qf_cal(self, qf, outputdf):
        for qfname in self.target_qf_cal_list:
            outputdf[qfname] = qf

    def l0tol0p(self) -> None:
        """
        L0 to l0p transformation.
        :param in_path: The input path for files.
        :param out_path: The output path for linking.
        :param file_dirs: The directories to files need l0 to l0p transformation.
        :param relative_path_index: Starting index of the input path to include in the output path.
        """

        outputdf = pd.DataFrame()
        outfile = ''
        for root, directories, files in os.walk(str(self.in_path)):
            if not outputdf.empty and len(directories) > 0:
                if any(dir in directories for dir in self.file_dirs):
                    outputdf.to_parquet(outfile)
                    outputdf = pd.DataFrame()
                    outfile = ''
            if len(files) > 0:
                if len(files) > 1:
                    log.warn("There are more than 1 files under " + root)
                    log.warn(files)
                for file in files:
                    path = Path(root, file)
                    if "flag" in str(path):
                        if outputdf.empty:
                            outputdf = get_cal_val_flags(path, self.cal_term_map)
                        else:
                            outputdf = pd.merge(outputdf, get_cal_val_flags(path, self.cal_term_map), how='inner', left_on=['readout_time'], right_on=['readout_time'])
                        self.get_combined_qfcal(outputdf)
                    else:
                        outfile = Path(self.out_path, *Path(path).parts[self.relative_path_index:])
                        outfile.parent.mkdir(parents=True, exist_ok=True)
                        if outputdf.empty:
                            outputdf = self.data_conversion(path)
                        else:
                            outputdf = pd.merge(self.data_conversion(path), outputdf, how='inner', left_on=['readout_time'], right_on=['readout_time'])
        if not outputdf.empty and outfile != '':
            outputdf.to_parquet(outfile)
