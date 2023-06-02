#!/usr/bin/env python3
from pathlib import Path
from structlog import get_logger

import environs
import json
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shutil

from flow_sae_trst_dp0p.cal_flags import get_cal_val_flags
from flow_sae_trst_dp0p import log_config
from typing import List, Dict, Optional, Hashable

log = get_logger()


class L0toL0p:
    """
    base class for SAE L0 to L0p data transformation
    """

    def __init__(self, context_dp_map: Optional[Dict] = None, cal_term_map: Optional[Dict] = None,
                 calibrated_qf_list: Optional[List] = None, target_qf_cal_list: Optional[List] = None):

        """
        :param cal_term_map: map for calibrated variables between field names from avro schema and term names from ATBD
        :param calibrated_qf_list: list of quality flag names that were calibrated on site
        :param target_qf_cal_list: list of quality flag names that will take flags from calibrated_qf_list
        environment variables:
        in_path: The input path for files.
        out_path: The output path for linking.
        file_dirs: The directories to files need l0 to l0p transformation.
        relative_path_index: Starting index of the input path to include in the output path.
        new_source_type_name(optional): Replace source_type with the new name in the output path,
                may happen when multiple data products derived from same sensor type,
                e.g. mcseries -> mfcSampTurb
        When new_source_type_name is defined, relative_path_index is the index after that of replaced source type
        location_link_type(optional): Link or copy location directory to output,
                when defined, must be either "SYMLINK" or "COPY"
                if not defined, the location directory will not be shown in output repo
       """
        self.cal_term_map = cal_term_map or {}
        self.calibrated_qf_list = calibrated_qf_list or []
        self.target_qf_cal_list = target_qf_cal_list or []
        self.context_dp_map = context_dp_map or {}
        env = environs.Env()
        self.in_path: Path = env.path('IN_PATH')
        self.out_path: Path = env.path('OUT_PATH')
        self.file_dirs: list = env.list('FILE_DIR', [])
        self.relative_path_index: int = env.int('RELATIVE_PATH_INDEX')
        self.new_source_type_name: str = env.str('NEW_SOURCE_TYPE_NAME', None)
        self.location_link_type: str = env.str('LOCATION_LINK_TYPE', None)
        if self.location_link_type and self.location_link_type != 'SYMLINK' and self.location_link_type != 'COPY':
            raise ValueError('defined LOCATION_LINK_TYPE must be either "SYMLINK" or "COPY". '
                             'If not defined, location directory will not be linked/copied to output.')
        self.out_file = ''
        log_level: str = env.log_level('LOG_LEVEL', 'DEBUG')
        log_config.configure(log_level)

    def data_conversion(self, filename) -> pd.DataFrame:
        out_df = pd.read_parquet(filename)
        log.debug(f'{out_df.columns}')
        # drop columns not used in l0 to l0p data conversion
        self.drop_kafka_columns(out_df)
        return out_df

    @staticmethod
    def drop_kafka_columns(in_df: pd.DataFrame) -> None:
        kafka_columns = ['kafka_key', 'kafka_topic', 'kafka_partition', 'kafka_offset', 'kafka_ts', 'kakfa_ts_type', 'ds']
        if kafka_columns[0] in in_df.columns:
            in_df.drop(columns=kafka_columns, inplace=True)

    def get_combined_qfcal(self, out_df: pd.DataFrame) -> None:
        if len(self.calibrated_qf_list) == 0:
            return
        elif len(self.calibrated_qf_list) == 1:
            self.assign_qf_cal(out_df[self.calibrated_qf_list[0]], out_df)
        else:
            qf = 0
            for qfcal in self.calibrated_qf_list:
                if qfcal == 1:
                    qf = 1
                    break
                elif qfcal == -1:
                    qf = -1
            self.assign_qf_cal(qf, out_df)

    def assign_qf_cal(self, qf, out_df):
        for qfname in self.target_qf_cal_list:
            out_df[qfname] = qf

    def l0tol0p(self) -> None:
        """
        L0 to l0p transformation.
        """
        out_df = pd.DataFrame()
        hold_files = {}
        for root, directories, files in os.walk(str(self.in_path)):
            if root.endswith('location'):
                if self.context_dp_map:
                    # self.get_location_context(root, files)
                    self.get_dp_name(root, files)
                    # TODO
                    for filepath in hold_files.keys():
                        out_df = self.read_files(filepath, hold_files[filepath], out_df)
                if self.location_link_type:
                    self.link_location(root, files)
                continue
            if not out_df.empty and directories:
                if any(tmp_dir in directories for tmp_dir in self.file_dirs):
                    self.write_to_parquet(self.out_file, out_df)
                    out_df = pd.DataFrame()
                    self.out_file = ''
            if files:
                if self.context_dp_map and not self.new_source_type_name:
                    hold_files[root] = files
                    continue
                out_df = self.read_files(root, files, out_df)
        if not out_df.empty and self.out_file:
            self.write_to_parquet(self.out_file, out_df)

    def read_files(self, filepath: str, files: List, out_df: pd.DataFrame) -> pd.DataFrame:
        for file in files:
            path = Path(filepath, file)
            if "flag" in str(path):
                if out_df.empty:
                    out_df = get_cal_val_flags(path, self.cal_term_map)
                else:
                    out_df = pd.merge(out_df, get_cal_val_flags(path, self.cal_term_map), how='inner',
                                      left_on=['readout_time'], right_on=['readout_time'])
                self.get_combined_qfcal(out_df)
            else:
                self.out_file = self.create_output_path(path)
                if out_df.empty:
                    out_df = self.data_conversion(path)
                else:
                    out_df = pd.merge(self.data_conversion(path), out_df, how='inner', left_on=['readout_time'],
                                      right_on=['readout_time'])
            return out_df

    @staticmethod
    def write_to_parquet(out_file: str, out_df: pd.DataFrame) -> None:
        hashable_cols = [x for x in out_df.columns if isinstance(out_df[x].iloc[0], Hashable)]
        dupcols = [x.encode('UTF-8') for x in hashable_cols
                   if (out_df[x].duplicated().sum() / (int(out_df[x].size) - 1)) > 0.3]
        table = pa.Table.from_pandas(df=out_df)
        pq.write_table(table, out_file, use_dictionary=dupcols, version="2.4", compression='zstd', compression_level=8,
                       coerce_timestamps='ms', allow_truncated_timestamps=False)
        # out_df.to_parquet(out_file, use_dictionary=dupcols, version="2.4", compression='zstd', compression_level=8,
        #                   coerce_timestamps='ms', allow_truncated_timestamps=False)

    def create_output_path(self, path: str) -> Path:
        if self.new_source_type_name:
            new_path = Path(self.out_path, Path(self.new_source_type_name),
                            *Path(path).parts[self.relative_path_index:])
        else:
            new_path = Path(self.out_path, *Path(path).parts[self.relative_path_index:])
        new_path.parent.mkdir(parents=True, exist_ok=True)
        log.debug(f'new path is {new_path}.')
        return new_path

    def link_location(self, path: str, files: List[str]) -> None:
        new_path = self.create_output_path(path)
        if self.location_link_type == 'SYMLINK':
            log.debug(f'Linking path {new_path} to {path}')
            if not new_path.exists():
                new_path.symlink_to(path)
        else:
            log.debug(f'Copying path {path} to {new_path}.')
            new_path.mkdir(parents=True, exist_ok=True)
            for file in files:
                new_file_path = Path(new_path, file)
                if not new_file_path.exists():
                    shutil.copy2(Path(path, file), new_file_path)

    @staticmethod
    def get_location_context(path: str, files: List[str]) -> List:
        for file in files:
            f = open(Path(path, file))
            data = json.load(f)
            for feature in data['features']:
                context = feature['properties']['context']
                return context  # ','.join(context)

    def get_dp_name(self, path: str, files: List[str]) -> None:
        loc_ctxs = self.get_location_context(path, files)
        # TODO: all element of keys in list of loc_ctxs
        for key in self.context_dp_map.keys():
            tmp_keys = key.replace(' ','').split(sep=',')
            if all(ctx in loc_ctxs for ctx in tmp_keys):
                self.new_source_type_name = self.context_dp_map[key]
                return
