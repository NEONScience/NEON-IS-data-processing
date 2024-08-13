#!usr/bin/env python3

from pathlib import Path
from structlog import get_logger
from typing import Dict, Hashable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re

from raw_data_parser.pass_code import pass_code_dict

log = get_logger()
keep_columns = ['source_id', 'site_id', 'readout_time']


def pass_raw(source_type: str, pass_field: str, data_path: Path, out_path: Path,
             relative_path_index: int) -> None:
    out_df = pd.DataFrame()
    out_file = None

    for path in data_path.rglob('*'):
        if path.is_file():
            log.debug(f'reading file {path}.')
            df = pd.read_parquet(path)
            log.debug(f'{df.columns}')

            if out_df.empty:
                out_df = sensor_pass(df, source_type, pass_field)
            else:
                out_df = pd.concat([sensor_pass(df, source_type, pass_field), out_df], ignore_index=True)
            if not out_file:
                out_source_type: str = source_type.split('_')[0]
                out_file = create_output_path(source_type,
                                              Path(out_path, out_source_type, *Path(path).parts[relative_path_index:]))
    log.debug("end of loop.")

    if not out_df.empty and out_file:
        write_to_parquet(out_file, out_df)


def sensor_pass(df: pd.DataFrame, source_type: str, pass_field: str) -> pd.DataFrame:
    parser = pass_code_dict.get(source_type)

    if source_type.lower() == 'li7200_raw':
        # extract_and_rename(data_string: str, name_mapping: dict):
        extracted_df = df[pass_field].apply(lambda x: extract_and_rename(x, parser))
        # Convert the series of dictionaries into a DataFrame
        extracted_df = pd.json_normalize(extracted_df)

        out_df = pd.concat([df[keep_columns], extracted_df], axis=1)
        return out_df


def extract_and_rename(data_string: str, name_mapping: dict) -> Dict:
    # Regular expression to find all (name number) pairs
    pattern = r'\((\w+)\s+([\d.]+)\)'

    # Find all matches
    matches = re.findall(pattern, data_string)
    # Filter and rename based on the name_mapping dictionary
    extracted_data = {name_mapping[name]: float(number) for name, number in matches if name in name_mapping}

    # if want to keep integer as is
    # extracted_data = {}
    # for name, number in matches:
    #     if name in name_mapping:
    #         # Check if the number is an integer or a float
    #         if '.' in number:
    #             extracted_data[name_mapping[name]] = float(number)
    #         else:
    #             extracted_data[name_mapping[name]] = int(number)

    return extracted_data


def write_to_parquet(out_file: str, out_df: pd.DataFrame) -> None:
    hashable_cols = [x for x in out_df.columns if isinstance(out_df[x].iloc[0], Hashable)]
    dupcols = [x.encode('UTF-8') for x in hashable_cols
               if (out_df[x].duplicated().sum() / (int(out_df[x].size) - 1)) > 0.3]
    table = pa.Table.from_pandas(df=out_df)
    pq.write_table(table, out_file, use_dictionary=dupcols, version="2.4", compression='zstd', compression_level=8,
                   coerce_timestamps='ms', allow_truncated_timestamps=False)


def create_output_path(source_type: str, path: Path) -> Path:
    out_source_type: str = source_type.split('_')[0]
    new_type = path.stem.replace(source_type, out_source_type)

    pattern = r"(_\d+_\d+)$"
    new_type = re.sub(pattern, '', new_type)

    # Combine the new stem with the original suffix (extension)
    new_filename = new_type + path.suffix

    # Reconstruct the full path with the new filename
    new_path = path.with_name(new_filename)
    new_path.parent.mkdir(parents=True, exist_ok=True)

    return new_path
