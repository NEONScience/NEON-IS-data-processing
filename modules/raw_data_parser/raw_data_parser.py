#!usr/bin/env python3

from pathlib import Path
from structlog import get_logger
from typing import Dict, Hashable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re

from raw_data_parser.parse_code import parse_code_dict

log = get_logger()
keep_columns = ['source_id', 'site_id', 'readout_time']
drop_columns = ['kafka_key', 'kakfa_ts_type', 'kafka_ts', 'kafka_partition', 'kafka_offset', 'kafka_topic', 'ds']


def parse_raw(source_type: str, parse_field: str, data_path: Path, out_path: Path,
              relative_path_index: int) -> None:
    out_df = pd.DataFrame()
    out_file = ''

    for path in data_path.rglob('*'):
        if path.is_file():
            log.debug(f'reading file {path}.')
            df = pd.read_parquet(path)
            log.debug(f'{df.columns}')
            df.drop(columns=drop_columns, inplace=True)
            log.debug(f'after drop kafka columns {df.columns}')

            if out_df.empty:
                out_df = sensor_parse(df, source_type, parse_field)
            else:
                out_df = pd.concat([sensor_parse(df, source_type, parse_field), out_df], ignore_index=True)
            if not out_file:
                out_source_type: str = source_type.split('_')[0]
                out_file = create_output_path(source_type,
                                              Path(out_path, out_source_type, *Path(path).parts[relative_path_index:]))
    log.debug("end of loop.")

    if not out_df.empty and out_file:
        write_to_parquet(out_file, out_df)


def sensor_parse(df: pd.DataFrame, source_type: str, parse_field: str) -> pd.DataFrame:
    parser = parse_code_dict.get(source_type)

    if source_type.lower() == 'li7200_raw':
        # li7200_raw parse_field data example: (Data (Ndx 116847692)(DiagVal 8191)... ...(CO2AWO 34252))(CH4Data )
        extracted_df = df[parse_field].apply(lambda x: extract_and_rename(x, parser))
        # Convert the series of dictionaries into a DataFrame
        extracted_df = pd.json_normalize(extracted_df)
        return pd.concat([df[keep_columns], extracted_df], axis=1)

    elif source_type.lower() == 'g2131i_raw' or source_type.lower() == 'l2130i_raw':
        # g2131i_raw parse_field data example: 2024-03-13 17:24:47.224;140.001;45.000;40.715;... ...;
        # Split the string column into lists of floats
        df[parse_field] = df[parse_field].apply(lambda x: x.split(';'))
        # Extract and assign columns based on dictionary
        for index, term in parser.items():
            df[term] = df[parse_field].apply(lambda x: float(x[index]) if index < len(x) else None)
        df.drop(columns=parse_field, inplace=True)
        return df
    else:
        pass


def extract_and_rename(data_string: str, name_mapping: dict) -> Dict:
    # Regular expression to find all (name number) pairs
    pattern = r'\((\w+)\s([\w\.\-:]+)\)'
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


def write_to_parquet(out_file: Path, out_df: pd.DataFrame) -> None:
    log.debug(f'output path: {out_file}.')
    hashable_cols = [x for x in out_df.columns if isinstance(out_df[x].iloc[0], Hashable)]
    dupcols = [x.encode('UTF-8') for x in hashable_cols
               if (out_df[x].duplicated().sum() / (int(out_df[x].size) - 1)) > 0.3]
    table = pa.Table.from_pandas(df=out_df)
    pq.write_table(table, out_file, use_dictionary=dupcols, version="2.4", compression='zstd', compression_level=8,
                   coerce_timestamps='ms', allow_truncated_timestamps=False)


def create_output_path(source_type: str, path: Path) -> Path:
    out_source_type: str = source_type.split('_')[0]
    mac = path.parent.parent.name

    path = Path(str(path).replace(mac, mac.upper()))

    new_stem = path.stem.replace(source_type, out_source_type)
    pattern = r"(_\d+_\d+)$"
    new_stem = re.sub(pattern, '', new_stem)

    # Construct the new filename and full path
    new_filename = new_stem + path.suffix
    new_path = path.with_name(new_filename)

    new_path.parent.mkdir(parents=True, exist_ok=True)

    return new_path
