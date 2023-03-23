#!/usr/bin/env python3
import os
import json
import pandas as pd
from pandas import DataFrame
import fnmatch
import math
import csv
from pathlib import Path

from structlog import get_logger

log = get_logger()


def pub_transform(*, data_path: Path, out_path: Path, workbook_file: Path, year_index: int, data_type_index: int, group_metadata_dir: str) -> None:
    """
    :param year_index: index of year in data path
    :param data_type_index: index of data & metadata directories in data path. Group metadata directory must be at this index.
    :param group_metadata_dir: directory name with group metadata (e.g. "group")
    """
    #workbook_file = os.environ['WORKBOOK_PATH']
    #out_path = os.environ['OUT_PATH']
    #data_path = os.path.join(os.environ['DATA_PATH'])

    # import workbook
    log.debug(f'Workbook file {workbook_file}')
    workbook = pd.read_csv(workbook_file, delimiter='\t', keep_default_na=False)

    # get table names by TMI
    tmi_table = set([e.split('.')[-1]+"tmitok" for e in workbook['DPNumber']] + workbook['table'])
    table_by_tmi = dict(zip([e.split('tmitok')[0] for e in tmi_table], [e.split('tmitok')[1] for e in tmi_table]))

    # Each PUBLOC is a datum (/year/month/day/PUBLOC). Run through each PUBLOC
    for path in data_path.rglob(group_metadata_dir+'/'):
        parts = path.parts
        group_metadata_name = parts[data_type_index]
        publoc_path = path.parent
        
        # Double check that the directory at the group metadata index matches the group directory name
        if group_metadata_name != group_metadata_dir:
            log.warn(f'Path {publoc_path} looks to be a datum, but the directory at the group metadata index ({group_metadata_name}) does not match the expected name ({group_metadata_dir}). Skipping...')
            continue
        else:
            log.debug(f'Processing datum path {publoc_path}')

        group_metadata_path = Path(publoc_path, group_metadata_dir)
        data_path = Path(publoc_path, 'data')
    
        # get datum date
        data_path_parts = data_path.parts
        formatted_date = '-'.join(data_path_parts[year_index:year_index+3])
    
        # has data by output file
        has_data_by_file = {}
    
        groups = os.listdir(group_metadata_path)
        for group in groups:
            # import group json
            group_path = os.path.join(group_metadata_path, group)
            group_file = os.listdir(group_path)[0]
            with open(os.path.join(group_path, group_file)) as f:
                group_data = json.load(f)
    
            # construct output path
            site = group_data["features"][0]["site"]
            day_path = os.path.sep.join(data_path_parts[year_index:year_index+3])
            output_path = os.path.join(out_path, day_path, site)
              
            # determine time indexes from data filenames
            group_data_path = os.path.join(data_path, group)
            data_files = os.listdir(group_data_path)
            # data file must be of the form *_TMI.ext
            tmi_by_file = dict(zip(data_files, [os.path.splitext(data_file)[0].split('_')[-1] for data_file in data_files]))
    
            for tmi in list(set(tmi_by_file.values())):
                # construct filenames
                dp_parts = workbook['dpID'][0].split('.')
                dp_parts[dp_parts.index('DOM')] = group_data["features"][0]["domain"]
                dp_parts[dp_parts.index('SITE')] = site
                dp_parts.extend([group_data["features"][0]["HOR"], group_data["features"][0]["VER"], tmi, table_by_tmi[tmi]])
                basic_filename_parts = dp_parts
                expanded_filename_parts = dp_parts.copy()
                basic_filename_parts.extend([formatted_date, 'basic', 'csv'])
                expanded_filename_parts.extend([formatted_date, 'expanded', 'csv'])
                basic_filename = '.'.join(basic_filename_parts)
                expanded_filename = '.'.join(expanded_filename_parts)
                basic_filepath = os.path.join(output_path, basic_filename)
                expanded_filepath = os.path.join(output_path, expanded_filename)
    
                tmi_files = [os.path.join(group_data_path, k) for k, v in tmi_by_file.items() if v == tmi]
    
                # import and concatenate data files
                data = pd.read_parquet(tmi_files[0])
                for tmi_file in tmi_files[1:]:
                    data = pd.concat([data, pd.read_parquet(tmi_file).iloc[:, 2:]], 1)
    
                # format columns
                for column in data.columns:
                    if not workbook['fieldName'].str.contains(column).any():
                        column_format = 'asIs'
                    else:
                        column_format = workbook['pubFormat'][workbook[workbook['fieldName'] == column].index.values[0]]
                    format_column(data, column, column_format)
                
                # add empty columns for any fields specified in workbook that are not present in the data
                data[workbook['fieldName'][workbook['fieldName'].isin(data.columns) == False]] = ""
    
                # determine data availability
                has_data_by_file[basic_filename] = not data[workbook.loc[
                    (workbook['dataCategory'] == 'Y') &
                    (workbook['downloadPkg'] == 'basic')]['fieldName'].values].isnull().values.all()
                has_data_by_file[expanded_filename] = \
                    not data[workbook.loc[(workbook['dataCategory'] == 'Y') & ((workbook['downloadPkg'] == 'basic') |
                                        (workbook['downloadPkg'] == 'expanded'))]['fieldName'].values].isnull().values.all()
    
                # extract and write datasets
                os.makedirs(output_path, exist_ok=True)
                # basic
                basic_columns = workbook.loc[(workbook['downloadPkg'] == 'basic') &
                                             (workbook['table'] == table_by_tmi[tmi]), ['rank','fieldName']]
                data[basic_columns.sort_values('rank')['fieldName']].to_csv(basic_filepath, index=False)
                # expanded
                if workbook['downloadPkg'].str.contains('expanded').any():
                    expanded_columns = workbook.loc[((workbook['downloadPkg'] == 'basic') |
                                                     (workbook['downloadPkg'] == 'expanded'))
                                                    & (workbook['table'] == table_by_tmi[tmi]), ['rank', 'fieldName']]
                    data[expanded_columns.sort_values('rank')['fieldName']].to_csv(expanded_filepath, index=False)
        write_manifest(output_path, has_data_by_file)


def write_manifest(output_path: str, has_data_by_file: dict):
    """Write metadata manifest."""
    manifest_filepath = os.path.join(output_path, 'manifest.csv')
    with open(manifest_filepath, 'w') as manifest_csv:
        writer = csv.writer(manifest_csv)
        writer.writerow(['file', 'hasData'])
        for key in has_data_by_file.keys():
            writer.writerow([key, has_data_by_file[key]])
    

# Apply pub format to a column of a dataframe
def format_column(dataframe: DataFrame, column: str, column_format: str):
    if column_format == "yyyy-MM-dd'T'HH:mm:ss'Z'(floor)":
        dataframe[column] = dataframe[column].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    if fnmatch.fnmatch(column_format, "*.*(round)"):
        py_format = '{:.' + str(column_format.count('#')) + '}'
        dataframe[column] = dataframe[column].map(py_format.format)

    if fnmatch.fnmatch(column_format, "signif_*(round)"):
        n_digits = column_format.count('#')
        dataframe[column] = [format_sig(element, n_digits) for element in dataframe[column]]

    if column_format == 'integer':
        dataframe[column] = dataframe[column].round().map('{:.0f}'.format)


def format_sig(element, n_digits):
    """Format a number to n_digits significant figures and at most 7 decimal places."""
    rounded = round(element, -int(math.floor(math.log10(abs(element))-(n_digits-1))))
    # Format as decimal
    formatted = ("%.16f" % rounded).rstrip('0').rstrip('.')
    # Retain at most 7 decimal places
    if '.' in formatted:
        n_decimal = len(formatted.split('.')[1])
        if n_decimal > 7:
            formatted = "{:.7f}".format(rounded)
    else:
        formatted = str(rounded)
    return formatted
