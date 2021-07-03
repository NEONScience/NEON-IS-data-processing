#!/usr/bin/env python3
import os
import json
import pandas as pd
from pandas import DataFrame
import datetime
import fnmatch
import math

from structlog import get_logger

log = get_logger()


def transform(*, year_index: int) -> None:
    """
    :param data_path: input data path
    :param out_path: output path
    :param year_index: index of year in data path
    """

    workbook_file = os.environ['WORKBOOK_PATH']
    locations_path = os.environ['DATA_PATH'] + os.path.sep + 'locations'
    data_path = os.environ['DATA_PATH'] + os.path.sep + 'data'
    out_path = os.environ['OUT_PATH']

    # import workbook
    workbook = pd.read_csv(workbook_file, delimiter='\t', keep_default_na=False)

    # get table names by TMI
    tmi_table = set([e.split('.')[-1]+"tmitok" for e in workbook['DPNumber']] + workbook['table'])
    table_by_tmi = dict(zip([e.split('tmitok')[0] for e in tmi_table], [e.split('tmitok')[1] for e in tmi_table]))

    # get datum date
    data_path_parts = os.environ['DATA_PATH'].split(os.path.sep)
    formatted_date = '-'.join(data_path_parts[year_index:year_index+3])

    # processing timestamp
    timestamp = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')

    locations = os.listdir(locations_path)
    for location in locations:
        # import location json
        location_path = locations_path + os.path.sep + location
        location_file = os.listdir(location_path)[0]
        with open(location_path + os.path.sep + location_file) as f:
            loc_data = json.load(f)

        # construct output path
        site = loc_data["features"][0]["properties"]["site"]
        day_path = os.path.sep.join(data_path_parts[year_index:year_index+3])
        output_path = out_path + os.path.sep + site + os.path.sep + day_path
          
        # determine time indexes from data filenames
        location_data_path = data_path + os.path.sep + location
        data_files = os.listdir(location_data_path)
        # data file must be of the form *_TMI.ext
        tmi_by_file = dict(zip(data_files, [os.path.splitext(data_file)[0].split('_')[-1] for data_file in data_files]))

        for tmi in list(set(tmi_by_file.values())):

            # construct filenames
            dp_parts = workbook['dpID'][0].split('.')
            dp_parts[dp_parts.index('DOM')] = loc_data["features"][0]["properties"]["domain"]
            dp_parts[dp_parts.index('SITE')] = site
            dp_parts.extend([loc_data["features"][0]["HOR"], loc_data["features"][0]["VER"], tmi, table_by_tmi[tmi]])
            basic_filename_parts = dp_parts
            expanded_filename_parts = dp_parts.copy()
            basic_filename_parts.extend([formatted_date, 'basic', timestamp, 'csv'])
            expanded_filename_parts.extend([formatted_date, 'expanded', timestamp, 'csv'])
            basic_filename = '.'.join(basic_filename_parts)
            expanded_filename = '.'.join(expanded_filename_parts)
            basic_filepath = output_path + os.path.sep + basic_filename
            expanded_filepath = output_path + os.path.sep + expanded_filename

            tmi_files = [location_data_path + os.path.sep + k for k,v in tmi_by_file.items() if v == tmi]

            # import and concatenate data files
            data = pd.read_parquet(tmi_files[0])
            for tmi_file in tmi_files[1:]:
                data = pd.concat([data, pd.read_parquet(tmi_file).iloc[:,2:]], 1)

            # format columns
            for column in data.columns:
                if not workbook['fieldName'].str.contains(column).any():
                    format = 'asIs'
                else:
                    format = workbook['pubFormat'][workbook[workbook['fieldName'] == column].index.values[0]]
                format_column(data, column, format)
            
            # add empty columns for any fields specified in workbook that are not present in the data
            data[workbook['fieldName'][workbook['fieldName'].isin(data.columns) == False]] = ""

            # extract and write datasets
            os.makedirs(output_path, exist_ok=True)
            # basic
            basic_columns = workbook.loc[(workbook['downloadPkg'] == 'basic') & (workbook['table'] == table_by_tmi[tmi]), ['rank','fieldName']]
            data[basic_columns.sort_values('rank')['fieldName']].to_csv(basic_filepath, index=False)
            # expanded
            if workbook['downloadPkg'].str.contains('expanded').any():
                expanded_columns = workbook.loc[((workbook['downloadPkg'] == 'basic') | (workbook['downloadPkg'] == 'expanded')) & (workbook['table'] == table_by_tmi[tmi]), ['rank','fieldName']]
                data[expanded_columns.sort_values('rank')['fieldName']].to_csv(expanded_filepath, index=False)


# Apply pub format to a column of a dataframe
def format_column(dataframe: DataFrame, column: str, format: str):

    if format == "yyyy-MM-dd'T'HH:mm:ss'Z'(floor)":
        dataframe[column] = dataframe[column].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    if fnmatch.fnmatch(format, "*.*(round)"):
        pyformat = '{:.' + str(format.count('#')) + '}'
        dataframe[column] = dataframe[column].map(pyformat.format)

    if fnmatch.fnmatch(format, "signif_*(round)"):
        n_digits = format.count('#')
        dataframe[column] = [format_sig(element, n_digits) for element in dataframe[column]]

    if format == 'integer':
        dataframe[column] = dataframe[column].round().map('{:.0f}'.format)


# Format a number to n_digits significant figures and at most 7 decimal places.
def format_sig(element, n_digits):
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