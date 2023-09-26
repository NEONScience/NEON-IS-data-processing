#!/usr/bin/env python3
import os
import json
import pandas as pd
from pandas import DataFrame
import fnmatch
import math
import csv
import sys
from pathlib import Path

from structlog import get_logger

log = get_logger()


def pub_transform(*, data_path: Path, out_path: Path, workbook_path: Path, product_index: int, year_index: int, data_type_index: int, group_metadata_dir: str, data_path_parse_index: int) -> None:
    """
    :param product_index: index of product in data path
    :param year_index: index of year in data path
    :param data_type_index: index of data & metadata directories in data path. Group metadata directory must be at this index.
    :param group_metadata_dir: directory name with group metadata (e.g. "group")
    :param data_path_parse_index: for the path given in data_path, recursively process all /year/month/day/group datums starting from this index (e.g. for the path just given, a data_path_parse_index of 2 would start at the day path)
    :param workbook_path: Path to the directory with one or more publication workbooks. Note: this is not the path to a specific file. Only publication workbooks must be in this directory.
    """
    #workbook_path = os.environ['WORKBOOK_PATH']
    #out_path = os.environ['OUT_PATH']
    #data_path = os.path.join(os.environ['DATA_PATH'])

    # Import workbooks
    workbooks = []
    workbook_products = []
    for workbook_file in workbook_path.rglob('*'):
        if workbook_file.is_file():
            log.info(f'Loading workbook file {workbook_file}')
            workbook = pd.read_csv(workbook_file, delimiter='\t', keep_default_na=False)
            workbooks.append(workbook)
            workbook_products.append(workbook['dpID'].unique())
    if len(workbooks) == 0:
        log.fatal(f'No workbook files found in directory {workbook_path}')
        sys.exit(1)
    
    # Each PUBLOC is a datum (/product/year/month/day/PUBLOC). Run through each PUBLOC, starting at the parse index
    data_base_path_parts = data_path.parts[0:data_path_parse_index + 1]
    data_base_path = Path(*data_base_path_parts)
    for path in data_base_path.rglob(group_metadata_dir+'/'):
        parts = path.parts
        product = parts[product_index]
        group_metadata_name = parts[data_type_index]
        publoc_path = path.parent
        
        # Double check that the directory at the group metadata index matches the group directory name
        if group_metadata_name != group_metadata_dir:
            log.warn(f'Path {publoc_path} looks to be a datum, but the directory at the group metadata index ({group_metadata_name}) does not match the expected name ({group_metadata_dir}). Skipping...')
            continue
        else:
            log.info(f'Processing datum path {publoc_path}')

        group_metadata_path = Path(publoc_path, group_metadata_dir)
        data_path = Path(publoc_path, 'data')
        
        # get datum date
        data_path_parts = data_path.parts
        formatted_date = '-'.join(data_path_parts[year_index:year_index+3])
        
        # has data by output file
        has_data_by_file = {}
        
        # visibility by output file
        visibility_by_file = {}
    
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
            
            # Get additional group properties
            domain = group_data["features"][0]["domain"]
            hor = group_data["features"][0]["HOR"]
            ver = group_data["features"][0]["VER"]
            visibility = group_data["features"][0]["visibility_code"]

            # determine time indexes from data filenames
            group_data_path = os.path.join(data_path, group)
            data_files = os.listdir(group_data_path)
        
            output_path = os.path.join(out_path, product, day_path, site)
    
            # Find the relevant workbook
            workbook_index = [workbook_products.index(i) for i in workbook_products if i == 'NEON.DOM.SITE.'+product]
            if len(workbook_index) == 0:
                continue
            workbook=workbooks[workbook_index[0]]
            
            # Create each table in the workbook if possible
            for table in workbook['table'].unique():
                
                # Load the data file(s) corresponding to this table (must have already been placed into a parquet file for each table)
                data = None
                for file in data_files:
                    if table in file:
                        file_path = os.path.join(group_data_path, file) 
                        data = pd.read_parquet(file_path)
                        break
    
                # Skip if no data files relevant to this table
                if data is None:
                    log.warn(f'No filenames in {group_data_path} contain a match to table {table}')
                    continue
    
                # Get the table
                workbook_table=workbook.loc[workbook['table']==table,]
                
                # Get the full 45-digit DP IDs to grab fields from
                dp_number = [element for element in workbook_table['DPNumber'] if len(element)==45]
                dp_number = dp_number[0]
                
                # construct filenames
                dp_parts = dp_number.split('.')
                dp_parts[dp_parts.index('DOM')] = domain
                dp_parts[dp_parts.index('SITE')] = site
                tmi=dp_parts[9]
                dp_parts=dp_parts[0:6] # truncate to end of main data product ID
                dp_parts.extend([hor, ver, tmi, table])
                basic_filename_parts = dp_parts
                expanded_filename_parts = dp_parts.copy()
                basic_filename_parts.extend([formatted_date, 'basic', 'csv'])
                expanded_filename_parts.extend([formatted_date, 'expanded', 'csv'])
                basic_filename = '.'.join(basic_filename_parts)
                expanded_filename = '.'.join(expanded_filename_parts)
                basic_filepath = os.path.join(output_path, basic_filename)
                expanded_filepath = os.path.join(output_path, expanded_filename)
    
                # format columns
                for column in data.columns:
                    if not workbook_table['fieldName'].str.contains(column).any():
                        column_format = 'asIs'
                    else:
                        column_format = workbook_table['pubFormat'][workbook_table[workbook_table['fieldName'] == column].index.values[0]]
                    format_column(data, column, column_format)
                
                # add empty columns for any fields specified in workbook that are not present in the data
                data[workbook_table['fieldName'][workbook_table['fieldName'].isin(data.columns) == False]] = ""
    
                # determine data availability
                if product not in has_data_by_file.keys():
                    has_data_by_file[product]={}
                has_data_by_file[product][basic_filename] = not data[workbook_table.loc[
                    (workbook_table['dataCategory'] == 'Y') &
                    (workbook_table['downloadPkg'] == 'basic')]['fieldName'].values].isnull().values.all()
                has_data_by_file[product][expanded_filename] = \
                    not data[workbook_table.loc[(workbook_table['dataCategory'] == 'Y') & ((workbook_table['downloadPkg'] == 'basic') |
                                        (workbook_table['downloadPkg'] == 'expanded'))]['fieldName'].values].isnull().values.all()
                
                # Record portal visibility
                if product not in visibility_by_file.keys():
                    visibility_by_file[product]={}
                visibility_by_file[product][basic_filename] = visibility
                visibility_by_file[product][expanded_filename] = visibility
                
                # extract and write datasets
                os.makedirs(output_path, exist_ok=True)
                # basic
                basic_columns = workbook_table.loc[(workbook_table['downloadPkg'] == 'basic'), ['rank','fieldName']]
                data[basic_columns.sort_values('rank')['fieldName']].to_csv(basic_filepath, index=False)
                # expanded
                if workbook_table['downloadPkg'].str.contains('expanded').any():
                    expanded_columns = workbook_table.loc[((workbook_table['downloadPkg'] == 'basic') |
                                                     (workbook_table['downloadPkg'] == 'expanded')), ['rank', 'fieldName']]
                    data[expanded_columns.sort_values('rank')['fieldName']].to_csv(expanded_filepath, index=False)
            
        # Write manifest for each product
        for product in visibility_by_file.keys():
            try:
                output_path = os.path.join(out_path, product, day_path, site)
                write_manifest(output_path, has_data_by_file[product], visibility_by_file[product])
            except Exception:
                err_msg = sys.exc_info()
                # route datum to pfs/errored on ERROR
                # Remove any partial output for the datum
                err_datum_path(err=err_msg,DirDatm=path,DirErrBase='pfs/errored',RmvDatmOut=True,
                           DirOutBase=output_path)


def write_manifest(output_path: str, has_data_by_file: dict, visibility_by_file: dict):
    """Write metadata manifest."""
    manifest_filepath = os.path.join(output_path, 'manifest.csv')
    log.debug(f'Writing manifest file {manifest_filepath}')
    with open(manifest_filepath, 'w') as manifest_csv:
        writer = csv.writer(manifest_csv)
        writer.writerow(['file', 'hasData', 'visibility'])
        for key in has_data_by_file.keys():
            writer.writerow([key, has_data_by_file[key], visibility_by_file[key]])
    

# Apply pub format to a column of a dataframe
def format_column(dataframe: DataFrame, column: str, column_format: str):
    if column_format == "yyyy-MM-dd'T'HH:mm:ss'Z'(floor)":
        dataframe[column] = dataframe[column].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    if fnmatch.fnmatch(column_format, "*.*(round)"):
        py_format = '{:.' + str(column_format.count('#')) + 'f}'
        dataframe[column] = dataframe[column].map(py_format.format)

    if fnmatch.fnmatch(column_format, "signif_*(round)"):
        n_digits = column_format.count('#')
        dataframe[column] = [format_sig(element, n_digits) for element in dataframe[column]]
        # py_format = '{:.' + str(column_format.count('#')) + '}' # This works but doesn't limit to 7 decimal places
        # dataframe[column] = dataframe[column].map(py_format.format)
        
    if column_format == 'integer':
        dataframe[column] = dataframe[column].round().map('{:.0f}'.format)


def format_sig(element, n_digits):
    """Format a number to n_digits significant figures and at most 7 decimal places."""
    if pd.isna(element) or element == 0:
        rounded = element
    else:
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
