import os
import sys
import pathlib
import datetime

from structlog import get_logger

import lib.file_linker as file_linker
import timeseries_padder.timeseries_padder.padder_util as padder_util
import timeseries_padder.timeseries_padder.output_writer as output_writer


log = get_logger()


class Padder(object):

    def __init__(self, data_path, out_path, year_index, month_index, day_index, loc_index, subdir_index):
        self.data_path = data_path
        self.out_path = out_path
        # date and location indices in file path
        self.year_index = year_index
        self.month_index = month_index
        self.day_index = day_index
        self.config_location_index = loc_index
        self.sub_dir_index = subdir_index
        self.sub_dirs_to_output = ['data']
        self.out_dir_parts = list(pathlib.Path(out_path).parts)

    def pad(self):
        try:
            max_window_size_by_date_and_location = {}
            min_data_rate_by_date_and_location = {}
            manifests = {}
            manifest_file_names = {}
            for root, dirs, files in os.walk(self.data_path):
                for filename in files:
                    if not filename.startswith('.'):
                        file_path = os.path.join(root, filename)
                        parts = pathlib.Path(file_path).parts
                        subdir = parts[self.sub_dir_index]
                        if subdir in self.sub_dirs_to_output:
                            year = parts[self.year_index]
                            month = parts[self.month_index]
                            day = parts[self.day_index]
                            config_location = parts[self.config_location_index]
                            date_location_key = year+month+day+config_location

                            config_location_path = os.path.join(*parts[0:self.config_location_index + 1])

                            if config_location not in manifests:
                                manifests[config_location] = []

                            # get min of all data rates (to ensure adequate window coverage)
                            if date_location_key not in min_data_rate_by_date_and_location:
                                location_path = os.path.join(config_location_path, 'location')
                                location_files = [f for f in os.listdir(location_path) if f.endswith('.json')]
                                location_file = os.path.join(location_path, location_files[0])
                                min_data_rate_by_date_and_location[date_location_key] = \
                                    padder_util.get_min_data_rate(location_file)
                            data_rate = min_data_rate_by_date_and_location[date_location_key]

                            # get max of all window sizes
                            if date_location_key not in max_window_size_by_date_and_location:
                                threshold_path = os.path.join(config_location_path, 'threshold')
                                threshold_files = [f for f in os.listdir(threshold_path) if f.endswith('.json')]
                                threshold_file = os.path.join(threshold_path, threshold_files[0])
                                log.debug(f'thresholdFile: {threshold_file}')
                                max_window_size_by_date_and_location[date_location_key] = \
                                    padder_util.get_max_window_size(threshold_file, data_rate)
                            window_size = max_window_size_by_date_and_location[date_location_key]

                            # get data date
                            date = datetime.date(int(year), int(month), int(day))

                            # calculate pad size
                            pad_size = padder_util.calculate_pad_size(window_size)

                            # get dates in padded range
                            dates_in_padded_range = padder_util.get_dates_in_padded_range(date, pad_size)

                            # link file into each date in padded range
                            destination_parts = list(parts)
                            for idx in range(1, len(self.out_dir_parts)):
                                destination_parts[idx] = self.out_dir_parts[idx]
                            for dateInPaddedRange in dates_in_padded_range:
                                destination_parts[self.year_index] = str(dateInPaddedRange.year)
                                destination_parts[self.month_index] = str(dateInPaddedRange.month).zfill(2)
                                destination_parts[self.day_index] = str(dateInPaddedRange.day).zfill(2)
                                # generate destination path
                                destination_path = os.path.join(*destination_parts)
                                log.debug(f'source: {file_path}')
                                log.debug(f'destination: {destination_path}')
                                file_linker.link(file_path, destination_path)
                                manifests[config_location].append(dateInPaddedRange)
                                if dateInPaddedRange == date:
                                    # construct manifest filename
                                    manifest_path = os.path.dirname(destination_path)  # remove data file name
                                    manifest_file_names[config_location] = os.path.join(manifest_path, 'manifest.txt')
                                output_writer.write_thresholds(config_location_path, destination_path)
            output_writer.write_manifests(manifests, manifest_file_names)  # write manifest files

        except Exception:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            log.error("Exception at line " + str(exc_tb.tb_lineno) + ": " + str(sys.exc_info()))
