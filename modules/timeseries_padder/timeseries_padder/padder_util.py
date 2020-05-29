#!/usr/bin/env python3
import math
from datetime import datetime, timedelta
import json
import yaml
from yaml import Loader


def convert_window_size(window_size: int, data_rate: float):
    """
    Divide the window size by the data rate.

    :param window_size: window size in number of data points
    :param data_rate: the data rate in Hz
    :returns: window size in seconds
    """
    if window_size < 0:
        raise ValueError("windowSize cannot be less than 0")
    if data_rate <= 0:
        raise ValueError("dataRate must be greater than 0")
    return window_size / data_rate


def calculate_pad_size(window_size: int):
    """
    Calculate the data pad size in days.

    :param window_size: window size in seconds
    :returns: padSize in days
    """
    if window_size < 0:
        raise ValueError("windowSize cannot be less than 0")
    seconds_per_day = 86400.
    return math.ceil(window_size / seconds_per_day)


def get_dates_in_padded_range(date, pad_size):
    """
    Get all the dates in the padded date range.

    :param date: date to pad
    :type date: datetime
    :param pad_size: pad size in days
    :type pad_size: float
    :returns: Sorted array of dates whose padded ranges include date.
    """
    pad_in_days = math.ceil(pad_size)
    padded_range = [date]
    if pad_size < 0:  # pad size is negative, only go backward in time by the number of days.
        for day in range(1, abs(pad_in_days) + 1):
            padded_range.append(date - timedelta(days=day))
    else:  # pad size is positive, go backward and forward in time by the number of days.
        for day in range(1, pad_in_days + 1):
            padded_range.append(date - timedelta(days=day))
            padded_range.append(date + timedelta(days=day))
    return sorted(padded_range)


def get_max_window_size(threshold_file: str, data_rate: int):
    """
    Get the maximum window size.

    :param threshold_file: json file containing window sizes in either points or seconds
    :param data_rate: the data rate in Hz
    :returns: max window size
    """
    with open('timeseries_padder/config/windowSizeNames.yaml', 'r') as file:
        window_size_yaml = yaml.load(file, Loader=Loader)
    with open(threshold_file, "r") as jsonFile:
        threshold_json = json.load(jsonFile)
    max_window_size = 0
    this_window_size = 0
    for threshold in threshold_json['thresholds']:
        if threshold['threshold_name'] in window_size_yaml['windowSizesInPoints']:
            this_window_size = convert_window_size(threshold['number_value'], data_rate)
        if threshold['threshold_name'] in window_size_yaml['windowSizesInSeconds']:
            this_window_size = threshold['number_value']
        max_window_size = this_window_size if this_window_size > max_window_size else max_window_size
    return max_window_size


def get_min_data_rate(location_file: str):
    """
    This should be refactored to extract from engineering metadata
    the actual rate(s) at which the sensor produced data.

    :param location_file: yaml file containing location metadata
    :returns: The data rate.
    """
    with open(location_file, "r") as jsonFile:
        location_json = json.load(jsonFile)
    return float(location_json['features'][0]['Data Rate'])
