#!/usr/bin/env python3
import math
from datetime import datetime, timedelta
import json
import structlog
from typing import Union, List

import timeseries_padder.timeseries_padder.file_loader as file_loader

log = structlog.getLogger()


def convert_window_size(window_size: int, data_rate: float) -> float:
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


def calculate_pad_size(window_size: int) -> Union[float, int]:
    """
    Calculate the data pad size in days.

    :param window_size: window size in seconds
    :returns: padSize in days
    """
    if window_size < 0:
        raise ValueError("windowSize cannot be less than 0")
    seconds_per_day = 86400.
    pad_size = math.ceil(window_size / seconds_per_day)
    return pad_size


def get_padded_dates(date: datetime, pad_size: float) -> List[datetime]:
    """
    Get all the dates in the padded date range.

    :param date: date to pad
    :param pad_size: pad size in days
    :returns: Sorted array of dates whose padded ranges include date.
    """
    pad_days = math.ceil(pad_size)
    padded_range = [date]
    if pad_size < 0:  # pad size is negative, only go backward in time by the number of days.
        for day in range(1, abs(pad_days) + 1):
            padded_range.append(date - timedelta(days=day))
    else:  # pad size is positive, go backward and forward in time by the number of days.
        for day in range(1, pad_days + 1):
            padded_range.append(date - timedelta(days=day))
            padded_range.append(date + timedelta(days=day))
    return sorted(padded_range)


def get_max_window_size(threshold_file: str, data_rate: float) -> Union[float, int]:
    """
    Get the maximum window size.

    :param threshold_file: json file containing window sizes in either points or seconds
    :param data_rate: the data rate in Hz
    :returns: max window size
    """
    window_size_yaml = file_loader.load_window_size_file()
    threshold_json = file_loader.load_threshold_file(threshold_file)
    max_window_size = 0
    this_window_size = 0
    for threshold in threshold_json['thresholds']:
        if threshold['threshold_name'] in window_size_yaml['windowSizesInPoints']:
            this_window_size = convert_window_size(threshold['number_value'], data_rate)
        if threshold['threshold_name'] in window_size_yaml['windowSizesInSeconds']:
            this_window_size = threshold['number_value']
        max_window_size = this_window_size if this_window_size > max_window_size else max_window_size
    return max_window_size


def get_data_rate(location_file: str) -> float:
    """
    This should be refactored to extract from engineering metadata
    the actual rate(s) at which the sensor produced data.

    :param location_file: yaml file containing location metadata
    :returns: The data rate.
    """
    with open(location_file, "r") as file:
        location_json = json.load(file)
        data_rate = location_json['features'][0]['Data Rate']
    return float(data_rate)
