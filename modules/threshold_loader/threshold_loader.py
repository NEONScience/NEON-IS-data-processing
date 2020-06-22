#!/usr/bin/env python3
from pathlib import Path
import json


def write_threshold_file(thresholds: list, out_path: Path):
    """
    Write a threshold file into the given output path.

    :param thresholds: The thresholds.
    :param out_path: The path for writing results.
    """
    out_path.mkdir(parents=True, exist_ok=True)
    threshold_file_path = Path(out_path, 'thresholds.json')
    with open(threshold_file_path, 'w') as threshold_file:
        threshold_data = {}
        threshold_data.update({'thresholds': thresholds})
        json_data = json.dumps(threshold_data, indent=4, sort_keys=False, default=str)
        threshold_file.write(json_data)
