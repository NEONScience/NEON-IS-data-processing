#!/usr/bin/env python3
import csv

from typing import Dict


def map_reader() -> Dict:
    """
    return a dictionary mapping macaddress with assetuid
    temporarily use csv file, will use database query later
    """
    maps = dict()
    with open('path_refactor/map.csv', mode='r') as f:
        reader = csv.reader(f)
        header = next(reader)
        maps = {row[0]: row[1] for row in reader}

    return maps
