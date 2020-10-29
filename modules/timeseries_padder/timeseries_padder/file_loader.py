import sys
import os
from yaml import Loader
import yaml
import json
from pathlib import Path

import structlog

log = structlog.getLogger()


def load_window_size_file():
    try:
        this_path = Path(os.path.dirname(__file__))
        file_path = Path(this_path.parent, 'config/windowSizeNames.yaml')
        log.debug(f'yaml path: {file_path}')
        with open(file_path) as file:
            return yaml.load(file, Loader=Loader)
    except FileNotFoundError:
        log.error('Could not open window size names yaml file.')
        sys.exit(-1)


def load_threshold_file(threshold_file: str):
    try:
        with open(threshold_file, "r") as jsonFile:
            return json.load(jsonFile)
    except FileNotFoundError:
        log.error('Could not open threshold file.')
        sys.exit(-1)
