#!/usr/bin/env python3
from pathlib import Path
from structlog import get_logger
from typing import Dict, List
from datetime import datetime

from timeseries_padder.timeseries_padder.timeseries_padder_config import Config


log = get_logger()


def write_manifests(manifests: Dict[str, List[datetime]], manifest_file_names: Dict[str, Path]):
    """
    Write the manifest files.

    :param manifests: The manifests.
    :param manifest_file_names: The manifest file names.
    """
    for location in manifests.keys():
        with open(str(manifest_file_names[location]), 'w') as manifest_file:
            for date in manifests[location]:
                manifest_file.write("%s\n" % date)


def link_thresholds(source_path: Path, destination_path: Path):
    """
    Link a threshold file if present in the source directory.

    :param source_path: The data file path.
    :param destination_path: The path to write the file.
    """
    threshold_file = Path(source_path, Config.threshold_dir, Config.threshold_filename)
    if threshold_file.exists():
        path = destination_path.parent.parent
        link_path = Path(path, Config.threshold_dir, Config.threshold_filename)
        log.debug(f'threshold file: {threshold_file} link: {link_path}')
        link_path.parent.mkdir(parents=True, exist_ok=True)
        if not link_path.exists():
            link_path.symlink_to(threshold_file)
