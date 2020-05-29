#!/usr/bin/env python3
from pathlib import Path
from structlog import get_logger

from lib.file_linker import link


log = get_logger()


def write_manifests(manifests: dict, manifest_file_names: dict):
    """
    Write the manifest files.

    :param manifests: The manifests.
    :param manifest_file_names: The manifest file names.
    """
    for config_location in manifests.keys():
        with open(manifest_file_names[config_location], 'w') as f:
            for item in manifests[config_location]:
                f.write("%s\n" % item)


def link_thresholds(source_path: Path, destination_path: Path):
    """
    Link a threshold file if present in the source directory.

    :param source_path: The threshold file path.
    :param destination_path: The path to write the file.
    """
    threshold_dir = 'threshold'
    threshold_filename = 'thresholds.json'
    threshold_file = Path(source_path, threshold_dir, threshold_filename)
    if threshold_file.exists():
        path = destination_path.parent.parent
        threshold_link = Path(path, threshold_dir, threshold_filename)
        log.debug(f'Threshold file: {threshold_file} link: {threshold_link}')
        link(threshold_file, threshold_link)
