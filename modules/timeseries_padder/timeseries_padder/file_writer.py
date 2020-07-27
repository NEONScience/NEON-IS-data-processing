#!/usr/bin/env python3
from pathlib import Path
from structlog import get_logger
from typing import Dict, List
from datetime import datetime

from timeseries_padder.timeseries_padder.timeseries_padder_config import Config


log = get_logger()


def write_manifests(manifests: Dict[str, List[datetime]], manifest_paths: Dict[str, Path]) -> None:
    """
    Write the manifest files.

    :param manifests: The manifests.
    :param manifest_paths: The manifest file names.
    """
    for key in manifests.keys():
        with open(str(manifest_paths[key]), 'w') as manifest_file:
            for date in manifests[key]:
                log.debug(f'writing date {date} into {manifest_paths[key]}')
                manifest_file.write("%s\n" % date)


def write_manifest(manifest: List[datetime], manifest_path: Path) -> None:
    """
    Write the manifest files.

    :param manifest: The manifest dates.
    :param manifest_path: The manifest path.
    """
    with open(str(manifest_path), 'w') as manifest_file:
        for date in manifest:
            log.debug(f'writing date {date} into {manifest_path}')
            manifest_file.write("%s\n" % date)


def link_thresholds(source_path: Path, destination_path: Path) -> None:
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
