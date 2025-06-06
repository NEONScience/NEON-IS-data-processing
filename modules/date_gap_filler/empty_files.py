#!/usr/bin/env python3
from pathlib import Path
import structlog
import shutil

from date_gap_filler.date_gap_filler_config import DateGapFillerConfig


log = structlog.getLogger()


def link_files(config: DateGapFillerConfig, out_path: Path, location, year, month, day) -> None:
    output_directories = config.output_directories
    index = config.empty_file_type_index
    empty_file_path = config.empty_file_path
    create_directories(output_directories, out_path)
    symlink = config.symlink
    for path in empty_file_path.rglob('*'):
        if path.is_file():
            empty_file_type = path.parts[index]
            if empty_file_type in output_directories:
                link_empty_file(path, Path(out_path, empty_file_type), location, year, month, day, symlink)


def create_directories(output_directories: list, out_path: Path) -> None:
    for directory in output_directories:
        path = Path(out_path, directory)
        path.mkdir(parents=True, exist_ok=True)


def link_empty_file(path: Path, out_path: Path, location: str, year: str, month: str, day: str, symlink: bool) -> None:
    filename = path.name
    filename = filename.replace('location', location)
    filename = filename.replace('year', year)
    filename = filename.replace('month', month)
    filename = filename.replace('day', day)
    link_path = Path(out_path, filename)
    log.debug(f'source: {path}, link: {link_path}')
    link_path.parent.mkdir(parents=True, exist_ok=True)
    if not link_path.exists():
        if symlink:
            log.debug(f'Linking path {link_path} to {path}.')
            link_path.symlink_to(path)
        else:
            log.debug(f'Copying {path} to {link_path}.')
            shutil.copy2(path,link_path)
