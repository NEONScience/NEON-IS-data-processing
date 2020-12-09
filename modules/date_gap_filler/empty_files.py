#!/usr/bin/env python3
from pathlib import Path
import structlog

from date_gap_filler.date_gap_filler_config import DateGapFillerConfig


log = structlog.getLogger()


def link_files(config: DateGapFillerConfig, out_path: Path, location, year, month, day) -> None:
    output_directories = config.output_directories
    index = config.empty_file_type_index
    empty_file_path = config.empty_file_path
    create_directories(output_directories, out_path)
    for path in empty_file_path.rglob('*'):
        if path.is_file():
            empty_file_type = path.parts[index]
            if empty_file_type in output_directories:
                link_empty_file(path, Path(out_path, empty_file_type), location, year, month, day)


def create_directories(output_directories: list, out_path: Path) -> None:
    for directory in output_directories:
        path = Path(out_path, directory)
        path.mkdir(parents=True, exist_ok=True)


def link_empty_file(path: Path, out_path: Path, location: str, year: str, month: str, day: str) -> None:
    filename = path.name
    filename = filename.replace('location', location)
    filename = filename.replace('year', year)
    filename = filename.replace('month', month)
    filename = filename.replace('day', day)
    filename += '.empty'  # add extension to distinguish from real data files
    link_path = Path(out_path, filename)
    log.debug(f'source: {path}, link: {link_path}')
    link_path.parent.mkdir(parents=True, exist_ok=True)
    if not link_path.exists():
        link_path.symlink_to(path)
