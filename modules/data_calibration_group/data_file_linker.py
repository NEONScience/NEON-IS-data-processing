#!/usr/bin/env python3
from pathlib import Path
from typing import NamedTuple
from typing import Iterator

import structlog

from data_calibration_group.data_filename import DataFilename
from data_calibration_group.data_calibration_group_config import Config
from data_calibration_group.data_path_parser import DataPathParser

log = structlog.get_logger()


class SourcePath(NamedTuple):
    source_id: str
    output_path: Path


class DataFileLinker:

    def __init__(self, config: Config):
        self.data_path = config.data_path
        self.out_path = config.out_path
        self.path_parser = DataPathParser(config)

    def link_files(self) -> Iterator[SourcePath]:
        """
        Link the data files into the output path and yield the source ID and output path for each data file.
        """
        for path in self.data_path.rglob('*'):
            if path.is_file():
                log.debug(f'data file path: {path}')
                source_type, year, month, day = self.path_parser.parse(path)
                source_id = DataFilename(path.name).source_id()
                log.debug(f'type: {source_type} Y: {year} M: {month} D: {day} id: {source_id} file: {path.name}')
                output_path = Path(self.out_path, source_type, year, month, day, source_id)
                link_path = Path(output_path, 'data', path.name)
                link_path.parent.mkdir(parents=True, exist_ok=True)
                link_path.symlink_to(path)
                yield SourcePath(source_id, output_path)
