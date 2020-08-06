#!/usr/bin/env python3
import os
import sys
from pathlib import Path

from structlog import get_logger

log = get_logger()


class Egress:

    def __init__(self, data_path: Path, out_path: Path, output_name: str, date_index: int, location_index: int) -> None:
        """
        Constructor.

        :param data_path: The data path.
        :param out_path: The output path for writing results.
        :param output_name: The output name.
        :param date_index: The date index.
        :param location_index: The location index.
        """
        self.output_name = output_name
        self.data_path = data_path
        self.out_path = out_path
        # date and location indices refer to locations within the filename (not the path)
        self.date_index = date_index
        self.location_index = location_index
        self.filename_delimiter = "_"

    def upload(self) -> None:
        """Link the source files into the output directory."""
        try:
            for root, dirs, files in os.walk(str(self.data_path)):
                for filename in files:
                    file_path = Path(root, filename)
                    filename_parts = filename.split(self.filename_delimiter)
                    date_time = filename_parts[self.date_index]
                    location = filename_parts[self.location_index]
                    # construct link filename
                    link_parts = [self.out_path, self.output_name, date_time, location,
                                  filename_parts[len(filename_parts) - 2],
                                  filename_parts[len(filename_parts) - 1]]
                    link_filename = self.filename_delimiter.join(link_parts[1:])
                    link_path = Path(*link_parts[:len(link_parts) - 2], link_filename)
                    log.debug(f'source_path: {file_path} link_path: {link_path}')
                    link_path.parent.mkdir(parents=True, exist_ok=True)
                    if not link_path.exists():
                        link_path.symlink_to(file_path)
        except Exception:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            log.error("Exception at line " + str(exc_tb.tb_lineno) + ": " + str(sys.exc_info()))
