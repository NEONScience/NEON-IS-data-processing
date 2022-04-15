#!/usr/bin/env python3
from date_gap_filler.date_gap_filler_config import DateGapFillerConfig
from date_gap_filler.file_linker import FileLinker


class DateGapFiller:

    def __init__(self, config: DateGapFillerConfig) -> None:
        self.config = config

    def fill_gaps(self) -> None:
        # Link relevant files
        if self.config.location_path is not None:
            file_linker = FileLinker(self.config)
            file_linker.link_files()
