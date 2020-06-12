#!/usr/bin/env python3
from pathlib import Path


class CalibrationFilePath(object):

    def __init__(self, *, source_type_index: int, source_id_index: int, stream_index: int):
        self.source_type_index = source_type_index
        self.source_id_index = source_id_index
        self.stream_index = stream_index

    def parse(self, path: Path):
        parts = path.parts
        source_type = parts[self.source_type_index]
        source_id = parts[self.source_id_index]
        stream = parts[self.stream_index]
        return source_type, source_id, stream
