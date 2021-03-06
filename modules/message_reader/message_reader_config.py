from typing import NamedTuple
from pathlib import Path


class Config(NamedTuple):
    out_path: Path
    bootstrap_server: str
    topic: str
    group_id: str
    auto_offset_reset: str
    enable_auto_commit: bool
    is_test: bool
