#!/usr/bin/env python3
from typing import NamedTuple, Optional


class Asset(NamedTuple):
    id: int
    type: str
    model: Optional[str] = None
    manufacturer: Optional[str] = None
    software_version: Optional[str] = None
