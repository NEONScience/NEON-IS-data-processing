#!/usr/bin/env python3
from typing import NamedTuple, Optional


class ActivePeriod(NamedTuple):
    start_date: str
    end_date: Optional[str]
