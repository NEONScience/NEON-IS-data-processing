#!/usr/bin/env python3
from datetime import datetime
from typing import NamedTuple


class DataSpan(NamedTuple):
    start: datetime
    end: datetime


def get_data_span(start: datetime, end: datetime) -> DataSpan:
    return DataSpan(start, end)
