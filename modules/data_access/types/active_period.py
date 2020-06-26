#!/usr/bin/env python3
from typing import NamedTuple, Optional
from datetime import datetime


class ActivePeriod(NamedTuple):
    start_date: datetime
    end_date: Optional[datetime]
