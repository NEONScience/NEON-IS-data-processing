from datetime import datetime
from typing import Optional


def to_datetime(date: str) -> Optional[datetime]:
    """Convert the dates in files to datetime objects."""
    if date is not None:
        return datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
    else:
        return None
