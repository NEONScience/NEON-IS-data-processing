from datetime import datetime, timezone


def get_timestamp() -> datetime:
    """Returns the current time in UTC."""
    return datetime.now(timezone.utc)
