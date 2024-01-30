from datetime import datetime, timedelta


def format_date(date: datetime, publication_format: str):
    """Format a datetime value according to the given publication format."""
    if publication_format == "yyyy-MM-dd'T'HH:mm:ss'Z'(round)":
        return time_round(date, delta=timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    elif publication_format == "yyyy-MM(floor)":
        return date.strftime('%Y-%m')
    elif publication_format == "yyyy-MM-dd'T'HH'Z'(round)":
        return time_round(date, delta=timedelta(hours=1)).strftime('%Y-%m-%dT%HZ')
    elif publication_format == "yyyy-MM-dd'T'HH:mm:ss'Z'(floor)":
        return time_floor(date, delta=timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:SZ')
    elif publication_format == "yyyy-MM-dd(round)":
        return date.strftime('%Y-%m-%d')
    elif publication_format == "yyyy-MM-dd'T'HH:mm'Z'(floor)":
        return time_floor(date, delta=timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%MZ')
    elif publication_format == "yyyy-MM-dd(floor)":
        return time_floor(date, delta=timedelta(days=1)).strftime('%Y-%m-%d')
    elif publication_format == "yyyy(floor)":
        return date.strftime('%Y')
    elif publication_format == "yyyy-MM-dd'T'HH'Z'(floor)":
        return time_floor(date, delta=timedelta(hours=1)).strftime('%Y-%m-%dT%HZ')
    elif publication_format == "yyyy-MM-dd'T'HH:mm'Z'(round)":
        return time_round(date, delta=timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%MZ')
    else:
        return date.strftime('%Y-%m-%dT%H:%M:SZ')


def time_mod(time: datetime, delta: timedelta, epoch=None):
    if epoch is None:
        epoch = datetime(1970, 1, 1, tzinfo=time.tzinfo)
    return (time - epoch) % delta


def time_round(time: datetime, delta: timedelta, epoch=None) -> datetime:
    mod = time_mod(time, delta, epoch)
    if mod < delta / 2:
       return time - mod
    return time + (delta - mod)


def time_floor(time: datetime, delta: timedelta, epoch=None) -> datetime:
    mod = time_mod(time, delta, epoch)
    return time - mod


def time_ceil(time: datetime, delta: timedelta, epoch=None) -> datetime:
    mod = time_mod(time, delta, epoch)
    if mod:
        return time + (delta - mod)
    return time
