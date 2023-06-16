from datetime import datetime


def format_date(date: datetime) -> str:
    return date.strftime('%Y%m%d')

def format_dashed_date(date: datetime) -> str:
    return date.strftime('%Y-%m-%d')
