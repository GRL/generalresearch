from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from dateutil import relativedelta


def get_date_list(start_datetime: datetime, end_datetime: Optional[datetime] = None):
    start_datetime = start_datetime.replace(tzinfo=timezone.utc)
    end_datetime = end_datetime if end_datetime else datetime.now(tz=timezone.utc)
    return (
        pd.date_range(start_datetime, end_datetime, freq="1D")
        .strftime("%Y-%m-%d")
        .tolist()
    )


def year_start(periods_ago: int = 6) -> datetime:
    """
    Returns the starting date of the last N Full
    years. Goal is to provide a simple way to
    know when to do filters from
    """
    n: datetime = datetime.now(tz=timezone.utc)
    d: datetime = n - relativedelta.relativedelta(years=periods_ago)
    return d.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)


def month_start(periods_ago: int = 6) -> datetime:
    """
    Returns the starting date of the last N Full
    months. Goal is to provide a simple way to
    know when to do filters from
    """
    n: datetime = datetime.now(tz=timezone.utc)
    d: datetime = n - relativedelta.relativedelta(months=periods_ago)
    return d.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def day_start(periods_ago: int = 6) -> datetime:
    """
    Returns the starting date of the last N Full
    days. Goal is to provide a simple way to
    know when to do filters from
    """
    n: datetime = datetime.now(tz=timezone.utc)
    d: datetime = n - relativedelta.relativedelta(days=periods_ago)
    return d.replace(hour=0, minute=0, second=0, microsecond=0)


def hour_start(periods_ago: int = 6) -> datetime:
    """
    Returns the starting date of the last N Full
    hours. Goal is to provide a simple way to
    know when to do filters from
    """
    n: datetime = datetime.now(tz=timezone.utc)
    d: datetime = n - relativedelta.relativedelta(hours=periods_ago)
    return d.replace(minute=0, second=0, microsecond=0)
