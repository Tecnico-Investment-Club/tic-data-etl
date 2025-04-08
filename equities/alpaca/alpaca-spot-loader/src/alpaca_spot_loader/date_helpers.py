"""Date helper functions."""

from datetime import datetime, timezone, timedelta
from typing import Dict


SECONDS_PER_UNIT: Dict[str, int] = {
    "m": 60,
    "h": 60 * 60,
    "d": 24 * 60 * 60,
    "w": 7 * 24 * 60 * 60,
}

def interval_to_seconds(interval: str):
    """Converts interval to milliseconds."""
    return parse_interval_to_timedelta(interval).total_seconds()


def parse_interval_to_timedelta(interval_str: str) -> timedelta:
    value = int(interval_str[:-1])
    unit = interval_str[-1]
    total_seconds = value * SECONDS_PER_UNIT[unit]
    return timedelta(seconds=total_seconds)


def binance_timestamp_to_datetime(timestamp: int) -> datetime:
    """Converts Binance timestamp (ms) into datetime."""
    ts = timestamp / 1000
    # UTC
    return datetime.utcfromtimestamp(ts)


def datetime_to_binance_timestamp(d: datetime) -> int:
    """Converts datetime into Binance timestamp (ms)."""
    # UTC
    timestamp = int(d.replace(tzinfo=timezone.utc).timestamp() * 1000)
    return timestamp



def get_next_interval(interval: str, timestamp: datetime) -> datetime:
    """Gets timestamp of the next interval."""
    return timestamp + parse_interval_to_timedelta(interval)


def check_active(interval: str, d: datetime) -> bool:
    """Check if datetime is recent."""
    ts = datetime_to_binance_timestamp(d)
    lag1 = int(interval[:-1]) * SECONDS_PER_UNIT[interval[-1]] * 1000
    now = datetime_to_binance_timestamp(datetime.utcnow())
    if now - lag1 >= ts:
        return False
    else:
        return True
