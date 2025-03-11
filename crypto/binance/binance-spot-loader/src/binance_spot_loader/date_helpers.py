"""Date helper functions."""

from datetime import datetime, timezone
from typing import Dict


seconds_per_unit: Dict[str, int] = {
    "m": 60,
    "h": 60 * 60,
    "d": 24 * 60 * 60,
    "w": 7 * 24 * 60 * 60,
}


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


def interval_to_milliseconds(interval: str) -> int:
    """Convert a Binance interval string to milliseconds.

    Args:
        interval: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w

    Returns:
        int value of interval in milliseconds
    """
    return int(interval[:-1]) * seconds_per_unit[interval[-1]] * 1000


def get_next_interval(interval: str, timestamp: int) -> int:
    """Gets timestamp of the next interval."""
    return timestamp + interval_to_milliseconds(interval)


def check_active(interval: str, d: datetime) -> bool:
    """Check if datetime is recent."""
    ts = datetime_to_binance_timestamp(d)
    lag1 = int(interval[:-1]) * seconds_per_unit[interval[-1]] * 1000
    now = datetime_to_binance_timestamp(datetime.utcnow())
    if now - lag1 >= ts:
        return False
    else:
        return True
