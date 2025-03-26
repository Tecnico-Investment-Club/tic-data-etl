"""Kline model."""

from datetime import datetime
from decimal import Decimal
from typing import List, Tuple, Optional

import alpaca_spot_loader.date_helpers as date_helpers
from alpaca_spot_loader.model.base import BaseModel
from alpaca.data.models import Bar


class BarRecord:
    """BarRecord class."""
    id: int
    symbol: str
    open_time: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume_stock: Decimal
    volume_dollar: Decimal
    vwap: Decimal
    trades: int

    @classmethod
    def build_record(cls, id: int, bar: Bar) -> "BarRecord":
        """Build record object."""
        record = cls()
        record.id = id
        record.symbol = bar.symbol
        record.open_time = bar.timestamp
        record.open_price = Decimal(bar.open)
        record.high_price = Decimal(bar.high)
        record.low_price = Decimal(bar.low)
        record.close_price = Decimal(bar.close)
        record.volume_stock = Decimal(bar.volume)
        record.volume_dollar = Decimal(bar.volume * bar.vwap)
        record.vwap = Decimal(bar.vwap)
        record.trades = int(bar.trade_count)
        return record

    def as_tuple(self) -> Tuple:
        """Get object as tuple."""
        return (
            self.id,
            self.symbol,
            self.open_time,
            self.open_price,
            self.high_price,
            self.low_price,
            self.close_price,
            self.volume_stock,
            self.volume_dollar,
            self.vwap,
            self.trades,
        )

    def __repr__(self) -> str:
        return f"{self.symbol}, {self.open_time}"
