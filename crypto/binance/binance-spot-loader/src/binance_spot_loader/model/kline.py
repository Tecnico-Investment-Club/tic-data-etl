"""Kline model."""

from datetime import datetime
from decimal import Decimal
from typing import List, Tuple

import binance_spot_loader.date_helpers as date_helpers
from binance_spot_loader.model.base import BaseModel


class Kline(BaseModel):
    """Kline class."""

    id: int
    symbol: str
    open_time: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    close_time: datetime
    quote_volume: Decimal
    trades: int
    taker_buy_volume: Decimal
    taker_buy_quote_volume: Decimal

    @classmethod
    def build_record(cls, record: List) -> "Kline":
        """Build record object."""
        res = cls()

        res.id = record[0]
        res.symbol = record[1]
        res.open_time = date_helpers.binance_timestamp_to_datetime(record[2])
        res.open_price = Decimal(record[3])
        res.high_price = Decimal(record[4])
        res.low_price = Decimal(record[5])
        res.close_price = Decimal(record[6])
        res.volume = Decimal(record[7])
        res.close_time = date_helpers.binance_timestamp_to_datetime(record[8])
        res.quote_volume = Decimal(record[9])
        res.trades = int(record[10])
        res.taker_buy_volume = Decimal(record[11])
        res.taker_buy_quote_volume = Decimal(record[12])

        return res

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
            self.volume,
            self.close_time,
            self.quote_volume,
            self.trades,
            self.taker_buy_volume,
            self.taker_buy_quote_volume,
        )

    def __repr__(self) -> str:
        return f"{self.symbol}, {self.open_time}"
