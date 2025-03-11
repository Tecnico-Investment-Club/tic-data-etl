"""Latest model."""


from datetime import datetime
from typing import List, Tuple

from binance_spot_loader.model.base import BaseModel


class Latest(BaseModel):
    """Latest class."""

    symbol: str
    id: int
    open_time: datetime
    active: bool
    source: str

    @classmethod
    def build_record(cls, record: List) -> "Latest":
        """Build record object."""
        res = cls()

        res.symbol = record[0]
        res.id = record[1]
        res.open_time = record[2]
        res.active = record[3]
        res.source = record[4]

        return res

    def as_tuple(self) -> Tuple:
        """Get object as tuple."""
        return (self.symbol, self.id, self.open_time, self.active, self.source)
