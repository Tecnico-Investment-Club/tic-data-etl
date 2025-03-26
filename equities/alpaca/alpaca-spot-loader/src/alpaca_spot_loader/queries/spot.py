"""Spot 1h queries."""

from alpaca_spot_loader.queries.base import BaseQueries


class Queries(BaseQueries):
    """Spot 1h queries."""

    UPSERT = (
         "INSERT INTO alpaca.spot_{interval} ("
        "   id, "
        "   symbol, "
        "   open_time, "
        "   open_price, "
        "   high_price, "
        "   low_price, "
        "   close_price, "
        "   volume_stock, "
        "   volume_dollar, "
        "   vwap, "
        "   trades "
        ") VALUES %s "
        "ON CONFLICT (symbol, open_time) DO "
        "UPDATE SET "
        "    symbol=EXCLUDED.symbol,"
        "    open_time=EXCLUDED.open_time,"
        "    open_price=EXCLUDED.open_price,"
        "    high_price=EXCLUDED.high_price,"
        "    low_price=EXCLUDED.low_price,"
        "    close_price=EXCLUDED.close_price,"
        "    volume_stock=EXCLUDED.volume_stock,"
        "    volume_dollar=EXCLUDED.volume_dollar,"
        "    vwap=EXCLUDED.vwap,"
        "    trades=EXCLUDED.trades;"
    )
