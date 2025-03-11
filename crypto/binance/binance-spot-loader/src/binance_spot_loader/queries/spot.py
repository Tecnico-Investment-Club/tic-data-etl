"""Spot 1h queries."""

from binance_spot_loader.queries.base import BaseQueries


class Queries(BaseQueries):
    """Spot 1h queries."""

    UPSERT = (
        "INSERT INTO binance.spot_{interval} ("
        "   id, "
        "   symbol, "
        "   open_time, "
        "   open_price, "
        "   high_price, "
        "   low_price, "
        "   close_price, "
        "   volume, "
        "   close_time, "
        "   quote_volume, "
        "   trades, "
        "   taker_buy_volume, "
        "   taker_buy_quote_volume "
        ") VALUES %s "
        "ON CONFLICT (symbol, open_time) DO "
        "UPDATE SET "
        "    symbol=EXCLUDED.symbol,"
        "    open_time=EXCLUDED.open_time,"
        "    open_price=EXCLUDED.open_price,"
        "    high_price=EXCLUDED.high_price,"
        "    low_price=EXCLUDED.low_price,"
        "    close_price=EXCLUDED.close_price,"
        "    volume=EXCLUDED.volume,"
        "    close_time=EXCLUDED.close_time,"
        "    quote_volume=EXCLUDED.quote_volume,"
        "    trades=EXCLUDED.trades,"
        "    taker_buy_volume=EXCLUDED.taker_buy_volume,"
        "    taker_buy_quote_volume=EXCLUDED.taker_buy_quote_volume;"
    )
