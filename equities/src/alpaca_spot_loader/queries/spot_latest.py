"""Latest Spot queries."""

from alpaca_spot_loader.queries.base import BaseQueriesLatest


class Queries(BaseQueriesLatest):
    """Latest Spot queries."""

    UPSERT = (
        "INSERT INTO alpaca.spot_{interval}_latest("
        "   symbol, "
        "   id, "
        "   latest_close, "
        "   active, "
        "   source "
        ") VALUES %s "
        "ON CONFLICT (symbol) DO "
        "UPDATE SET "
        "    symbol=EXCLUDED.symbol, "
        "    id=EXCLUDED.id, "
        "    latest_close=EXCLUDED.latest_close, "
        "    active=EXCLUDED.active, "
        "    source=EXCLUDED.source;"
    )

    CORRECT_TRADING_STATUS = (
        "UPDATE alpaca.spot_{interval}_latest SET "
        "   active=data.active "
        "FROM (VALUES %s) AS data (symbol, active) "
        "WHERE spot_1h_latest.symbol = data.symbol;"
    )
