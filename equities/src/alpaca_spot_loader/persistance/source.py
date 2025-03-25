"""Source."""

from datetime import datetime, timedelta
import logging
import os
from sys import stdout
from typing import List, Optional, Tuple, Union
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestBarRequest, TimeFrame
from alpaca.data.models import Bar, BarSet
from alpaca.data import TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.common.exceptions import APIError
from alpaca.trading.requests import GetAssetsRequest, AssetStatus, AssetExchange
from alpaca.trading.models import Asset
from alpaca.data.enums import DataFeed
from alpaca.trading.enums import AssetClass


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=stdout,
)
logger = logging.getLogger(__name__)


class Source:
    """Source class."""
    _market_data_client: StockHistoricalDataClient
    _trading_data_client: TradingClient

    def __init__(self, connection_string: str, interval: str) -> None:
        credentials = dict(kv.split("=") for kv in connection_string.split(" "))
        self._api_key = credentials["API_KEY"]
        self._secret_key = credentials["SECRET_KEY"]
        self.interval = interval

    def connect(self) -> None:
        """Connect to the Alpaca Rest API."""
        self._market_data_client = StockHistoricalDataClient(self._api_key, self._secret_key)
        self._trading_data_client = TradingClient(self._api_key, self._secret_key)
        self.ping()

    def ping(self) -> None:
        """Test Alpaca Rest API connection by fetching the latest bar."""
        try:
            request = StockLatestBarRequest(symbol_or_symbols="AAPL", feed=DataFeed.IEX)
            self._market_data_client.get_stock_latest_bar(request)
            logger.info("Connected to the Alpaca API.")
        except APIError as e:
            logger.info(f"Connection failed with status code {e.status_code}")

    def get_symbols(self) -> Optional[List[str]]:
        """Gets all symbols."""
        try:
            request = GetAssetsRequest(status=AssetStatus.ACTIVE,                                 
                                       asset_class=AssetClass.US_EQUITY)
            assets: List[Asset] = self._trading_data_client.get_all_assets(request)
            symbols = [asset.symbol for asset in assets if asset.exchange not in ["OTC", "CRYPTO"]]
            return symbols
        except APIError as e:
            logger.warning(f"Request failed with status code {e.status_code}")
            return None

    def get_trading_status(
        self, symbols: Optional[List[str]]
    ) -> Optional[List[Tuple[str, bool]]]:
        """Get trading status of the provided symbols."""
        try:
            request = GetAssetsRequest(status=AssetStatus.ACTIVE, 
                                       exchange=AssetExchange.NYSE)
            assets: List[Asset] = self._trading_data_client.get_all_assets(request)
            asset_status = []
            if symbols:
                asset_status = [
                    (asset.symbol, asset.tradable)
                    for asset in assets
                    if asset.symbol in symbols
                ]
            return asset_status
        except APIError as e:
            logger.warning(f"Request failed with status code {e.status_code}")
            return None

    def get_bars(
        self,
        symbol: Union[str, List[str]],
        interval: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 1000,
    ) -> Optional[Union[List[Bar], BarSet]]:
        """Get Alpaca bars."""
        try:
            request: StockBarsRequest = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=self.interval_to_timeframe(interval),
                start=start_time,
                feed=DataFeed.IEX,
                end=end_time,
                limit=limit,
            )
            bars = self._market_data_client.get_stock_bars(request)
            return bars[symbol] #if not isinstance(symbol, list) else bars
        except KeyError as e:
            logger.warning(f"No bars found for symbol {symbol}.")
            return None
        except APIError as e:
            logger.warning(f"Request failed with status code {e.status_code}")
            return None
    

    def get_earliest_valid_timestamp(self, symbol: str) -> Optional[int]:
        """Get earliest Alpaca timestamp for the provided symbol."""
        logger.info(f"Getting earliest timestamp for {symbol}...")
        bars = self.get_bars(
            symbol=symbol,
            interval=self.interval,
            start_time=datetime.min + timedelta(days=1), # 1 day after the minimum datetime since Alpaca does not support datetime.min
            end_time=datetime.now(),
            limit=1,
        )
        return bars[0].timestamp if bars else None
    
    def interval_to_timeframe(self, interval: str) -> TimeFrame:
        """Convert interval to TimeFrame."""
        units_to_timeframe = {
            "m": TimeFrameUnit.Minute,
            "h": TimeFrameUnit.Hour,
            "d": TimeFrameUnit.Day,
            "w": TimeFrameUnit.Week,
        }
        return TimeFrame(amount=int(interval[:-1]), unit=units_to_timeframe[interval[-1]])
