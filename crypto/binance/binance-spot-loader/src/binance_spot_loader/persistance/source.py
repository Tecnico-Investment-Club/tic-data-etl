"""Source."""

import hashlib
import hmac
import logging
import os
from sys import stdout
import time
from typing import Dict, List, Optional, Tuple

import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=stdout,
)
logger = logging.getLogger(__name__)


class Source:
    """Source class."""

    _api_url = "https://api.binance.com/api/"
    _version = "v3/"
    base_url = _api_url + _version

    _headers: Dict[str, str]
    _session: requests.Session

    mkt_cap_filter: int = 5_000_000

    def __init__(self, connection_string: str, interval: str) -> None:
        credentials = dict(kv.split("=") for kv in connection_string.split(" "))

        self._api_key = credentials["API_KEY"]
        self._secret_key = credentials["SECRET_KEY"]
        self.interval = interval

    def connect(self) -> None:
        """Connect to the Binance Rest API."""
        self._session = requests.Session()
        self._headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",  # noqa: B950
            "X-MBX-APIKEY": self._api_key,
        }
        self._session.headers.update(self._headers)

        self.ping()

    def ping(self) -> None:
        """Ping Binance Rest API."""
        url = f"{self.base_url}ping"
        response = self._session.get(url)

        if response.status_code == 200:
            logger.info("Connected to the Binance API.")
        else:
            logger.info(f"Connection failed with status code {response.status_code}")

    def get_symbols(
        self, quote_symbols: Optional[Dict[str, int]]
    ) -> Optional[List[str]]:
        """Gets all symbols quoted in the provided currencies (and their length)."""
        url = f"{self.base_url}exchangeInfo"
        response = self._session.get(url)

        if response.status_code == 200:
            symbols = []
            if quote_symbols:
                for quote_symbol, length in quote_symbols.items():
                    temp_symbols = [
                        symbol["symbol"]
                        for symbol in response.json()["symbols"]
                        if symbol["symbol"][-length:] == quote_symbol
                    ]
                    symbols.extend(temp_symbols)
            else:
                symbols = [symbol["symbol"] for symbol in response.json()["symbols"]]
            return symbols
        else:
            logger.warning(f"Request failed with status code {response.status_code}")
            return None

    def get_trading_status(
        self, symbols: Optional[List[str]]
    ) -> Optional[List[Tuple[str, str]]]:
        """Get trading status of the provided symbols."""
        url = f"{self.base_url}exchangeInfo"
        response = self._session.get(url)

        if response.status_code == 200:
            symbol_status = []
            if symbols:
                symbol_status = [
                    (symbol["symbol"], symbol["status"])
                    for symbol in response.json()["symbols"]
                    if symbol["symbol"] in symbols
                ]
            return symbol_status
        else:
            logger.warning(f"Request failed with status code {response.status_code}")
            return None

    def get_klines(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1000,
    ) -> Optional[List[List]]:
        """Get Binance klines."""
        url = f"{self.base_url}klines"
        if start_time is not None and end_time is not None:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": start_time,
                "endTime": end_time,
                "limit": limit,
            }
        elif start_time is not None:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": start_time,
                "limit": limit,
            }
        else:
            params = {"symbol": symbol, "interval": interval, "limit": limit}

        timestamp = str(int(time.time() * 1000))
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        signature = hmac.new(
            self._secret_key.encode("utf-8"),
            f"{query_string}&timestamp={timestamp}".encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        self._headers["X-MBX-TIMESTAMP"] = timestamp
        self._headers["X-MBX-SIGNATURE"] = signature
        self._session.headers.update(self._headers)

        response = self._session.get(url, params=params)

        if response.status_code == 200:
            # Print the response data
            data = response.json()
            return data
        else:
            # Print the error message
            logger.warning(f"Request failed with status code {response.status_code}")
            return None

    def get_earliest_valid_timestamp(self, symbol: str) -> Optional[int]:
        """Get earliest Binance timestamp for the provided symbol."""
        logger.info(f"Getting earliest timestamp for {symbol}...")
        kline = self.get_klines(
            symbol=symbol,
            interval=self.interval,
            start_time=0,
            end_time=int(time.time() * 1000),
            limit=1,
        )
        return kline[0][0] if kline else None
