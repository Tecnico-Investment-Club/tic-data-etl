"""Main."""

import argparse
from datetime import datetime, timedelta
import logging
import os
import secrets
from sys import stdout
import time
from typing import Dict, List, Optional, Tuple

import binance_spot_loader.date_helpers as date_helpers
from binance_spot_loader.model import Kline, Latest
from binance_spot_loader.persistance import source, target
from binance_spot_loader.queries import SpotQueries, SpotLatestQueries

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=stdout,
)
logger = logging.getLogger(__name__)


class Loader:
    """Loader class."""

    _source: source.Source
    _target: target.Target

    _interval: str
    _quote_symbols: Dict[str, int]
    _base_symbols: Dict[str, int]
    _n_active_symbols: int

    def __init__(self) -> None:
        self.source_name = "BINANCE"
        self.mode = "FAST"
        self.n_requests = 1
        self._schema = "binance"

    def setup(self, args: argparse.Namespace) -> None:
        """Set up loader and connections."""
        self._source = source.Source(args.interval)
        self._target = target.Target(args.target)
        self._interval = args.interval
        quote_symbols_str = args.quote_symbols
        if quote_symbols_str:
            self._quote_symbols = dict(
                (symbol, len(symbol)) for symbol in quote_symbols_str.split(sep=",")
            )
        base_symbols_str = args.base_symbols
        if base_symbols_str:
            self._base_symbols = dict(
                (symbol, len(symbol)) for symbol in base_symbols_str.split(sep=",")
            )
        else:
            self._base_symbols = {}

        self._source.connect()
        self._target.connect()

    def check_request_limit(self) -> None:
        """Check if loader has made 1000 requests."""
        self.n_requests += 1
        if self.n_requests >= 1000:
            logger.info("Waiting 1m before requesting more...")
            time.sleep(60)
            self.n_requests = 1

    def run_once(self, symbol_lst: List[str]) -> None:
        """Run process once."""
        self.mode = "SLOW"
        start = datetime.utcnow()

        keys = self.get_keys(symbol_lst)
        logger.info(f"Processing {self._n_active_symbols} symbols.")

        record_objs = []
        new_latest = []
        i = 1
        for symbol, start_time in keys:
            logger.info(f"Processing {symbol} ({i}/{self._n_active_symbols})...")
            i += 1

            raw_records = self._source.get_klines(
                symbol=symbol, interval=self._interval, start_time=start_time
            )
            if not raw_records:
                logger.warning(f"No response for symbol: {symbol}.")
                continue
            self.check_request_limit()

            symbol_record_objs = []
            for record in raw_records:
                record_id = self._target.get_next_id(self._schema, self._interval)
                symbol_record_objs.append(
                    Kline.build_record([record_id, symbol] + record)
                )
            new_latest.append(self.latest_closed(symbol, symbol_record_objs))
            record_objs.extend(symbol_record_objs)

        records = [record.as_tuple() for record in record_objs]
        latest_records = [record.as_tuple() for record in new_latest if record]

        if len(records) != self._n_active_symbols:
            self.mode = "FAST"

        logger.info("Persisting records...")
        self._target.execute(SpotQueries.UPSERT.format(interval=self._interval), records)
        self._target.execute(SpotLatestQueries.UPSERT.format(interval=self._interval), latest_records)
        self._target.commit_transaction()

        self.check_trading_status()
        end = datetime.utcnow()
        logger.info(
            f"Persisted klines ({len(records)})"
            f" for {self._n_active_symbols} symbols in {end - start}."
        )

    def get_keys(self, symbol_lst: List[str]) -> List[Tuple[str, int]]:
        """Get (symbol, timestamp) combinations to request."""
        latest = self._target.get_latest(self._schema, self._interval)
        keys = []
        if latest:
            for k in latest:
                if k[2] is True:
                    keys.append(
                        (
                            k[0],
                            date_helpers.get_next_interval(
                                self._interval,
                                date_helpers.datetime_to_binance_timestamp(k[1]),
                            ),
                        )
                    )
            new_symbols = list(set(symbol_lst) - set(k[0] for k in latest))
        else:
            new_symbols = symbol_lst
        if new_symbols:
            logger.info("Fetching earliest timestamps for new symbols...")
            for s in new_symbols:
                earliest_ts = self._source.get_earliest_valid_timestamp(s)
                if earliest_ts:
                    keys.append((s, earliest_ts))
                self.check_request_limit()

        self._n_active_symbols = len(keys)
        return keys

    def latest_closed(self, symbol: str, record_objs: List[Kline]) -> Optional[Latest]:
        """Build Latest object from record objects."""
        res = None
        active = True
        if len(record_objs) > 1:
            last_closed_kline = record_objs[-2]
            res = Latest.build_record(
                [
                    symbol,
                    last_closed_kline.id,
                    last_closed_kline.open_time,
                    active,
                    self.source_name,
                ]
            )
        else:
            active = date_helpers.check_active(self._interval, record_objs[0].open_time)
            if not active:
                last_kline = record_objs[0]
                res = Latest.build_record(
                    [
                        symbol,
                        last_kline.id,
                        last_kline.open_time,
                        active,
                        self.source_name,
                    ]
                )
        return res

    def check_trading_status(self) -> None:
        """Check if inactive pairs are trading again."""
        logger.info("Checking inactive symbols...")
        inactive_symbols = self._target.get_inactive_symbols(self._schema, self._interval)
        trading_status = self._source.get_trading_status(inactive_symbols)
        self.check_request_limit()
        if trading_status:
            active_symbols = [(s[0],True) for s in trading_status if s[1] == "TRADING"]
            if active_symbols:
                self._target.execute(
                    SpotLatestQueries.CORRECT_TRADING_STATUS.format(interval=self._interval),
                    active_symbols,
                )
                self._target.commit_transaction()
                for symbol in active_symbols:
                    logger.info(f"Reinstated {symbol}.")

    def run_as_service(self) -> None:
        """Run process continuously."""
        # ON THE FIRST RUN IT GETS SYMBOLS ACCORDING TO FILTERS
        # AFTER THAT IT ONLY UPDATE THOSE SYMBOLS
        logger.info("Fetching symbols...")
        symbol_list = self._source.get_symbols(self._quote_symbols, self._base_symbols)
        if not symbol_list:
            logger.warning("No symbols found. Stopping.")
            return None
        logger.info("Running...")
        break_process = False
        while not break_process:
            try:
                self.run_once(symbol_list)
                t = None
                if self.mode == "FAST":
                    t = secrets.choice([1, 5] + [i for i in range(1, 5)])
                    logger.info(f"Waiting {timedelta(seconds=t)}... ({self.mode})")
                elif self.mode == "SLOW":
                    interval_sec = int(
                        (date_helpers.interval_to_milliseconds(self._interval) / 1000)
                        / 4
                    )
                    t = secrets.choice(
                        [interval_sec, interval_sec + 10]
                        + [i for i in range(interval_sec, interval_sec + 10)]
                    )
                    logger.info(f"Waiting {timedelta(seconds=t)}... ({self.mode})")
                if t:
                    time.sleep(t)
                else:
                    logger.warning("No waiting mode selected.")
                    return
            except Exception as e:
                logger.warning("Error while importing:", e)
                break_process = True

        logger.info("Terminating...")

    def run(self, args: argparse.Namespace) -> None:
        """Run process."""
        logger.info("Starting process...")
        self.setup(args=args)

        if args.run_as_service:
            self.run_as_service()
        else:
            symbol_list = self._source.get_symbols(self._quote_symbols, self._base_symbols)
            if symbol_list:
                self.run_once(symbol_list)


def parse_args() -> argparse.Namespace:
    """Parses user input arguments when starting loading process."""
    parser = argparse.ArgumentParser(prog="python ./src/binance_spot_loader/__main__.py")

    parser.add_argument(
        "--run_as_service",
        dest="run_as_service",
        type=str,
        required=False,
        default=os.environ.get("RUN_AS_SERVICE"),
        help="Enable continuous running.",
    )

    parser.add_argument(
        "--target",
        dest="target",
        type=str,
        required=False,
        default=os.environ.get("TARGET"),
        help="Postgres connection URL. e.g.: "
        "user=username password=password "
        "host=localhost port=5432 dbname=binance",
    )

    parser.add_argument(
        "--interval",
        dest="interval",
        type=str,
        required=False,
        default=os.environ.get("INTERVAL", default="1h"),
        help="Postgres connection URL. e.g.: "
        "user=username password=password "
        "host=localhost port=5432 dbname=binance",
    )

    parser.add_argument(
        "--quote_symbols",
        dest="quote_symbols",
        type=str,
        required=False,
        default=os.environ.get("QUOTE_SYMBOLS"),
        help="Load symbols quoted in quote_symbols. e.g.: "
        "USDT,TUSD,BUSD,BNB,BTC,ETH",
    )

    parser.add_argument(
        "--base_symbols",
        dest="base_symbols",
        type=str,
        required=False,
        default=os.environ.get("BASE_SYMBOLS"),
        help="Load symbols based in base_symbols. e.g.: "
             "BTC,ETH,DOGE",
    )

    a = parser.parse_args()

    return a


if __name__ == "__main__":
    parsed_args = parse_args()

    loader = Loader()
    loader.run(parsed_args)
