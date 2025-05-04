"""Main."""

import argparse
from datetime import datetime, timedelta
import logging
import os
import secrets
from sys import stdout
import time
from typing import Dict, List, Optional, Tuple

import alpaca_spot_loader.date_helpers as date_helpers
from alpaca_spot_loader.model import Latest
from alpaca_spot_loader.persistance import source, target
from alpaca_spot_loader.queries import SpotQueries, SpotLatestQueries
from alpaca_spot_loader.model.bar_record import BarRecord

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
    _n_active_symbols: int

    def __init__(self) -> None:
        self.source_name = "ALPACA"
        self.mode = "FAST"
        self.n_requests = 1
        self.schema = "alpaca"

    def setup(self, args: argparse.Namespace) -> None:
        """Set up loader and connections."""
        self._source = source.Source(args.source, args.interval)
        self._target = target.Target(args.target)
        self._interval = args.interval

        self._source.connect()
        self._target.connect()

    def check_request_limit(self) -> None:
        """Check if loader has made 200 requests."""
        self.n_requests += 1
        if self.n_requests >= 800:
            logger.info("Waiting 1m before requesting more...")
            time.sleep(60)
            self.n_requests = 1

    def run_once(self, symbol_lst: List[str]) -> None:
        """Run process once."""
        self.mode = "SLOW"
        start = datetime.utcnow()
        self.check_trading_status()
        keys = self.get_keys(symbol_lst)
        logger.info(f"Processing {self._n_active_symbols} symbols.")
        persisted_records = self.load_from_keys(keys)
        if persisted_records > self._n_active_symbols:
            self.mode = "FAST"
        end = datetime.utcnow()
        logger.info(
            f"Persisted records for {self._n_active_symbols} symbols in {end - start}."
        )

        

    def load_from_keys(self, keys: List[Tuple[str, datetime]]) -> int:
        logger.info(f"Processing {self._n_active_symbols} symbols.")  
        BATCH_SIZE = 1000
        record_objs: List[BarRecord] = []
        new_latest: List[Latest] = []
        processed_symbols = 0
        persisted_records = 0
        for symbol, start_time in keys:
            processed_symbols += 1
            logger.info(f"Processing {symbol} ({processed_symbols}/{self._n_active_symbols})...")
            self.check_request_limit()
            symbol_record_objs: List[BarRecord] = []
            # Get bars and check limits
            bars = self._source.get_bars(
                symbol=symbol, 
                interval=self._interval, 
                start_time=start_time
            )
            
            if not bars:
                logger.warning(f"No bars available for symbol {symbol} starting at {start_time}.")
            else:        
                # Process bars without temporary list
                record_ids = self._target.get_next_ids(self.schema, self._interval, len(bars))
                for bar, record_id in zip(bars, record_ids):
                    symbol_record_objs.append(BarRecord.build_record(record_id, bar))
                new_latest.append(self.latest_closed(symbol, symbol_record_objs))
                record_objs.extend(symbol_record_objs)
                
            # Batch persistence
            if record_objs and (processed_symbols % BATCH_SIZE == 0 or processed_symbols == self._n_active_symbols):
                logger.info(f"Persisting {len(record_objs)} records...")
                self.persist_records(
                    records=[r.as_tuple() for r in record_objs],
                    latest_records=[r.as_tuple() for r in new_latest if r],
                )
                persisted_records += len(record_objs)
                record_objs.clear()
                new_latest.clear()
                logger.info(f"Persisted batch up to symbol {processed_symbols}")
        
        return persisted_records
            

    def persist_records(self, records: List[Tuple], latest_records: List[Tuple]) -> None:
        self._target.execute(SpotQueries.UPSERT.format(interval=self._interval), records)
        self._target.execute(SpotLatestQueries.UPSERT.format(interval=self._interval), latest_records)
        self._target.commit_transaction()


    def get_keys(self, symbol_lst: List[str]) -> List[Tuple[str, datetime]]:
        """Get (symbol, timestamp) combinations to request."""
        latest = self._target.get_latest(self.schema, self._interval)

        keys = []
        if latest:
            for symbol, latest_close, active in latest:
                if active:
                    keys.append(
                        (
                            symbol,
                            date_helpers.get_next_interval(
                                self._interval,
                                latest_close
                            ),
                        )
                    )
            new_symbols = list(set(symbol_lst) - set(symbol for symbol,_,_ in latest))
        else:
            new_symbols = symbol_lst
        if new_symbols:
            logger.info("Fetching earliest timestamps for new symbols...")
            for s in new_symbols:
                keys.append((s, datetime.min + timedelta(days=1))) # Earliest datetime will be replaced by earliest bar by the api 

        self._n_active_symbols = len(keys)
        return keys

    def latest_closed(self, symbol: str, record_objs: List[BarRecord]) -> Optional[Latest]:
        """Build Latest object from record objects."""
        res = None
        active = True
        if len(record_objs) > 1:
            last_closed_bar = record_objs[-2]
            res = Latest.build_record(
                [
                    symbol,
                    last_closed_bar.id,
                    last_closed_bar.open_time,
                    active,
                    self.source_name,
                ]
            )
        else:
            active = date_helpers.check_active(self._interval, record_objs[0].open_time)
            if not active:
                last_bar = record_objs[0]
                res = Latest.build_record(
                    [
                        symbol,
                        last_bar.id,
                        last_bar.open_time,
                        active,
                        self.source_name,
                    ]
                )
        return res

    def check_trading_status(self) -> None:
        """Check if inactive pairs are trading again."""
        logger.info("Checking inactive symbols...")
        inactive_symbols = self._target.get_inactive_symbols(self.schema, self._interval)
        trading_status = self._source.get_trading_status(inactive_symbols)
        self.check_request_limit()
        if trading_status:
            active_symbols = [(symbol, active) for symbol, active in trading_status if active]
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
        symbol_list = self._source.get_symbols()
        if not symbol_list:
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
                        date_helpers.interval_to_seconds(self._interval) 
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

        if args.as_service:
            logger.info("Running as service...")
            self.run_as_service()
        else:
            logger.info("Running once...")
            symbol_list = self._source.get_symbols()
            if symbol_list:
                self.run_once(symbol_list)


def parse_args() -> argparse.Namespace:
    """Parses user input arguments when starting loading process."""
    parser = argparse.ArgumentParser(prog="python ./src/binace_spot_loader/__main__.py")

    parser.add_argument(
        "--as_service",
        action='store_true',
        help="Enable continuous running.",
    )

    parser.add_argument(
        "--source",
        dest="source",
        type=str,
        required=False,
        default=os.environ.get("SOURCE"),
        help="API Credentials.",
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
        help="Candlestick interval. e.g.: 1h, 1d"
    )

    a = parser.parse_args()

    return a


if __name__ == "__main__":
    parsed_args = parse_args()
    loader = Loader()
    loader.run(parsed_args)
