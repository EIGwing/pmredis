#!/usr/bin/env python3
"""
PMRedis - Polymarket and Binance Data Collector to Redis

Collects real-time data from:
- Polymarket 5-minute BTC up/down markets
- Binance BTC/USDT spot market
- Chainlink BTC/USD price feeds

Stores data in Redis streams with 10-minute rolling window.
"""

import argparse
import logging
import os
import signal
import sys
import time
from typing import List, Optional

from pmredis.collectors import (
    BinanceCollector,
    PolymarketCollector,
)
from pmredis.storage.redis_manager import RedisManager, get_stream_key

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


DEFAULT_TABLES = [
    "polymarket_prices",
    "polymarket_stream",
    "polymarket_orderbook",
    "polymarket_orderbook_next",
    "chainlink_prices",
    "binance_orderbook",
    "binance_impact_prices",
    "binance_cumulative_sizes",
    "binance_trades",
]


class DataCollectorRedis:
    """Main data collector that writes to Redis streams."""

    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        tables: Optional[List[str]] = None,
        sample_interval: float = 0.05,
    ):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.tables = tables or DEFAULT_TABLES
        self.sample_interval = sample_interval

        self.running = False

        # Redis manager
        self.redis = RedisManager(host=redis_host, port=redis_port)

        # Collectors
        self.pm_collector = PolymarketCollector()
        self.binance_collector = BinanceCollector()

        # Data state
        self._pm_ws_lock = __import__("threading").Lock()
        self._ws_lock = __import__("threading").Lock()

        # Polymarket state
        self._pm_yes_price: Optional[float] = None
        self._pm_no_price: Optional[float] = None
        self._pm_yes_orderbook: dict = {}
        self._pm_no_orderbook: dict = {}
        self._pm_session_price: Optional[float] = None
        self._pm_session_start_ts: Optional[int] = None
        self._pm_current_epoch: int = 0
        self._pm_ws_timestamp: Optional[int] = None
        self._asset_ids: List[str] = []
        self._next_asset_ids: List[str] = []

        # Next session state
        self._next_pm_yes_orderbook: dict = {}
        self._next_pm_no_orderbook: dict = {}
        self._next_session_epoch: Optional[int] = None

        # Binance state
        self.binance_data: dict = {}
        self._bn_timestamp: Optional[int] = None

        # Session tracking
        self._current_session_epoch: int = 0

    def run(self):
        """Run the data collector."""
        # Connect to Redis
        if not self.redis.connect():
            logger.error("Failed to connect to Redis")
            return

        logger.info(
            f"Starting data collection to Redis at {self.redis_host}:{self.redis_port}"
        )
        logger.info(f"Tables: {self.tables}")
        logger.info(f"Sample interval: {self.sample_interval}s")

        self.running = True

        # Setup signal handlers
        def signal_handler(sig, frame):
            logger.info("Shutting down...")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Main loop
        loop_count = 0
        last_flush = time.time()

        while self.running:
            try:
                # Update session epoch
                self._update_session_epoch()

                # Sample data
                self._sample_data(loop_count)
                loop_count += 1

                # Trim streams periodically
                if time.time() - last_flush >= 60:
                    self._trim_streams()
                    last_flush = time.time()

                time.sleep(self.sample_interval)

            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(1)

        self.redis.close()
        logger.info("Data collection stopped")

    def _update_session_epoch(self):
        """Update current 5-minute session epoch."""
        current_epoch = int(time.time()) - (int(time.time()) % 300)
        if current_epoch != self._current_session_epoch:
            logger.info(
                f"Session switch: {self._current_session_epoch} -> {current_epoch}"
            )
            self._current_session_epoch = current_epoch

            # Move next session data to current
            with self._pm_ws_lock:
                if self._next_pm_yes_orderbook or self._next_pm_no_orderbook:
                    self._pm_yes_price = getattr(self, "_next_pm_yes_price", None)
                    self._pm_no_price = getattr(self, "_next_pm_no_price", None)
                    self._pm_yes_orderbook = (
                        dict(self._next_pm_yes_orderbook)
                        if self._next_pm_yes_orderbook
                        else {}
                    )
                    self._pm_no_orderbook = (
                        dict(self._next_pm_no_orderbook)
                        if self._next_pm_no_orderbook
                        else {}
                    )
                    self._next_pm_yes_orderbook = {}
                    self._next_pm_no_orderbook = {}

            # Move next asset IDs to current
            if self._next_asset_ids:
                self._asset_ids = list(self._next_asset_ids)
                self._next_asset_ids = []

    def _sample_data(self, loop_count: int):
        """Sample data based on loop count for different frequencies."""
        import time as time_module
        from datetime import datetime as dt

        ts = dt.utcnow()
        ts_ms = int(ts.timestamp() * 1000)
        session_epoch = int(time_module.time()) - (int(time_module.time()) % 300)

        # polymarket_prices - 2Hz (every 10th loop at 20Hz)
        if "polymarket_prices" in self.tables and loop_count % 10 == 0:
            self._sample_polymarket_prices(ts, ts_ms, session_epoch)

        # polymarket_orderbook - 4Hz (every 5th loop)
        if "polymarket_orderbook" in self.tables and loop_count % 5 == 0:
            self._sample_polymarket_orderbook(
                ts, ts_ms, session_epoch, "polymarket_orderbook"
            )

        # polymarket_orderbook_next - 2Hz (every 10th loop)
        if "polymarket_orderbook_next" in self.tables and loop_count % 10 == 0:
            self._sample_polymarket_orderbook(
                ts, ts_ms, session_epoch, "polymarket_orderbook_next"
            )

        # polymarket_stream - 2Hz (every 10th loop)
        if "polymarket_stream" in self.tables and loop_count % 10 == 0:
            pass  # TODO: Implement

        # chainlink_prices - 1Hz (every 20th loop)
        if "chainlink_prices" in self.tables and loop_count % 20 == 0:
            pass  # TODO: Implement

        # binance_orderbook - 4Hz (every 5th loop)
        if "binance_orderbook" in self.tables and loop_count % 5 == 0:
            pass  # TODO: Implement

        # binance_impact_prices - 2Hz (every 10th loop)
        if "binance_impact_prices" in self.tables and loop_count % 10 == 0:
            pass  # TODO: Implement

        # binance_cumulative_sizes - 2Hz (every 10th loop)
        if "binance_cumulative_sizes" in self.tables and loop_count % 10 == 0:
            pass  # TODO: Implement

        # binance_trades - 4Hz (every 5th loop)
        if "binance_trades" in self.tables and loop_count % 5 == 0:
            pass  # TODO: Implement

    def _sample_polymarket_prices(self, ts, ts_ms: int, session_epoch: int):
        """Sample Polymarket prices."""
        with self._pm_ws_lock:
            yes_price = self._pm_yes_price
            no_price = self._pm_no_price
            session_price = self._pm_session_price
            current_epoch = self._pm_current_epoch
            source_ts = self._pm_ws_timestamp or ts_ms

        if yes_price is not None:
            direction = (
                1
                if yes_price > (session_price or 0)
                else (2 if yes_price < (session_price or 0) else 0)
            )
            change = (yes_price - session_price) if session_price else 0.0

            record = {
                "timestamp": str(ts_ms),
                "source_timestamp": str(source_ts),
                "session_epoch": str(current_epoch * 1000),
                "yes_price": str(yes_price),
                "no_price": str(no_price),
                "direction": str(direction),
                "change_from_session": str(change),
            }

            stream_key = get_stream_key("polymarket_prices")
            self.redis.xadd(stream_key, record)

    def _sample_polymarket_orderbook(
        self, ts, ts_ms: int, session_epoch: int, table_name: str
    ):
        """Sample Polymarket orderbook with flattened schema."""
        with self._pm_ws_lock:
            if table_name == "polymarket_orderbook":
                yes_ob = dict(self._pm_yes_orderbook) if self._pm_yes_orderbook else {}
                no_ob = dict(self._pm_no_orderbook) if self._pm_no_orderbook else {}
                epoch = session_epoch
                source_ts = self._pm_ws_timestamp or ts_ms
            else:
                yes_ob = (
                    dict(self._next_pm_yes_orderbook)
                    if self._next_pm_yes_orderbook
                    else {}
                )
                no_ob = (
                    dict(self._next_pm_no_orderbook)
                    if self._next_pm_no_orderbook
                    else {}
                )
                epoch = session_epoch
                source_ts = ts_ms

        record = {
            "timestamp": str(ts_ms),
            "source_timestamp": str(source_ts),
            "session_epoch": str(epoch * 1000),
        }

        # Flatten YES orderbook (8 levels)
        yes_bids = yes_ob.get("bids", [])
        yes_asks = yes_ob.get("asks", [])

        for level in range(1, 9):
            price = 0.0
            size = 0.0
            if level - 1 < len(yes_bids):
                bid = yes_bids[level - 1]
                price = float(bid.get("price", 0) or 0)
                size = float(bid.get("size", 0) or 0)
            record[f"yes_bid_{level}_price"] = str(price)
            record[f"yes_bid_{level}_size"] = str(size)
            record[f"yes_bid_{level}_dv"] = str(price * size)

        for level in range(1, 9):
            price = 0.0
            size = 0.0
            if level - 1 < len(yes_asks):
                ask = yes_asks[level - 1]
                price = float(ask.get("price", 0) or 0)
                size = float(ask.get("size", 0) or 0)
            record[f"yes_ask_{level}_price"] = str(price)
            record[f"yes_ask_{level}_size"] = str(size)
            record[f"yes_ask_{level}_dv"] = str(price * size)

        # Flatten NO orderbook (8 levels)
        no_bids = no_ob.get("bids", [])
        no_asks = no_ob.get("asks", [])

        for level in range(1, 9):
            price = 0.0
            size = 0.0
            if level - 1 < len(no_bids):
                bid = no_bids[level - 1]
                price = float(bid.get("price", 0) or 0)
                size = float(bid.get("size", 0) or 0)
            record[f"no_bid_{level}_price"] = str(price)
            record[f"no_bid_{level}_size"] = str(size)
            record[f"no_bid_{level}_dv"] = str(price * size)

        for level in range(1, 9):
            price = 0.0
            size = 0.0
            if level - 1 < len(no_asks):
                ask = no_asks[level - 1]
                price = float(ask.get("price", 0) or 0)
                size = float(ask.get("size", 0) or 0)
            record[f"no_ask_{level}_price"] = str(price)
            record[f"no_ask_{level}_size"] = str(size)
            record[f"no_ask_{level}_dv"] = str(price * size)

        stream_key = get_stream_key(table_name)
        self.redis.xadd(stream_key, record)

    def _trim_streams(self):
        """Trim all streams to maintain 10-minute window."""
        for table in self.tables:
            stream_key = get_stream_key(table)
            self.redis.xtrim(stream_key)


def main():
    parser = argparse.ArgumentParser(
        description="PMRedis - Collect market data to Redis streams"
    )
    parser.add_argument(
        "command",
        nargs="?",
        default="start",
        help="Command (start, help)",
    )
    parser.add_argument(
        "--redis-host",
        default="localhost",
        help="Redis host (default: localhost)",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="Redis port (default: 6379)",
    )
    parser.add_argument(
        "--tables",
        help="Comma-separated list of tables (default: all)",
    )
    parser.add_argument(
        "--sample-interval",
        type=float,
        default=0.05,
        help="Sample interval in seconds (default: 0.05)",
    )

    args = parser.parse_args(sys.argv[1:])

    if args.command == "help":
        print("""
PMRedis - Polymarket and Binance Data Collector to Redis

Usage:
    python main.py start [options]

Options:
    --redis-host HOST      Redis host (default: localhost)
    --redis-port PORT     Redis port (default: 6379)
    --tables TABLES       Comma-separated tables (default: all)
    --sample-interval SEC Sample interval in seconds (default: 0.05)

Tables:
    polymarket_prices, polymarket_stream, polymarket_orderbook,
    polymarket_orderbook_next, chainlink_prices, binance_orderbook,
    binance_impact_prices, binance_cumulative_sizes, binance_trades

Example:
    python main.py start --redis-host localhost --redis-port 6379
        """)
        return

    tables = None
    if args.tables:
        tables = [t.strip() for t in args.tables.split(",")]

    collector = DataCollectorRedis(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        tables=tables,
        sample_interval=args.sample_interval,
    )
    collector.run()


if __name__ == "__main__":
    main()
