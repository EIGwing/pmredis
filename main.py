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
import asyncio
import datetime
import logging
import os
import signal
import sys
import threading
import time
from typing import Any, Dict, List, Optional

from pmredis.collectors import (
    BinanceCollector,
    ChainlinkCollector,
    PolymarketCollector,
)
from pmredis.collectors.binance import BinanceDepthWSManager, BinanceTradesWSManager
from pmredis.collectors.polymarket import PolymarketRTDS, PolymarketCLOB
from pmredis.collectors.chainlink_stream import ChainlinkStreamScraper
from pmredis.storage.redis_manager import RedisManager, get_stream_key
from pmredis.storage.parquet_exporter import ParquetExporter

TRACE = 5
logging.addLevelName(TRACE, "TRACE")

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
        sample_interval: float = 0.2,
        dry_run: bool = False,
        parquet_dir: str = "./data",
    ):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.tables = tables or DEFAULT_TABLES
        self.sample_interval = sample_interval
        self.dry_run = dry_run
        self.parquet_dir = parquet_dir

        self.running = False

        # Redis manager
        self.redis = RedisManager(host=redis_host, port=redis_port, dry_run=dry_run)

        # Parquet exporter
        self.parquet_exporter = ParquetExporter(output_dir=parquet_dir)
        self._last_export_epoch: int = -1

        # Collectors
        self.pm_collector = PolymarketCollector()
        self.binance_collector = BinanceCollector()
        self.chainlink_collector = ChainlinkCollector()

        # Chainlink stream scraper
        self._chainlink_scraper_thread: Optional[threading.Thread] = None
        self._chainlink_mid_price: Optional[float] = None
        self._chainlink_bid_price: Optional[float] = None
        self._chainlink_ask_price: Optional[float] = None
        self._chainlink_timestamp: Optional[str] = None

        # Binance orderbook and trades
        self._binance_orderbook: BinanceDepthWSManager = None
        self._binance_trades: BinanceTradesWSManager = None

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

        # Polymarket market tracking
        self._pm_current_market: Optional[dict] = None
        self._next_pm_current_market: Optional[dict] = None
        self._pm_last_market_fetch: float = 0
        self._pm_last_orderbook_fetch: float = 0
        self._pm_market_fetch_interval: float = 30.0
        self._pm_orderbook_fetch_interval: float = 0.25

        # Staleness tracking
        self._pm_orderbook_last_update: float = 0
        self._pm_stream_last_update: float = 0
        self._pm_staleness_threshold: float = 15.0  # seconds

        # Polymarket WebSocket collectors
        self._pm_rtdws: Optional[PolymarketRTDS] = None
        self._pm_clob_ws: Optional[PolymarketCLOB] = None
        self._pm_rtdws_price: Optional[float] = None
        self._pm_btc_price: Optional[float] = None

        # Binance state
        self.binance_data: dict = {}
        self._bn_timestamp: Optional[int] = None
        self._binance_orderbook_bids: List[List] = []
        self._binance_orderbook_asks: List[List] = []
        self._binance_trades_list: List[Dict] = []
        self._binance_last_fetch: float = 0

        # Chainlink state
        self._chainlink_price: Optional[float] = None
        self._chainlink_last_fetch: float = 0

        # Polymarket stream state
        self._pm_stream_thread: Optional[threading.Thread] = None
        self._pm_stream_scraper = None  # Reference to scraper for recovery
        self._pm_stream_price: Optional[float] = None
        self._pm_stream_price_to_beat: Optional[float] = None
        self._pm_stream_timestamp: Optional[str] = None
        self._pm_stream_last_chart_timestamp: Optional[int] = (
            None  # Last chart timestamp in ms
        )
        self._pm_stream_last_price: Optional[float] = None  # Last price value
        self._pm_stream_tooltip_url: str = ""  # Current tooltip page URL
        self._pm_stream_no_change_threshold: float = 30.0  # seconds
        self._pm_stream_recovery_in_progress = False

        # Session tracking
        self._current_session_epoch: int = 0

    def _initialize_collectors(self):
        """Initialize collectors based on configured tables."""
        # Start Binance orderbook collector
        if (
            "binance_orderbook" in self.tables
            or "binance_cumulative_sizes" in self.tables
        ):
            try:
                self._binance_orderbook = BinanceDepthWSManager(symbol="BTCUSDT")
                if self._binance_orderbook.start():
                    logger.info("Binance orderbook collector started")
                else:
                    logger.warning("Failed to start Binance orderbook collector")
            except Exception as e:
                logger.error(f"Error initializing Binance orderbook: {e}")

        # Start Binance trades collector
        if "binance_trades" in self.tables:
            try:
                self._binance_trades = BinanceTradesWSManager(symbol="BTCUSDT")
                if self._binance_trades.start():
                    logger.info("Binance trades collector started")
                else:
                    logger.warning("Failed to start Binance trades collector")
            except Exception as e:
                logger.error(f"Error initializing Binance trades: {e}")

        # Start Chainlink stream scraper
        if "chainlink_prices" in self.tables:
            self._start_chainlink_stream()

        # Start Polymarket RTDS WebSocket for prices
        if "polymarket_prices" in self.tables:
            try:
                self._pm_rtdws = PolymarketRTDS()
                self._pm_rtdws.add_callback(self._on_pm_price_update)
                self._pm_rtdws.start()
                logger.info("Polymarket RTDS WebSocket started")
            except Exception as e:
                logger.error(f"Error initializing Polymarket RTDS: {e}")

        # Start Polymarket CLOB WebSocket for orderbook
        if (
            "polymarket_orderbook" in self.tables
            or "polymarket_orderbook_next" in self.tables
        ):
            try:
                self._pm_clob_ws = PolymarketCLOB()
                self._pm_clob_ws.start()
                logger.info("Polymarket CLOB WebSocket started")
            except Exception as e:
                logger.error(f"Error initializing Polymarket CLOB: {e}")

        # Start Polymarket stream scraper in a separate thread
        if "polymarket_stream" in self.tables:
            self._start_polymarket_stream()

    def _on_pm_price_update(self, asset: str, price_data: Dict[str, Any]):
        """Callback for Polymarket RTDS price updates."""
        if asset == "BTC-USD":
            self._pm_btc_price = price_data.get("price")
            logger.debug(f"RTDS BTC price: {self._pm_btc_price}")

    def _start_polymarket_stream(self):
        """Start Polymarket stream scraper in a separate thread."""
        try:
            from pmredis.collectors.polymarket_stream import PolymarketStreamScraper

            def on_price_update(price, price_to_beat, timestamp, tooltip_url):
                """Callback for price updates from polymarket_stream."""
                # Parse chart timestamp to epoch ms for staleness detection
                chart_ts_utc = 0
                if timestamp:
                    current_epoch = int(time.time()) - (int(time.time()) % 300)
                    chart_ts_utc = self._parse_chart_timestamp(timestamp, current_epoch)

                with self._pm_ws_lock:
                    self._pm_stream_price = price
                    self._pm_stream_price_to_beat = price_to_beat
                    self._pm_stream_timestamp = timestamp
                    self._pm_stream_tooltip_url = tooltip_url
                    self._pm_stream_last_update = time.time()
                    # Store for staleness detection
                    if chart_ts_utc:
                        self._pm_stream_last_chart_timestamp = chart_ts_utc
                    if price:
                        self._pm_stream_last_price = price
                logger.log(
                    TRACE,
                    f"Price update: {price} | ptb: {price_to_beat} | url: {tooltip_url}",
                )

            def run_scraper():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                scraper = PolymarketStreamScraper(check_interval_ms=250)

                # Store scraper reference for recovery trigger
                self._pm_stream_scraper = scraper

                scraper.add_callback(on_price_update)

                async def monitor():
                    try:
                        await scraper.monitor()
                    except Exception as e:
                        logger.error(f"Polymarket stream error: {e}")
                        await scraper.close()
                        try:
                            await asyncio.sleep(5)
                            await scraper.monitor()
                        except Exception as e2:
                            logger.error(f"Polymarket stream recovery failed: {e2}")

                try:
                    loop.run_until_complete(monitor())
                except KeyboardInterrupt:
                    pass
                finally:
                    loop.run_until_complete(scraper.close())
                    loop.close()

            self._pm_stream_thread = threading.Thread(target=run_scraper, daemon=True)
            self._pm_stream_thread.start()
            logger.info("Polymarket stream collector started")

        except Exception as e:
            logger.error(f"Error starting Polymarket stream: {e}")

    def _start_chainlink_stream(self):
        """Start Chainlink stream scraper in a separate thread."""
        try:
            from pmredis.collectors.chainlink_stream import ChainlinkStreamScraper

            def on_price_update(mid, bid, ask, timestamp):
                """Callback for price updates from chainlink_stream."""
                with self._ws_lock:
                    self._chainlink_mid_price = mid
                    self._chainlink_bid_price = bid
                    self._chainlink_ask_price = ask
                    self._chainlink_timestamp = timestamp
                logger.log(TRACE, f"Chainlink price: mid={mid}, bid={bid}, ask={ask}")

            def run_scraper():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                scraper = ChainlinkStreamScraper(check_interval_ms=1000)
                scraper.add_callback(on_price_update)

                async def monitor():
                    try:
                        await scraper.monitor()
                    except Exception as e:
                        logger.error(f"Chainlink stream error: {e}")
                        await scraper.close()
                        try:
                            await asyncio.sleep(5)
                            await scraper.monitor()
                        except Exception as e2:
                            logger.error(f"Chainlink stream recovery failed: {e2}")

                try:
                    loop.run_until_complete(monitor())
                except KeyboardInterrupt:
                    pass
                finally:
                    loop.run_until_complete(scraper.close())
                    loop.close()

            self._chainlink_scraper_thread = threading.Thread(
                target=run_scraper, daemon=True
            )
            self._chainlink_scraper_thread.start()
            logger.info("Chainlink stream collector started")

        except Exception as e:
            logger.error(f"Error starting Chainlink stream: {e}")

    def _stop_collectors(self):
        """Stop all collectors gracefully."""
        if self._binance_orderbook:
            try:
                self._binance_orderbook.stop()
                logger.info("Binance orderbook collector stopped")
            except Exception as e:
                logger.error(f"Error stopping Binance orderbook: {e}")

        if self._binance_trades:
            try:
                self._binance_trades.stop()
                logger.info("Binance trades collector stopped")
            except Exception as e:
                logger.error(f"Error stopping Binance trades: {e}")

        if self._pm_rtdws:
            try:
                self._pm_rtdws.stop()
                logger.info("Polymarket RTDS WebSocket stopped")
            except Exception as e:
                logger.error(f"Error stopping Polymarket RTDS: {e}")

        if self._pm_clob_ws:
            try:
                self._pm_clob_ws.stop()
                logger.info("Polymarket CLOB WebSocket stopped")
            except Exception as e:
                logger.error(f"Error stopping Polymarket CLOB: {e}")

    def run(self):
        """Run the data collector."""
        # Connect to Redis (skip in dry-run mode)
        if not self.dry_run:
            max_retries = 10
            retry_delay = 1.0
            for attempt in range(max_retries):
                if self.redis.connect():
                    break
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Redis connection attempt {attempt + 1} failed, "
                        f"retrying in {retry_delay:.1f}s..."
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2
            else:
                logger.error(f"Failed to connect to Redis after {max_retries} attempts")
                return

        logger.info(
            f"Starting data collection to Redis at {self.redis_host}:{self.redis_port}"
        )
        logger.info(f"Tables: {self.tables}")
        logger.info(f"Sample interval: {self.sample_interval}s")

        self.running = True

        # Start background trimming thread (skip in dry-run mode)
        if not self.dry_run:
            self._start_trimming_thread(interval_seconds=60, window_seconds=2700)

        # Initialize collectors based on tables
        self._initialize_collectors()

        # Setup signal handlers
        def signal_handler(sig, frame):
            logger.info("Shutting down...")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Main loop
        loop_count = 0
        last_flush = time.time()
        last_market_fetch = 0

        while self.running:
            try:
                # Update session epoch
                self._update_session_epoch()

                # Fetch Polymarket market info and orderbook
                current_time = time.time()
                if current_time - last_market_fetch >= self._pm_market_fetch_interval:
                    self._fetch_polymarket_markets()
                    last_market_fetch = current_time

                # Fetch orderbook frequently
                if (
                    current_time - self._pm_last_orderbook_fetch
                    >= self._pm_orderbook_fetch_interval
                ):
                    self._fetch_polymarket_orderbooks()
                    self._pm_last_orderbook_fetch = current_time

                # Fetch Chainlink prices (every 1 second)
                if "chainlink_prices" in self.tables:
                    if current_time - self._chainlink_last_fetch >= 1.0:
                        self._fetch_chainlink_prices()
                        self._chainlink_last_fetch = current_time

                # Fetch Binance data (every 0.25 seconds)
                if (
                    "binance_orderbook" in self.tables
                    or "binance_trades" in self.tables
                ):
                    if current_time - self._binance_last_fetch >= 0.25:
                        self._fetch_binance_data()
                        self._binance_last_fetch = current_time

                # Sample data
                self._sample_data(loop_count)
                loop_count += 1

                # Export to parquet every minute
                current_time = time.localtime()
                current_minute = current_time.tm_min
                current_hour = current_time.tm_hour

                # Track minute (hour * 60 + minute) to handle hour boundary
                current_minute_key = current_hour * 60 + current_minute

                # Export at minutes 0, 10, 20, 30, 40, 50 (every 10 minutes)
                export_minutes = {0, 10, 20, 30, 40, 50}
                if (
                    not self.dry_run
                    and current_minute in export_minutes
                    and current_minute_key != self._last_export_epoch
                    and self._last_export_epoch > 0
                ):
                    # Export the previous 10-minute window
                    # At minute 10, export 00-09; at minute 20, export 10-19, etc.
                    minute_to_export = self._last_export_epoch
                    logger.info(
                        f"Exporting parquet for minute {minute_to_export} "
                        f"(current minute: {current_minute_key})"
                    )
                    results = self.parquet_exporter.export_all_tables(
                        self.redis, self.tables, minute_to_export
                    )
                    for table, success in results.items():
                        if not success:
                            logger.error(f"Failed to export {table}")

                self._last_export_epoch = current_minute_key

                time.sleep(self.sample_interval)

            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(1)

        # Stop collectors before closing
        self._stop_collectors()

        # Stop background trimming thread
        self._stop_trimming_thread()

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

            # FIXED: Instead of clearing all, just rotate subscriptions
            # 1. Unsubscribe old current (2 tokens that are now expired)
            old_asset_ids = list(self._asset_ids) if self._asset_ids else []
            if self._pm_clob_ws and old_asset_ids:
                self._pm_clob_ws.unsubscribe(old_asset_ids)

            # 2. Move next session data to current (orderbook data is ALREADY subscribed)
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

            # 3. Move next asset IDs to current (these are ALREADY subscribed!)
            if self._next_asset_ids:
                self._asset_ids = list(self._next_asset_ids)
                self._next_asset_ids = []

            # 4. Fetch new next market IMMEDIATELY to subscribe to it
            self._fetch_polymarket_markets()

    def _fetch_polymarket_markets(self):
        """Fetch current and upcoming Polymarket BTC markets."""
        try:
            markets = self.pm_collector.get_current_and_upcoming_markets(
                interval_minutes=5, hours_ahead=0.5
            )
            logger.debug(f"Fetched {len(markets)} markets from Polymarket")

            now = int(time.time())
            current_epoch = now - (now % 300)
            next_epoch = current_epoch + 300

            current_market = None
            next_market = None

            for market in markets:
                market_ts = market.get("timestamp")
                logger.debug(
                    f"Market timestamp: {market_ts}, current_epoch: {current_epoch}, next_epoch: {next_epoch}"
                )
                if market_ts == current_epoch:
                    current_market = market
                elif market_ts == next_epoch:
                    next_market = market

            if current_market:
                logger.info(f"Found current market: {current_market.get('slug')}")
                self._pm_current_market = current_market
                self._asset_ids = current_market.get("clobTokenIds", [])
                logger.info(f"Current market asset_ids: {self._asset_ids}")

                # Subscribe to CLOB WebSocket for orderbook
                if self._pm_clob_ws and self._asset_ids:
                    self._pm_clob_ws.subscribe(self._asset_ids)

            if next_market:
                logger.info(f"Found next market: {next_market.get('slug')}")
                self._next_pm_current_market = next_market
                self._next_asset_ids = next_market.get("clobTokenIds", [])
                logger.info(f"Next market asset_ids: {self._next_asset_ids}")

                # Subscribe to CLOB WebSocket for next market orderbook
                if self._pm_clob_ws and self._next_asset_ids:
                    self._pm_clob_ws.subscribe(self._next_asset_ids)

            # Cleanup: remove stale tokens that are no longer needed
            if self._pm_clob_ws and (self._asset_ids or self._next_asset_ids):
                subscribed = self._pm_clob_ws.get_all_orderbooks()
                subscribed_ids = set(subscribed.keys())
                needed_ids = set(self._asset_ids) | set(self._next_asset_ids)
                tokens_to_remove = subscribed_ids - needed_ids

                if tokens_to_remove:
                    logger.info(
                        f"CLOB cleaning up {len(tokens_to_remove)} stale tokens: {list(tokens_to_remove)[:3]}..."
                    )
                    self._pm_clob_ws.unsubscribe(list(tokens_to_remove))

            # Log subscription count
            if self._pm_clob_ws:
                sub_count = self._pm_clob_ws.get_subscription_count()
                logger.info(f"CLOB WebSocket subscriptions: {sub_count} (expected: 4)")

                # Debug: log raw orderbook cache
                all_obs = self._pm_clob_ws.get_all_orderbooks()
                if all_obs:
                    logger.info(f"CLOB orderbook cache: {list(all_obs.keys())}")
                else:
                    logger.warning("CLOB orderbook cache is empty!")

        except Exception as e:
            logger.error(f"Error fetching Polymarket markets: {e}")

    def _fetch_polymarket_orderbooks(self):
        """Fetch orderbooks for current and next Polymarket markets via WebSocket only."""

        # Get current market orderbook from WebSocket
        if self._pm_current_market and self._asset_ids and self._pm_clob_ws:
            try:
                yes_token_id = self._asset_ids[0] if len(self._asset_ids) > 0 else None
                no_token_id = self._asset_ids[1] if len(self._asset_ids) > 1 else None

                with self._pm_ws_lock:
                    if yes_token_id:
                        yes_ob = self._pm_clob_ws.get_orderbook(yes_token_id)
                        if yes_ob is not None:
                            self._pm_yes_orderbook = yes_ob
                            bids = yes_ob.get("bids", [])
                            asks = yes_ob.get("asks", [])
                            # Sort bids by price descending (highest first) to get best bid
                            sorted_bids = sorted(
                                bids,
                                key=lambda x: float(x.get("price", 0) or 0),
                                reverse=True,
                            )
                            logger.debug(
                                f"YES orderbook for token {yes_token_id}: bids={len(bids)}, asks={len(asks)}"
                            )
                            if sorted_bids:
                                self._pm_yes_price = float(
                                    sorted_bids[0].get("price", 0) or 0
                                )
                                logger.debug(
                                    f"YES sorted bids (first 3): {sorted_bids[:3]}"
                                )
                                # Set session price to 0.5 (even odds at session open)
                                if self._pm_session_price is None:
                                    self._pm_session_price = 0.5
                                    logger.info(
                                        f"Session price set to: {self._pm_session_price}"
                                    )
                            else:
                                self._pm_yes_price = 0.0
                            if asks:
                                logger.debug(f"YES token ask[0]: {asks[0]}")
                            logger.debug(
                                f"YES token bid[0]: {bids[0] if bids else 'none'}"
                            )
                        else:
                            logger.debug(
                                f"No orderbook data for YES token {yes_token_id}"
                            )

                    if no_token_id:
                        no_ob = self._pm_clob_ws.get_orderbook(no_token_id)
                        if no_ob is not None:
                            self._pm_no_orderbook = no_ob
                            bids = no_ob.get("bids", [])
                            asks = no_ob.get("asks", [])
                            # Sort bids by price descending (highest first) to get best bid
                            sorted_bids = sorted(
                                bids,
                                key=lambda x: float(x.get("price", 0) or 0),
                                reverse=True,
                            )
                            logger.debug(
                                f"NO orderbook for token {no_token_id}: bids={len(bids)}, asks={len(asks)}"
                            )
                            if sorted_bids:
                                self._pm_no_price = float(
                                    sorted_bids[0].get("price", 0) or 0
                                )
                                logger.debug(
                                    f"NO sorted bids (first 3): {sorted_bids[:3]}"
                                )
                            else:
                                self._pm_no_price = 0.0
                            if asks:
                                logger.debug(f"NO token ask[0]: {asks[0]}")
                        else:
                            logger.debug(
                                f"No orderbook data for NO token {no_token_id}"
                            )

            except Exception as e:
                logger.debug(f"Error fetching from CLOB WebSocket: {e}")

        # Get next market orderbook from WebSocket
        if self._next_pm_current_market and self._next_asset_ids and self._pm_clob_ws:
            try:
                yes_token_id = (
                    self._next_asset_ids[0] if len(self._next_asset_ids) > 0 else None
                )
                no_token_id = (
                    self._next_asset_ids[1] if len(self._next_asset_ids) > 1 else None
                )

                with self._pm_ws_lock:
                    if yes_token_id:
                        yes_ob = self._pm_clob_ws.get_orderbook(yes_token_id)
                        if yes_ob is not None:
                            self._next_pm_yes_orderbook = yes_ob
                        else:
                            logger.debug(
                                f"No orderbook data for next YES token {yes_token_id}"
                            )

                    if no_token_id:
                        no_ob = self._pm_clob_ws.get_orderbook(no_token_id)
                        if no_ob is not None:
                            self._next_pm_no_orderbook = no_ob
                        else:
                            logger.debug(
                                f"No orderbook data for next NO token {no_token_id}"
                            )

            except Exception as e:
                logger.debug(f"Error fetching next orderbook from WebSocket: {e}")

        # Update staleness tracking
        if self._pm_clob_ws:
            last_update = self._pm_clob_ws.get_last_update_time()
            if last_update > 0:
                self._pm_orderbook_last_update = last_update

        # Check for stale orderbook data
        current_time = time.time()
        if self._pm_orderbook_last_update > 0:
            time_since_update = current_time - self._pm_orderbook_last_update
            if time_since_update > self._pm_staleness_threshold:
                logger.warning(
                    f"Orderbook data stale: {time_since_update:.1f}s since last update"
                )

    def _fetch_chainlink_prices(self):
        """Fetch Chainlink BTC/USD price from scraper."""
        with self._ws_lock:
            mid_price = self._chainlink_mid_price
            bid_price = self._chainlink_bid_price
            ask_price = self._chainlink_ask_price
            timestamp = self._chainlink_timestamp

        if mid_price is not None:
            self._chainlink_price = mid_price
            logger.log(
                TRACE,
                f"Chainlink price: mid={mid_price}, bid={bid_price}, ask={ask_price}",
            )
        else:
            logger.debug("Chainlink price fetch returned None")

    def _fetch_binance_data(self):
        """Fetch Binance orderbook and trades data."""
        # Fetch orderbook
        if self._binance_orderbook and self._binance_orderbook.is_initialized():
            try:
                bids, asks = self._binance_orderbook.get_orderbook()
                with self._ws_lock:
                    self._binance_orderbook_bids = list(bids)
                    self._binance_orderbook_asks = list(asks)
            except Exception as e:
                logger.debug(f"Error fetching Binance orderbook: {e}")

        # Fetch trades
        if self._binance_trades and self._binance_trades.is_initialized():
            try:
                # Get recent trades from the collector
                volumes = self._binance_trades.get_volumes()
                with self._ws_lock:
                    self.binance_data = volumes
            except Exception as e:
                logger.debug(f"Error fetching Binance trades: {e}")

    def _sample_data(self, loop_count: int):
        """Sample data based on loop count for different frequencies."""
        import time as time_module
        from datetime import datetime as dt
        from datetime import timezone

        ts = dt.now(timezone.utc)
        ts_ms = int(ts.timestamp() * 1000)
        session_epoch = int(time_module.time()) - (int(time_module.time()) % 300)

        # Sample every loop - sample_interval controls the rate directly
        if "polymarket_prices" in self.tables:
            self._sample_polymarket_prices(ts, ts_ms, session_epoch)

        if "polymarket_orderbook" in self.tables:
            self._sample_polymarket_orderbook(
                ts, ts_ms, session_epoch, "polymarket_orderbook"
            )

        if "polymarket_orderbook_next" in self.tables:
            self._sample_polymarket_orderbook(
                ts, ts_ms, session_epoch + 300, "polymarket_orderbook_next"
            )

        if "polymarket_stream" in self.tables:
            self._sample_polymarket_stream(ts, ts_ms, session_epoch)

        if "chainlink_prices" in self.tables:
            self._sample_chainlink_prices(ts, ts_ms, session_epoch)

        if "binance_orderbook" in self.tables:
            self._sample_binance_orderbook(ts, ts_ms, session_epoch)

        if "binance_impact_prices" in self.tables:
            self._sample_binance_impact_prices(ts, ts_ms, session_epoch)

        if "binance_cumulative_sizes" in self.tables:
            self._sample_binance_cumulative_sizes(ts, ts_ms, session_epoch)

        if "binance_trades" in self.tables:
            self._sample_binance_trades(ts, ts_ms, session_epoch)

    def _sample_polymarket_prices(self, ts, ts_ms: int, session_epoch: int):
        """Sample Polymarket prices from polymarket_stream."""
        # Get price from polymarket_stream scraper
        with self._pm_ws_lock:
            rtdws_price = self._pm_stream_price
            price_to_beat = self._pm_stream_price_to_beat

        # Get YES/NO prices from orderbook
        with self._pm_ws_lock:
            yes_price = self._pm_yes_price
            no_price = self._pm_no_price
            session_price = self._pm_session_price

        logger.debug(
            f"polymarket_prices values - stream: {rtdws_price}, ptb: {price_to_beat}, yes: {yes_price}, session: {session_price}"
        )

        # Check for stale polymarket_stream data
        current_time = time.time()
        if self._pm_stream_last_update > 0:
            time_since_update = current_time - self._pm_stream_last_update
            if time_since_update > self._pm_staleness_threshold:
                logger.warning(
                    f"polymarket_stream data stale: {time_since_update:.1f}s since last update"
                )

        # Use stream price if available, otherwise fall back to YES price
        price_to_use = rtdws_price if rtdws_price is not None else yes_price

        if price_to_use is not None and yes_price is not None:
            # Calculate NO price as 1 - YES price (since YES + NO = 1)
            no_price_calc = 1.0 - yes_price

            direction = (
                1
                if yes_price > (session_price or 0)
                else (2 if yes_price < (session_price or 0) else 0)
            )
            change = (yes_price - session_price) if session_price else 0.0

            record = {
                "timestamp": str(ts_ms),
                "source_timestamp": str(ts_ms),
                "session_epoch": str(session_epoch * 1000),
                "yes_price": str(yes_price) if yes_price else "",
                "no_price": str(no_price_calc) if no_price_calc is not None else "",
                "btc_price": str(rtdws_price) if rtdws_price else "",
                "price_to_beat": str(price_to_beat) if price_to_beat else "",
                "direction": str(direction),
                "change_from_session": str(change),
            }

            stream_key = get_stream_key("polymarket_prices")
            self.redis.xadd(stream_key, record)

            logger.debug(f"polymarket_prices: {record}")

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
        # Sort bids: highest price first (descending), asks: lowest price first (ascending)
        yes_bids = sorted(
            yes_ob.get("bids", []),
            key=lambda x: float(x.get("price", 0) or 0),
            reverse=True,
        )
        yes_asks = sorted(
            yes_ob.get("asks", []), key=lambda x: float(x.get("price", 0) or 0)
        )

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
        no_bids = sorted(
            no_ob.get("bids", []),
            key=lambda x: float(x.get("price", 0) or 0),
            reverse=True,
        )
        no_asks = sorted(
            no_ob.get("asks", []), key=lambda x: float(x.get("price", 0) or 0)
        )

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

    def _parse_chart_timestamp(self, chart_time: str, session_epoch: int) -> int:
        """Parse chart timestamp (local time) to UTC epoch milliseconds.

        Handles:
        - 12-hour format with AM/PM: "6:17:48 PM", "12:30:00 AM"
        - Polymarket midnight bug: "-1:05:30 AM" -> 00:05:30

        Args:
            chart_time: Local time string from chart
            session_epoch: Session epoch in seconds (used for date context)

        Returns:
            Epoch timestamp in milliseconds, or 0 on error
        """
        if not chart_time:
            return 0

        try:
            chart_time = chart_time.strip()
            is_pm = "PM" in chart_time.upper()
            is_am = "AM" in chart_time.upper()

            # Handle Polymarket midnight bug: "-1:MM:SS" -> "12:MM:SS"
            if "-1:" in chart_time:
                chart_time = chart_time.replace("-1:", "12:")

            chart_time_clean = (
                chart_time.upper().replace("AM", "").replace("PM", "").strip()
            )

            parts = chart_time_clean.split(":")
            if len(parts) != 3:
                return 0

            hour = int(parts[0])
            minute = int(parts[1])
            second = int(parts[2])

            if hour < 0:
                hour = 0
            elif is_pm and hour != 12:
                hour += 12
            elif is_am and hour == 12:
                hour = 0

            session_dt = datetime.datetime.utcfromtimestamp(session_epoch)

            # Parse as UTC - browser shows local time, convert to UTC
            utc_tz = datetime.timezone.utc
            utc_dt = datetime.datetime(
                session_dt.year,
                session_dt.month,
                session_dt.day,
                hour,
                minute,
                second,
                tzinfo=utc_tz,
            )

            return int(utc_dt.timestamp() * 1000)
        except (ValueError, AttributeError):
            return 0

    def _sample_polymarket_stream(self, ts, ts_ms: int, session_epoch: int):
        """Sample Polymarket stream data (from async scraper)."""
        current_time = time.time()

        with self._pm_ws_lock:
            price = self._pm_stream_price
            price_to_beat = self._pm_stream_price_to_beat
            timestamp = self._pm_stream_timestamp
            tooltip_url = self._pm_stream_tooltip_url

        if price is not None:
            chart_ts_utc = 0
            if timestamp:
                chart_ts_utc = self._parse_chart_timestamp(timestamp, session_epoch)

            # Staleness detection: check if chart_timestamp AND price not advancing
            if (
                self._pm_stream_last_chart_timestamp
                and self._pm_stream_last_price
                and chart_ts_utc == self._pm_stream_last_chart_timestamp
                and price == self._pm_stream_last_price
            ):
                time_since_no_change = current_time - self._pm_stream_last_update
                if time_since_no_change > self._pm_stream_no_change_threshold:
                    if not self._pm_stream_recovery_in_progress:
                        logger.warning(
                            f"No price/chart change for {time_since_no_change:.1f}s, triggering recovery"
                        )
                        self._pm_stream_recovery_in_progress = True
                        # Trigger reload in scraper thread
                        if self._pm_stream_scraper:
                            self._pm_stream_scraper.trigger_reload()

                        # Reset recovery flag after a delay (actual reset happens in scraper)
                        def reset_recovery():
                            time.sleep(5)
                            self._pm_stream_recovery_in_progress = False

                        threading.Thread(target=reset_recovery, daemon=True).start()

            record = {
                "timestamp": str(ts_ms),
                "session_epoch": str(session_epoch * 1000),
                "current_price": str(price),
                "price_to_beat": str(price_to_beat) if price_to_beat else "",
                "chart_timestamp": str(timestamp) if timestamp else "",
                "chart_timestamp_utc": str(chart_ts_utc) if chart_ts_utc else "0",
                "tooltip_url": tooltip_url,
            }
            stream_key = get_stream_key("polymarket_stream")
            self.redis.xadd(stream_key, record)

    def _sample_chainlink_prices(self, ts, ts_ms: int, session_epoch: int):
        """Sample Chainlink BTC/USD price."""
        price = self._chainlink_price
        if price is not None:
            record = {
                "timestamp": str(ts_ms),
                "session_epoch": str(session_epoch * 1000),
                "price": str(price),
            }
            stream_key = get_stream_key("chainlink_prices")
            self.redis.xadd(stream_key, record)

    def _sample_binance_orderbook(self, ts, ts_ms: int, session_epoch: int):
        """Sample Binance orderbook (20 levels)."""
        with self._ws_lock:
            bids = list(self._binance_orderbook_bids)
            asks = list(self._binance_orderbook_asks)

        record = {
            "timestamp": str(ts_ms),
            "session_epoch": str(session_epoch * 1000),
        }

        # Flatten top 20 bid levels
        for level in range(1, 21):
            price = 0.0
            size = 0.0
            if level - 1 < len(bids):
                price = float(bids[level - 1][0])
                size = float(bids[level - 1][1])
            record[f"bid_{level}_price"] = str(price)
            record[f"bid_{level}_size"] = str(size)

        # Flatten top 20 ask levels
        for level in range(1, 21):
            price = 0.0
            size = 0.0
            if level - 1 < len(asks):
                price = float(asks[level - 1][0])
                size = float(asks[level - 1][1])
            record[f"ask_{level}_price"] = str(price)
            record[f"ask_{level}_size"] = str(size)

        stream_key = get_stream_key("binance_orderbook")
        self.redis.xadd(stream_key, record)

    def _sample_binance_impact_prices(self, ts, ts_ms: int, session_epoch: int):
        """Sample Binance impact prices (calculated from orderbook)."""
        with self._ws_lock:
            bids = list(self._binance_orderbook_bids)
            asks = list(self._binance_orderbook_asks)

        if not bids or not asks:
            return

        def calculate_impact_price(orders: List[List], target_size: float) -> float:
            """Calculate price to fill target_size quantity."""
            remaining = target_size
            weighted_sum = 0.0
            for price, size in orders:
                price = float(price)
                size = float(size)
                if remaining <= 0:
                    break
                filled = min(remaining, size)
                weighted_sum += price * filled
                remaining -= filled
            if remaining > 0:
                return float(orders[-1][0])
            return weighted_sum / target_size if target_size > 0 else 0.0

        try:
            bid_impact_1pct = calculate_impact_price(bids, 10000)
            bid_impact_5pct = calculate_impact_price(bids, 50000)
            ask_impact_1pct = calculate_impact_price(asks, 10000)
            ask_impact_5pct = calculate_impact_price(asks, 50000)
            best_bid = float(bids[0][0]) if bids else 0.0
            best_ask = float(asks[0][0]) if asks else 0.0
            mid_price = (best_bid + best_ask) / 2.0

            record = {
                "timestamp": str(ts_ms),
                "session_epoch": str(session_epoch * 1000),
                "best_bid": str(best_bid),
                "best_ask": str(best_ask),
                "mid_price": str(mid_price),
                "bid_impact_1pct": str(bid_impact_1pct),
                "bid_impact_5pct": str(bid_impact_5pct),
                "ask_impact_1pct": str(ask_impact_1pct),
                "ask_impact_5pct": str(ask_impact_5pct),
                "spread": str(best_ask - best_bid),
            }
            stream_key = get_stream_key("binance_impact_prices")
            self.redis.xadd(stream_key, record)
        except Exception as e:
            logger.debug(f"Error calculating impact prices: {e}")

    def _sample_binance_cumulative_sizes(self, ts, ts_ms: int, session_epoch: int):
        """Sample Binance cumulative sizes at price levels."""
        with self._ws_lock:
            bids = list(self._binance_orderbook_bids)
            asks = list(self._binance_orderbook_asks)

        record = {
            "timestamp": str(ts_ms),
            "session_epoch": str(session_epoch * 1000),
        }

        cum_bid = 0.0
        cum_ask = 0.0
        for level in range(1, 21):
            if level - 1 < len(bids):
                cum_bid += float(bids[level - 1][1])
            if level - 1 < len(asks):
                cum_ask += float(asks[level - 1][1])
            record[f"cum_bid_{level}"] = str(cum_bid)
            record[f"cum_ask_{level}"] = str(cum_ask)

        stream_key = get_stream_key("binance_cumulative_sizes")
        self.redis.xadd(stream_key, record)

    def _sample_binance_trades(self, ts, ts_ms: int, session_epoch: int):
        """Sample Binance trade volumes."""
        with self._ws_lock:
            data = dict(self.binance_data)

        if not data:
            return

        def get_window_value(window_key: str, key: str) -> Any:
            if window_key in data:
                return data[window_key].get(key, 0)
            return 0

        record = {
            "timestamp": str(ts_ms),
            "session_epoch": str(session_epoch * 1000),
            "buy_volume_1m": str(get_window_value("1m", "buy")),
            "sell_volume_1m": str(get_window_value("1m", "sell")),
            "buy_volume_5m": str(get_window_value("5m", "buy")),
            "sell_volume_5m": str(get_window_value("5m", "sell")),
            "buy_volume_10s": str(get_window_value("10s", "buy")),
            "sell_volume_10s": str(get_window_value("10s", "sell")),
            "buy_volume_30s": str(get_window_value("30s", "buy")),
            "sell_volume_30s": str(get_window_value("30s", "sell")),
            "total_trades_1m": str(get_window_value("1m", "count")),
            "total_trades_5m": str(get_window_value("5m", "count")),
            "total_trades_10s": str(get_window_value("10s", "count")),
            "total_trades_30s": str(get_window_value("30s", "count")),
        }
        stream_key = get_stream_key("binance_trades")
        self.redis.xadd(stream_key, record)

    def _trim_streams(self):
        """Trim all streams to maintain 10-minute window."""
        for table in self.tables:
            stream_key = get_stream_key(table)
            self.redis.xtrim(stream_key)

    def _trim_worker(self, interval_seconds: int = 60, window_seconds: int = 2400):
        """Background worker that trims streams older than window_seconds."""
        logger.info(
            f"Starting trim worker: interval={interval_seconds}s, window={window_seconds}s ({window_seconds / 60:.0f} min)"
        )
        while not self._shutdown_event.is_set():
            try:
                for table in self.tables:
                    stream_key = get_stream_key(table)
                    removed = self.redis.trim_older_than(stream_key, window_seconds)
                    if removed > 0:
                        logger.debug(f"Trimmed {removed} old entries from {stream_key}")
            except Exception as e:
                logger.error(f"Error in trim worker: {e}")
            self._shutdown_event.wait(interval_seconds)
        logger.info("Trim worker stopped")

    def _start_trimming_thread(
        self, interval_seconds: int = 60, window_seconds: int = 2400
    ):
        """Start background trimming thread."""
        self._shutdown_event = threading.Event()
        self._trim_thread = threading.Thread(
            target=self._trim_worker,
            args=(interval_seconds, window_seconds),
            daemon=True,
        )
        self._trim_thread.start()
        logger.info(
            f"Background trim thread started (trimming every {interval_seconds}s, keeping last {window_seconds}s)"
        )

    def _stop_trimming_thread(self):
        """Stop background trimming thread."""
        if hasattr(self, "_shutdown_event"):
            self._shutdown_event.set()
        if hasattr(self, "_trim_thread") and self._trim_thread.is_alive():
            self._trim_thread.join(timeout=5)
            logger.info("Background trim thread stopped")


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
        default=os.environ.get("REDIS_HOST", "localhost"),
        help="Redis host (default: localhost)",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=int(os.environ.get("REDIS_PORT", 6379)),
        help="Redis port (default: 6379)",
    )
    parser.add_argument(
        "--tables",
        help="Comma-separated list of tables (default: all)",
    )
    parser.add_argument(
        "--sample-interval",
        type=float,
        default=0.2,
        help="Sample interval in seconds (default: 0.2)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without connecting to Redis (for debugging/testing)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["TRACE", "DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (default: INFO)",
    )
    parser.add_argument(
        "--parquet-dir",
        default="./data",
        help="Output directory for parquet files (default: ./data)",
    )

    args = parser.parse_args(sys.argv[1:])

    # Set log level
    log_level_map = {
        "TRACE": TRACE,
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
    }
    logging.getLogger().setLevel(log_level_map.get(args.log_level, logging.INFO))

    if args.command == "help":
        print("""
PMRedis - Polymarket and Binance Data Collector to Redis

Usage:
    python main.py start [options]

Options:
    --redis-host HOST      Redis host (default: localhost)
    --redis-port PORT      Redis port (default: 6379)
    --tables TABLES        Comma-separated tables (default: all)
    --sample-interval SEC  Sample interval in seconds (default: 0.05)
    --log-level LEVEL      Log level: TRACE, DEBUG, INFO, WARNING, ERROR (default: INFO)
    --parquet-dir DIR      Output directory for parquet files (default: ./data)

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
        dry_run=args.dry_run,
        parquet_dir=args.parquet_dir,
    )
    collector.run()


if __name__ == "__main__":
    main()
