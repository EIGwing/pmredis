"""Binance collector for orderbook data."""

import json
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import websocket
from web3 import Web3

from ..utils.helpers import (
    SIZE_BUCKETS,
    BTC_SIZE_BUCKETS,
    BPS_BUCKETS,
    get_price_at_cumulative_size,
    get_impact_price_at_size,
    get_cumulative_size_at_price_offset_bps,
)
from .base import BaseCollector

logger = logging.getLogger(__name__)

CHAINLINK_BTC_USD_ADDRESS = "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c"
CHAINLINK_ABI = '[{"inputs":[],"name":"latestRoundData","outputs":[{"name":"roundId","type":"uint80"},{"name":"answer","type":"int256"},{"name":"startedAt","type":"uint256"},{"name":"updatedAt","type":"uint256"},{"name":"answeredInRound","type":"uint80"}],"stateMutability":"view","type":"function"}]'
PUBLIC_RPC = "https://1rpc.io/eth"

_web3 = None


def _get_web3() -> Optional[Web3]:
    global _web3
    if _web3 is None:
        try:
            _web3 = Web3(Web3.HTTPProvider(PUBLIC_RPC))
            if _web3.is_connected():
                logger.debug("Connected to Ethereum node")
            else:
                _web3 = None
        except Exception as e:
            logger.debug(f"Failed to connect to Ethereum: {e}")
            _web3 = None
    return _web3


class BinanceDepthWSManager:
    """Manages Binance diff depth WebSocket with local orderbook state.

    Uses WebSocket for real-time incremental updates and periodic REST
    snapshots for resynchronization to prevent drift.
    """

    def __init__(
        self,
        symbol: str = "BTCUSDT",
        depth_limit: int = 5000,
        resync_interval: int = 30,
    ):
        self._symbol = symbol.lower()
        self._depth_limit = depth_limit
        self._resync_interval = resync_interval

        self._bids: Dict[str, str] = {}
        self._asks: Dict[str, str] = {}
        self._lock = threading.Lock()
        self._last_update_id: int = 0
        self._running = False
        self._ws: Optional[websocket.WebSocketApp] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._resync_thread: Optional[threading.Thread] = None
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "Mozilla/5.0"})
        self._initialized = threading.Event()
        self._binance_base_url = "https://api.binance.com/api/v3"

    def start(self) -> bool:
        """Initialize with REST snapshot, then start WebSocket + resync threads.

        Returns:
            True if initialization succeeded, False otherwise.
        """
        logger.info(f"Starting Binance Depth WebSocket for {self._symbol.upper()}...")

        snapshot = self._fetch_snapshot()
        if snapshot is None:
            logger.error("Failed to fetch initial snapshot")
            return False

        self._init_from_snapshot(snapshot)
        self._running = True
        self._initialized.set()

        self._ws_thread = threading.Thread(target=self._start_websocket, daemon=True)
        self._ws_thread.start()

        self._resync_thread = threading.Thread(target=self._resync_loop, daemon=True)
        self._resync_thread.start()

        logger.info("Binance Depth WebSocket started successfully")
        return True

    def stop(self) -> None:
        """Stop all threads and cleanup."""
        self._running = False
        if self._ws:
            self._ws.close()
        self._session.close()
        logger.info("Binance Depth WebSocket stopped")

    def _fetch_snapshot(self) -> Optional[Dict[str, Any]]:
        """Fetch full orderbook snapshot via REST API."""
        url = f"{self._binance_base_url}/depth"
        params = {"symbol": self._symbol.upper(), "limit": self._depth_limit}

        for attempt in range(3):
            try:
                response = self._session.get(url, params=params, timeout=30)
                if response.status_code == 429:
                    wait_time = 5 * (attempt + 1)
                    logger.warning(f"Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Snapshot fetch error (attempt {attempt + 1}): {e}")
                if attempt < 2:
                    time.sleep(2)
        return None

    def _init_from_snapshot(self, data: Dict[str, Any]) -> None:
        """Initialize local orderbook from REST snapshot."""
        with self._lock:
            self._last_update_id = data.get("lastUpdateId", 0)
            self._bids = {price: qty for price, qty in data.get("bids", [])}
            self._asks = {price: qty for price, qty in data.get("asks", [])}
        logger.debug(
            f"Initialized orderbook with {len(self._bids)} bids, {len(self._asks)} asks, lastUpdateId={self._last_update_id}"
        )

    def _start_websocket(self) -> None:
        """Start WebSocket connection in background thread."""
        ws_url = f"wss://stream.binance.com:9443/ws/{self._symbol}@depth@100ms"

        def on_message(ws, message):
            self._on_ws_message(message)

        def on_error(ws, error):
            logger.error(f"WebSocket error: {error}")

        def on_close(ws, close_status_code, close_msg):
            logger.debug(f"WebSocket closed: {close_status_code} - {close_msg}")
            if self._running:
                logger.info("WebSocket closed, will reconnect...")

        while self._running:
            try:
                self._ws = websocket.WebSocketApp(
                    ws_url,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                logger.error(f"WebSocket exception: {e}")

            if self._running:
                time.sleep(1)

    def _on_ws_message(self, message: str) -> None:
        """Handle WebSocket message - apply diff updates."""
        try:
            data = json.loads(message)

            if data.get("e") != "depthUpdate":
                return

            event_u = data.get("u", 0)

            with self._lock:
                if self._last_update_id == 0:
                    return

                if event_u <= self._last_update_id:
                    return

                for price, qty in data.get("b", []):
                    if float(qty) == 0:
                        self._bids.pop(price, None)
                    else:
                        self._bids[price] = qty

                for price, qty in data.get("a", []):
                    if float(qty) == 0:
                        self._asks.pop(price, None)
                    else:
                        self._asks[price] = qty

                self._last_update_id = event_u

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.debug(f"Error parsing WebSocket message: {e}")

    def _resync_loop(self) -> None:
        """Periodically resync from REST snapshot to prevent drift."""
        while self._running:
            time.sleep(self._resync_interval)
            if not self._running:
                break

            logger.debug("Performing periodic orderbook resync...")
            snapshot = self._fetch_snapshot()
            if snapshot:
                self._init_from_snapshot(snapshot)
            else:
                logger.warning("Resync failed, will retry next interval")

    def get_orderbook(self) -> Tuple[List[List], List[List]]:
        """Return sorted bids/asks as lists of [price, qty].

        Returns:
            Tuple of (bids, asks) where each is a list of [price, qty] pairs.
            Bids are sorted descending by price, asks ascending.
        """
        with self._lock:
            bids = sorted(
                [[float(p), float(q)] for p, q in self._bids.items()],
                key=lambda x: x[0],
                reverse=True,
            )
            asks = sorted(
                [[float(p), float(q)] for p, q in self._asks.items()],
                key=lambda x: x[0],
            )
        return bids, asks

    def is_initialized(self) -> bool:
        """Check if the orderbook has been initialized."""
        return self._initialized.is_set()

    def wait_for_init(self, timeout: float = 10.0) -> bool:
        """Wait for initialization to complete.

        Args:
            timeout: Maximum time to wait in seconds.

        Returns:
            True if initialized, False if timeout.
        """
        return self._initialized.wait(timeout=timeout)


class BinanceTradesWSManager:
    """Manages Binance trade WebSocket with rolling window volume tracking.

    Tracks buy/sell volumes over multiple time windows:
    - 10 seconds
    - 30 seconds
    - 1 minute
    - 5 minutes
    """

    WINDOWS_MS = {
        "10s": 10_000,
        "30s": 30_000,
        "1m": 60_000,
        "5m": 300_000,
    }

    def __init__(self, symbol: str = "BTCUSDT"):
        self._symbol = symbol.lower()
        self._running = False
        self._ws: Optional[websocket.WebSocketApp] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._initialized = threading.Event()

        # Trade data: list of (timestamp_ms, price, qty, is_buyer_maker)
        self._trades: deque = deque(maxlen=10000)

        # Windowed volumes
        self._windows: Dict[str, Dict[str, float]] = {
            "10s": {"buy": 0, "sell": 0, "count": 0},
            "30s": {"buy": 0, "sell": 0, "count": 0},
            "1m": {"buy": 0, "sell": 0, "count": 0},
            "5m": {"buy": 0, "sell": 0, "count": 0},
        }

        # Last cleanup timestamp
        self._last_cleanup_ms = 0

    def start(self) -> bool:
        """Start the trade WebSocket."""
        logger.info(f"Starting Binance Trade WebSocket for {self._symbol.upper()}...")

        self._running = True
        self._initialized.set()

        self._ws_thread = threading.Thread(target=self._start_websocket, daemon=True)
        self._ws_thread.start()

        logger.info("Binance Trade WebSocket started successfully")
        return True

    def stop(self) -> None:
        """Stop the WebSocket."""
        self._running = False
        if self._ws:
            self._ws.close()
        logger.info("Binance Trade WebSocket stopped")

    def _start_websocket(self) -> None:
        """Start WebSocket connection."""
        ws_url = f"wss://stream.binance.com:9443/ws/{self._symbol}@trade"

        def on_message(ws, message):
            self._on_ws_message(message)

        def on_error(ws, error):
            logger.error(f"Trade WebSocket error: {error}")

        def on_close(ws, close_status_code, close_msg):
            logger.debug(f"Trade WebSocket closed: {close_status_code} - {close_msg}")
            if self._running:
                logger.info("Trade WebSocket closed, will reconnect...")

        while self._running:
            try:
                self._ws = websocket.WebSocketApp(
                    ws_url,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                logger.error(f"Trade WebSocket exception: {e}")

            if self._running:
                time.sleep(1)

    def _on_ws_message(self, message: str) -> None:
        """Handle trade message."""
        try:
            data = json.loads(message)

            trade = {
                "t": data.get("t", 0),  # trade id
                "T": data.get("T", 0),  # trade time
                "p": data.get("p", "0"),  # price
                "q": data.get("q", "0"),  # quantity
                "m": data.get("m", True),  # is buyer maker (true = sell, false = buy)
            }

            self._add_trade(trade)

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.debug(f"Error parsing trade message: {e}")

    def _add_trade(self, trade: Dict) -> None:
        """Add trade and cleanup old trades."""
        timestamp_ms = trade["T"]
        price = float(trade["p"])
        qty = float(trade["q"])
        is_sell = trade["m"]  # True = sell (buyer is maker), False = buy

        with self._lock:
            self._trades.append(
                {
                    "timestamp_ms": timestamp_ms,
                    "price": price,
                    "qty": qty,
                    "is_sell": is_sell,
                }
            )

            # Cleanup old trades every second
            current_time_ms = int(time.time() * 1000)
            if current_time_ms - self._last_cleanup_ms > 1000:
                self._cleanup_old_trades(current_time_ms)
                self._last_cleanup_ms = current_time_ms

    def _cleanup_old_trades(self, current_time_ms: int) -> None:
        """Remove trades outside the 5-minute window and recalculate windows."""
        # Keep trades within 5 minutes
        cutoff_ms = current_time_ms - self.WINDOWS_MS["5m"]

        # Remove old trades
        while self._trades and self._trades[0]["timestamp_ms"] < cutoff_ms:
            self._trades.popleft()

        # Recalculate all windows
        self._recalculate_windows(current_time_ms)

    def _recalculate_windows(self, current_time_ms: int) -> None:
        """Recalculate volume for all windows."""
        for window_name, window_ms in self.WINDOWS_MS.items():
            cutoff_ms = current_time_ms - window_ms
            buy_vol = 0.0
            sell_vol = 0.0
            count = 0

            for trade in self._trades:
                if trade["timestamp_ms"] >= cutoff_ms:
                    if trade["is_sell"]:
                        sell_vol += trade["qty"]
                    else:
                        buy_vol += trade["qty"]
                    count += 1

            self._windows[window_name] = {
                "buy": buy_vol,
                "sell": sell_vol,
                "count": count,
            }

    def get_volumes(self) -> Dict[str, Dict[str, float]]:
        """Get current volumes for all windows.

        Returns:
            Dict with window names as keys, each containing:
            - buy: buy volume
            - sell: sell volume
            - count: number of trades
            - total: total volume
            - buy_pct: buy percentage (0-100)
        """
        with self._lock:
            current_time_ms = int(time.time() * 1000)
            self._recalculate_windows(current_time_ms)

            result = {}
            for window_name, data in self._windows.items():
                total = data["buy"] + data["sell"]
                buy_pct = (data["buy"] / total * 100) if total > 0 else 50.0
                result[window_name] = {
                    "buy": data["buy"],
                    "sell": data["sell"],
                    "total": total,
                    "count": data["count"],
                    "buy_pct": buy_pct,
                }
            return result

    def get_1m_avg_trade_size(self) -> float:
        """Get average trade size over 1 minute."""
        with self._lock:
            data = self._windows["1m"]
            if data["count"] > 0:
                return data["total"] / data["count"]
            return 0.0

    def is_initialized(self) -> bool:
        """Check if the manager has been initialized."""
        return self._initialized.is_set()


class BinanceCollector(BaseCollector):
    """Collector for Binance orderbook data."""

    def __init__(self):
        super().__init__()
        self.binance_base_url = "https://api.binance.com/api/v3"

    def fetch_orderbook(
        self, symbol: str = "BTCUSDT", limit: int = 100
    ) -> Optional[Dict[str, Any]]:
        """Fetch orderbook from Binance."""
        url = f"{self.binance_base_url}/depth"
        params = {"symbol": symbol, "limit": limit}

        for attempt in range(3):
            try:
                response = self.session.get(url, params=params, timeout=30)

                if response.status_code == 429:
                    logger.warning("Rate limited, waiting 5s...")
                    time.sleep(5)
                    continue

                response.raise_for_status()
                return response.json()

            except Exception as e:
                logger.error(f"Request error: {e}")
                if attempt < 2:
                    time.sleep(2)
        return None

    def fetch_chainlink_price(self) -> Optional[float]:
        """Fetch BTC/USD price from Chainlink Price Feed via RPC."""
        try:
            w3 = _get_web3()
            if w3 is None:
                return None

            contract = w3.eth.contract(
                Web3.to_checksum_address(CHAINLINK_BTC_USD_ADDRESS), abi=CHAINLINK_ABI
            )
            price = contract.functions.latestRoundData().call()[1]
            return price / 1e8
        except Exception as e:
            logger.debug(f"Chainlink price fetch error: {e}")
            global _web3
            _web3 = None
        return None

    def fetch_prev_5min_close_price(self, symbol: str = "BTCUSDT") -> Optional[float]:
        """Fetch the close price of the previous 5-minute kline.

        Used to determine session price when TUI starts mid-session.

        Args:
            symbol: Trading pair symbol (default BTCUSDT)

        Returns:
            Close price of previous 5-min kline, or None on failure.
        """
        try:
            # Calculate current 5-minute session start (UTC)
            current_ts = int(time.time())
            current_5min = current_ts - (current_ts % 300)

            # Previous session starts 5 minutes before current
            prev_5min = current_5min - 300

            # Convert to milliseconds for API
            prev_start_ms = prev_5min * 1000

            url = f"{self.binance_base_url}/klines"
            params = {
                "symbol": symbol,
                "interval": "5m",
                "startTime": prev_start_ms,
                "limit": 1,
            }

            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()
            if data and len(data) > 0:
                # data[0] = [open_time, open, high, low, close, volume, ...]
                # We want the close price
                close_price = float(data[0][4])
                logger.debug(f"Previous 5-min close price: {close_price}")
                return close_price

            return None
        except Exception as e:
            logger.debug(f"Failed to fetch previous 5-min close price: {e}")
            return None

    def fetch_binance_price(self) -> Optional[float]:
        """Fetch BTC/USD price from Binance."""
        try:
            url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
            response = self.session.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                price = data.get("price")
                if price:
                    return float(price)
        except Exception as e:
            logger.debug(f"Binance price fetch error: {e}")

        try:
            url = "https://api.binance.us/api/v3/ticker/price?symbol=BTCUSDT"
            response = self.session.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                price = data.get("price")
                if price:
                    return float(price)
        except Exception as e:
            logger.debug(f"Binance US price fetch error: {e}")
        return None

    def fetch_oracle_price(self) -> Optional[float]:
        """Fetch BTC/USD price from Chainlink as oracle reference."""
        return self.fetch_chainlink_price()

    def collect_orderbook(
        self,
        symbol: str = "BTCUSDT",
        poll_interval_seconds: int = 2,
        duration_seconds: Optional[int] = None,
        depth_limit: int = 100,
        output_dir: str = "logs",
    ) -> List[Dict[str, Any]]:
        """
        Collect Binance orderbook data with full depth.

        Args:
            symbol: Trading pair (default BTCUSDT)
            poll_interval_seconds: How often to poll
            duration_seconds: Total collection time (None = run forever)
            depth_limit: Number of levels to fetch
            output_dir: Directory to save data
        """
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(
            output_dir,
            f"binance_{symbol.lower()}_depth{depth_limit}_poll{poll_interval_seconds}_live.csv",
        )

        logger.info(
            f"Starting Binance {symbol} orderbook collection ({depth_limit} levels)..."
        )
        if duration_seconds:
            logger.info(f"Will collect for {duration_seconds} seconds")
        else:
            logger.info("Will collect until Ctrl+C")
        logger.info(f"Poll interval: {poll_interval_seconds} seconds")
        logger.info(f"Saving to: {output_file}")

        orderbook_records = []
        start_time = time.time()

        while True:
            if duration_seconds and time.time() - start_time >= duration_seconds:
                break

            try:
                ob = self.fetch_orderbook(symbol, limit=depth_limit)
                if ob:
                    timestamp = int(time.time() * 1000)
                    local_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    oracle_price = self.fetch_oracle_price()

                    bids = ob.get("bids", [])
                    asks = ob.get("asks", [])

                    best_bid = float(bids[0][0]) if bids else None
                    best_ask = float(asks[0][0]) if asks else None
                    bid_depth = sum(float(b[1]) for b in bids)
                    ask_depth = sum(float(a[1]) for a in asks)

                    record = {
                        "timestamp": timestamp,
                        "local_time": local_time,
                        "symbol": symbol,
                        "oracle_price_usd": oracle_price,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "mid_price": (best_bid + best_ask) / 2
                        if best_bid and best_ask
                        else None,
                        "bid_depth": bid_depth,
                        "ask_depth": ask_depth,
                        "spread": best_ask - best_bid
                        if best_bid and best_ask
                        else None,
                    }

                    for i, (price, size) in enumerate(bids):
                        record[f"bid_price_{i + 1}"] = float(price)
                        record[f"bid_size_{i + 1}"] = float(size)

                    for i, (price, size) in enumerate(asks):
                        record[f"ask_price_{i + 1}"] = float(price)
                        record[f"ask_size_{i + 1}"] = float(size)

                    orderbook_records.append(record)

                    df = pd.DataFrame(orderbook_records)
                    df.to_csv(output_file, index=False)

                    elapsed = time.time() - start_time
                    logger.info(
                        f"Snapshots: {len(orderbook_records)} | Bid: {best_bid} | Ask: {best_ask} | Oracle: ${oracle_price} | Time: {elapsed:.0f}s"
                    )

            except Exception as e:
                logger.error(f"Error fetching Binance orderbook: {e}")

            time.sleep(poll_interval_seconds)

        logger.info(
            f"Finished! Collected {len(orderbook_records)} snapshots to {output_file}"
        )
        return orderbook_records

    def collect_aggregated(
        self,
        symbol: str = "BTCUSDT",
        poll_interval_seconds: int = 2,
        duration_seconds: Optional[int] = None,
        depth_limit: int = 5000,
        output_dir: str = "logs",
    ) -> List[Dict[str, Any]]:
        """
        Collect Binance orderbook aggregated by cumulative size.

        Args:
            symbol: Trading pair (default BTCUSDT)
            poll_interval_seconds: How often to poll
            duration_seconds: Total collection time (None = run forever)
            depth_limit: Number of levels to fetch (max 5000)
            output_dir: Directory to save data
        """
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(
            output_dir,
            f"binance_{symbol.lower()}_agg_depth{depth_limit}_poll{poll_interval_seconds}_live.csv",
        )

        logger.info(f"Starting Binance {symbol} aggregated orderbook collection...")
        if duration_seconds:
            logger.info(f"Will collect for {duration_seconds} seconds")
        else:
            logger.info("Will collect until Ctrl+C")
        logger.info(f"Poll interval: {poll_interval_seconds} seconds")
        logger.info(f"Saving to: {output_file}")

        orderbook_records = []
        start_time = time.time()

        while True:
            if duration_seconds and time.time() - start_time >= duration_seconds:
                break

            try:
                ob = self.fetch_orderbook(symbol, limit=depth_limit)
                if ob:
                    timestamp = int(time.time() * 1000)
                    local_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    oracle_price = self.fetch_oracle_price()

                    bids = ob.get("bids", [])
                    asks = ob.get("asks", [])

                    record = {
                        "timestamp": timestamp,
                        "local_time": local_time,
                        "symbol": symbol,
                        "oracle_price_usd": oracle_price,
                    }

                    for size in SIZE_BUCKETS[:-1]:
                        record[f"bid_price_at_{size}"] = get_price_at_cumulative_size(
                            bids, size
                        )
                    record["bid_price_at_500+"] = get_price_at_cumulative_size(
                        bids, 500
                    )

                    for size in SIZE_BUCKETS[:-1]:
                        record[f"ask_price_at_{size}"] = get_price_at_cumulative_size(
                            asks, size
                        )
                    record["ask_price_at_500+"] = get_price_at_cumulative_size(
                        asks, 500
                    )

                    orderbook_records.append(record)

                    df = pd.DataFrame(orderbook_records)
                    df.to_csv(output_file, index=False)

                    elapsed = time.time() - start_time
                    best_bid = float(bids[0][0]) if bids else None
                    best_ask = float(asks[0][0]) if asks else None
                    logger.info(
                        f"Snapshots: {len(orderbook_records)} | Bid: {best_bid} | Ask: {best_ask} | Oracle: ${oracle_price} | Time: {elapsed:.0f}s"
                    )

            except Exception as e:
                logger.error(f"Error fetching Binance orderbook: {e}")

            time.sleep(poll_interval_seconds)

        logger.info(
            f"Finished! Collected {len(orderbook_records)} snapshots to {output_file}"
        )
        return orderbook_records

    def collect_impact(
        self,
        symbol: str = "BTCUSDT",
        poll_interval_seconds: int = 1,
        duration_seconds: Optional[int] = None,
        depth_limit: int = 5000,
        resync_interval: int = 30,
    ) -> None:
        """Collect Binance orderbook impact prices using WebSocket.

        Uses diff depth WebSocket for real-time updates with periodic REST
        resyncs to prevent drift. This avoids rate limiting issues with
        high-frequency REST polling.

        Args:
            symbol: Trading pair (default BTCUSDT)
            poll_interval_seconds: Display refresh interval (default 1s)
            duration_seconds: Total collection time (None = run forever)
            depth_limit: Number of levels to track (max 5000)
            resync_interval: Seconds between REST resyncs (default 30s)
        """
        logger.info(
            f"Starting Binance {symbol} impact analysis ({depth_limit} levels, WebSocket mode)..."
        )
        if duration_seconds:
            logger.info(f"Will collect for {duration_seconds} seconds")
        else:
            logger.info("Will collect until Ctrl+C")
        logger.info(f"Display interval: {poll_interval_seconds} seconds")
        logger.info(f"Resync interval: {resync_interval} seconds")

        ws_manager = BinanceDepthWSManager(
            symbol=symbol,
            depth_limit=depth_limit,
            resync_interval=resync_interval,
        )

        if not ws_manager.start():
            logger.error("Failed to start WebSocket manager")
            return

        start_time = time.time()
        session_open_price: Optional[float] = None
        session_open_time: Optional[int] = None

        try:
            while True:
                if duration_seconds and time.time() - start_time >= duration_seconds:
                    break

                try:
                    bids, asks = ws_manager.get_orderbook()

                    if not bids or not asks:
                        time.sleep(poll_interval_seconds)
                        continue

                    current_time = time.time()
                    local_time = datetime.now().strftime("%H:%M:%S")

                    best_bid = bids[0][0] if bids else None
                    best_ask = asks[0][0] if asks else None
                    mid_price = (
                        (best_bid + best_ask) / 2 if best_bid and best_ask else None
                    )

                    if mid_price is None:
                        time.sleep(poll_interval_seconds)
                        continue

                    current_5min_boundary = int(current_time) - (
                        int(current_time) % 300
                    )

                    if session_open_time != current_5min_boundary:
                        session_open_price = mid_price
                        session_open_time = current_5min_boundary

                    session_change = (
                        ((mid_price - session_open_price) / session_open_price * 100)
                        if session_open_price
                        else 0
                    )

                    print(
                        f"\n=== {local_time} | Mid: ${mid_price:,.2f} | Session Open: ${session_open_price:,.2f} ({session_change:+.2f}%)"
                    )

                    print("Impact Prices (BTC):")
                    header = "       |"
                    for size in BTC_SIZE_BUCKETS:
                        if size == 50:
                            header += f" {size}+   |"
                        else:
                            header += f" {size}    |"
                    print(header)
                    print("-" * len(header))

                    bid_row = "Bid   |"
                    ask_row = "Ask   |"
                    for size in BTC_SIZE_BUCKETS:
                        bid_price = get_impact_price_at_size(bids, size)
                        ask_price = get_impact_price_at_size(asks, size)
                        if bid_price:
                            bid_row += f" {bid_price:,.0f} |"
                        else:
                            bid_row += "  N/A  |"
                        if ask_price:
                            ask_row += f" {ask_price:,.0f} |"
                        else:
                            ask_row += "  N/A  |"
                    print(bid_row)
                    print(ask_row)

                    print("\nCumulative Size at % moves:")
                    bps_labels = ["10bps", "50bps", "1%", "2%", "3%", "4%", "5%+"]
                    header2 = "       |"
                    for label in bps_labels:
                        header2 += f" {label:>6} |"
                    print(header2)
                    print("-" * len(header2))

                    bid_cum_row = "Bid   |"
                    ask_cum_row = "Ask   |"
                    for bps in BPS_BUCKETS:
                        bid_size = get_cumulative_size_at_price_offset_bps(
                            bids, mid_price, bps, "down"
                        )
                        ask_size = get_cumulative_size_at_price_offset_bps(
                            asks, mid_price, bps, "up"
                        )
                        bid_cum_row += f" {bid_size:>6.1f} |"
                        ask_cum_row += f" {ask_size:>6.1f} |"
                    print(bid_cum_row)
                    print(ask_cum_row)

                except Exception as e:
                    logger.error(f"Error processing orderbook: {e}")

                time.sleep(poll_interval_seconds)

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            ws_manager.stop()

        logger.info("Impact analysis finished!")
