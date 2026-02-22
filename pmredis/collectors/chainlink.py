"""Chainlink collector for BTC/USD price from Polygon."""

import logging
import threading
import time
from typing import Any, Dict, List, Optional

from web3 import Web3

logger = logging.getLogger(__name__)

POLYGON_RPC = "https://polygon-rpc.com"
PROXY_ADDR = "0xc907E116054Ad103354f2D350FD2514433D57F6f"

ABI = [
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"name": "roundId", "type": "uint80"},
            {"name": "answer", "type": "int256"},
            {"name": "startedAt", "type": "uint256"},
            {"name": "updatedAt", "type": "uint256"},
            {"name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "_roundId", "type": "uint80"}],
        "name": "getRoundData",
        "outputs": [
            {"name": "roundId", "type": "uint80"},
            {"name": "answer", "type": "int256"},
            {"name": "startedAt", "type": "uint256"},
            {"name": "updatedAt", "type": "uint256"},
            {"name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

DECIMALS = 8


class ChainlinkCollector:
    """Collector for Chainlink BTC/USD price on Polygon."""

    def __init__(self):
        self._w3 = None
        self._contract = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._recent_prices: Optional[Dict[str, Any]] = None
        self._lock = threading.Lock()
        self._last_request_time = 0
        self._min_request_interval = 1.0

    def _get_web3(self) -> Optional[Web3]:
        if self._w3 is None:
            try:
                self._w3 = Web3(Web3.HTTPProvider(POLYGON_RPC))
                if self._w3.is_connected():
                    self._contract = self._w3.eth.contract(
                        address=self._w3.to_checksum_address(PROXY_ADDR), abi=ABI
                    )
                    logger.debug(
                        "Connected to Polygon and initialized Chainlink contract"
                    )
                else:
                    self._w3 = None
            except Exception as e:
                logger.debug(f"Failed to connect to Polygon: {e}")
                self._w3 = None
        return self._w3

    def _rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)
        self._last_request_time = time.time()

    def get_price_at_5min_boundary(
        self, target_5min_ts: int
    ) -> Optional[Dict[str, Any]]:
        """Get BTC/USD price at or after the target 5-minute boundary."""
        w3 = self._get_web3()
        if w3 is None or self._contract is None:
            return None

        try:
            self._rate_limit()
            latest_round = self._contract.functions.latestRoundData().call()
            round_id = latest_round[0]

            # Only check a few rounds back (prices update ~every 25sec)
            for _ in range(10):
                try:
                    self._rate_limit()
                    data = self._contract.functions.getRoundData(round_id).call()
                    updated_at = data[3]

                    if updated_at >= target_5min_ts:
                        price = round(int(data[1]) / (10**DECIMALS), 2)
                        return {
                            "timestamp": target_5min_ts,
                            "price": price,
                            "actual_updated_at": updated_at,
                        }

                    round_id -= 1

                except Exception as e:
                    logger.debug(f"Error getting round {round_id}: {e}")
                    break

        except Exception as e:
            logger.debug(f"Error getting price at 5min boundary: {e}")
            self._w3 = None

        return None

    def _poll(self) -> None:
        """Background polling loop - only update at 5-minute boundaries."""
        now = int(time.time())
        current_5min = now - (now % 300)
        price_data = self.get_price_at_5min_boundary(current_5min)
        if price_data:
            with self._lock:
                self._recent_prices = price_data

        while self._running:
            now = int(time.time())
            current_5min = now - (now % 300)

            cached = self.get_cached_prices()
            cached_5min = cached.get("timestamp", 0) if cached else 0

            if current_5min != cached_5min:
                price_data = self.get_price_at_5min_boundary(current_5min)
                if price_data:
                    with self._lock:
                        self._recent_prices = price_data

            for _ in range(30):
                if not self._running:
                    break
                time.sleep(1)

    def start_polling(self, interval: int = 30) -> None:
        """Start background polling."""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._thread.start()

    def stop_polling(self) -> None:
        """Stop background polling."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)

    def get_cached_prices(self) -> Optional[Dict[str, Any]]:
        """Get cached 5-minute price."""
        with self._lock:
            return self._recent_prices
