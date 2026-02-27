"""Polymarket collector for orderbook data."""

import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import websocket

TRACE = 5
logging.addLevelName(TRACE, "TRACE")

from .base import BaseCollector

logger = logging.getLogger(__name__)


class PolymarketCollector(BaseCollector):
    """Collector for Polymarket orderbook data."""

    def __init__(self):
        super().__init__()
        self.gamma_base_url = "https://gamma-api.polymarket.com/markets"
        self.events_base_url = "https://gamma-api.polymarket.com/events"
        self.clob_base_url = "https://clob.polymarket.com/prices-history"
        self.data_base_url = "https://data-api.polymarket.com/trades"
        self.markets_data = []
        self.prices_data = []
        self.trades_data = []
        self.orderbook_data = []
        self.ws_app = None

    def _is_btc_market(self, market: Dict[str, Any]) -> bool:
        """Check if market is BTC-related by question or slug."""
        question = market.get("question", "").lower()
        slug = market.get("slug", "").lower()

        return (
            "btc" in question
            or "bitcoin" in question
            or "btc" in slug
            or "bitcoin" in slug
            or "will btc" in question
            or "will bitcoin" in question
            or "btc-updown-" in slug
            or "btc-up-or-down" in slug
            or "bitcoin-updown-" in slug
            or "bitcoin-up-or-down" in slug
        )

    def _is_btc_up_down_market(self, market: Dict[str, Any]) -> bool:
        """Check if market is a BTC up/down market."""
        question = market.get("question", "").lower()
        slug = market.get("slug", "").lower()

        return (
            "btc-updown-" in slug
            or "btc-up-or-down" in slug
            or "bitcoin-updown-" in slug
            or "bitcoin-up-or-down" in slug
            or ("btc" in question and ("up" in question or "down" in question))
        )

    def _extract_btc_markets_from_response(
        self, markets: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Extract BTC up/down markets from API response."""
        btc_markets = []

        for market in markets:
            if self._is_btc_up_down_market(market):
                clob_token_ids = market.get("clobTokenIds", [])
                if isinstance(clob_token_ids, str):
                    clob_token_ids = json.loads(clob_token_ids)

                market_data = {
                    "market_id": market.get("id"),
                    "slug": market.get("slug"),
                    "question": market.get("question", ""),
                    "created_at": market.get("createdAt"),
                    "end_date": market.get("endDate"),
                    "startDate": market.get("startDate"),
                    "resolution_source": market.get("resolutionSource"),
                    "rules": market.get("rules"),
                    "category": market.get("category"),
                    "tags": market.get("tags"),
                    "clobTokenIds": clob_token_ids,
                    "condition_id": market.get("conditionId"),
                }
                btc_markets.append(market_data)

        return btc_markets

    def fetch_markets(
        self, days: int = 100, limit: int = 100, max_events: int = 1000
    ) -> List[Dict[str, Any]]:
        """Fetch markets from Gamma API, filter for BTC up/down markets."""
        cursor = None
        all_markets = []
        total_markets_processed = 0

        while total_markets_processed < max_events:
            params = {"limit": limit}
            if cursor:
                params["cursor"] = cursor

            logger.debug(
                f"Fetching markets with cursor {cursor} (processed: {total_markets_processed}/{max_events})..."
            )

            try:
                response = self._make_request(self.gamma_base_url, params)
                if not response or not isinstance(response, list):
                    break

                markets = response
                if not markets:
                    break

                btc_markets = [m for m in markets if self._is_btc_market(m)]
                all_markets.extend(btc_markets)
                total_markets_processed += len(markets)

                if len(markets) < limit:
                    break

                cursor = markets[-1].get("id")
                time.sleep(1)

            except Exception as e:
                logger.error(f"Error fetching markets: {e}")
                break

        btc_updown_markets = self._fetch_btc_updown_from_events(days, max_events)
        all_markets.extend(btc_updown_markets)

        seen_ids = set()
        unique_markets = []
        for m in all_markets:
            mid = m.get("market_id") or m.get("id")
            if mid not in seen_ids:
                seen_ids.add(mid)
                unique_markets.append(m)

        logger.debug(f"Found {len(unique_markets)} BTC/crypto markets")
        return unique_markets

    def _fetch_btc_updown_from_events(
        self, days: int = 100, max_markets: int = 100
    ) -> List[Dict[str, Any]]:
        """Fetch 5-minute BTC up/down markets from events API."""
        btc_markets = []
        now = int(time.time())
        start_time = now - (86400 * days)
        start_time = start_time - (start_time % 300)

        current_time = start_time
        api_calls = 0
        while current_time <= now and len(btc_markets) < max_markets:
            slug = f"btc-updown-5m-{current_time}"

            try:
                response = self._make_request(self.events_base_url, {"slug": slug})
                api_calls += 1

                if response and isinstance(response, list) and len(response) > 0:
                    event = response[0]
                    for market in event.get("markets", []):
                        clob_token_ids = market.get("clobTokenIds", [])
                        if isinstance(clob_token_ids, str):
                            clob_token_ids = json.loads(clob_token_ids)

                        market_data = {
                            "market_id": market.get("id"),
                            "slug": market.get("slug"),
                            "question": market.get("question", ""),
                            "startDate": market.get("startDate"),
                            "endDate": market.get("endDate"),
                            "clobTokenIds": clob_token_ids,
                            "condition_id": market.get("conditionId"),
                            "outcomePrices": market.get("outcomePrices", "[]"),
                        }
                        btc_markets.append(market_data)

                if api_calls % 50 == 0:
                    logger.debug(
                        f"Progress: {api_calls} calls, {len(btc_markets)} markets"
                    )

            except Exception as e:
                logger.debug(f"Error fetching {slug}: {e}")

            current_time += 300

        logger.debug(f"Found {len(btc_markets)} 5-minute markets")
        return btc_markets

    def get_current_and_upcoming_markets(
        self, interval_minutes: int = 5, hours_ahead: int = 1
    ) -> List[Dict[str, Any]]:
        """Get currently active and upcoming BTC markets."""
        markets = []
        now = int(time.time())
        current_ts = now - (now % (interval_minutes * 60))

        for i in range(-1, int(hours_ahead * 60 / interval_minutes) + 2):
            market_ts = current_ts + (i * interval_minutes * 60)

            if interval_minutes == 5:
                slug = f"btc-updown-5m-{market_ts}"
            else:
                slug = f"btc-updown-15m-{market_ts}"

            try:
                response = self._make_request(self.events_base_url, {"slug": slug})

                if response and isinstance(response, list) and len(response) > 0:
                    event = response[0]
                    for market in event.get("markets", []):
                        clob_token_ids = market.get("clobTokenIds", [])
                        if isinstance(clob_token_ids, str):
                            clob_token_ids = json.loads(clob_token_ids)

                        local_start = datetime.fromtimestamp(market_ts).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        local_end = datetime.fromtimestamp(
                            market_ts + interval_minutes * 60
                        ).strftime("%Y-%m-%d %H:%M:%S")

                        market_data = {
                            "market_id": market.get("id"),
                            "slug": market.get("slug"),
                            "question": market.get("question", ""),
                            "timestamp": market_ts,
                            "local_start": local_start,
                            "local_end": local_end,
                            "clobTokenIds": clob_token_ids,
                            "interval": interval_minutes,
                            "status": "active"
                            if market_ts <= now < market_ts + interval_minutes * 60
                            else "upcoming"
                            if market_ts > now
                            else "ended",
                        }
                        markets.append(market_data)
                        logger.debug(
                            f"Found {market_data['status']} market: {slug} ({local_start})"
                        )

            except Exception as e:
                logger.debug(f"Error fetching {slug}: {e}")

        return markets

    def fetch_orderbook(self, token_id: str) -> Optional[Dict[str, Any]]:
        """Fetch current orderbook snapshot for a token."""
        url = f"https://clob.polymarket.com/book"
        params = {"token_id": token_id}
        return self._make_request(url, params)

    def collect_orderbook(
        self,
        interval_minutes: int = 5,
        poll_interval_seconds: int = 2,
        duration_seconds: Optional[int] = None,
        output_dir: str = "logs",
    ) -> List[Dict[str, Any]]:
        """Collect orderbook for current active BTC market."""
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(
            output_dir,
            f"polymarket_btc_{interval_minutes}m_poll{poll_interval_seconds}_live.csv",
        )

        logger.info(
            f"Starting automatic orderbook collection for {interval_minutes}min markets..."
        )
        if duration_seconds:
            logger.info(f"Will collect for {duration_seconds} seconds")
        else:
            logger.info("Will collect until Ctrl+C")
        logger.info(f"Poll interval: {poll_interval_seconds} seconds")
        logger.info(f"Saving to: {output_file}")

        orderbook_records = []
        start_time = time.time()
        current_market = None
        market_switch_count = 0

        while True:
            if duration_seconds and time.time() - start_time >= duration_seconds:
                break

            markets = self.get_current_and_upcoming_markets(
                interval_minutes=interval_minutes, hours_ahead=0.5
            )

            active_markets = [m for m in markets if m.get("status") == "active"]

            if active_markets:
                new_market = active_markets[0]
                market_key = new_market.get("slug")

                if current_market != market_key:
                    current_market = market_key
                    market_switch_count += 1
                    logger.info(
                        f"[{market_switch_count}] Switched to market: {market_key}"
                    )
                    logger.info(
                        f"  Local time: {new_market.get('local_start')} - {new_market.get('local_end')}"
                    )

                token_ids = new_market.get("clobTokenIds", [])

                for token_id in token_ids:
                    try:
                        ob = self.fetch_orderbook(token_id)
                        if ob:
                            timestamp = int(time.time())
                            local_time = datetime.fromtimestamp(timestamp).strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                            bids = ob.get("bids", [])
                            asks = ob.get("asks", [])

                            best_bid = float(bids[0].get("price")) if bids else None
                            best_ask = float(asks[0].get("price")) if asks else None
                            bid_depth = sum(float(b.get("size", 0)) for b in bids)
                            ask_depth = sum(float(a.get("size", 0)) for a in asks)

                            record = {
                                "timestamp": timestamp,
                                "local_time": local_time,
                                "market_slug": new_market.get("slug"),
                                "market_ts": new_market.get("timestamp"),
                                "market_start_local": new_market.get("local_start"),
                                "market_end_local": new_market.get("local_end"),
                                "interval_min": interval_minutes,
                                "token_id": token_id,
                                "best_bid": best_bid,
                                "best_ask": best_ask,
                                "bid_depth": bid_depth,
                                "ask_depth": ask_depth,
                                "spread": best_ask - best_bid
                                if best_bid and best_ask
                                else None,
                            }
                            orderbook_records.append(record)

                            if len(orderbook_records) % 10 == 0:
                                df = pd.DataFrame(orderbook_records)
                                df.to_csv(output_file, index=False, float_format="%.8f")
                    except Exception as e:
                        logger.error(f"Error fetching orderbook for {token_id}: {e}")
            else:
                logger.warning("No active market found - waiting for next market...")
                time.sleep(poll_interval_seconds)
                continue

            elapsed = time.time() - start_time
            if duration_seconds:
                logger.info(
                    f"Snapshots: {len(orderbook_records)} | Market switches: {market_switch_count} | Time: {elapsed:.0f}s / {duration_seconds}s"
                )
            else:
                logger.info(
                    f"Snapshots: {len(orderbook_records)} | Market switches: {market_switch_count} | Time: {elapsed:.0f}s"
                )

            time.sleep(poll_interval_seconds)

        df = pd.DataFrame(orderbook_records)
        df.to_csv(output_file, index=False, float_format="%.8f")
        logger.info(
            f"Finished! Collected {len(orderbook_records)} snapshots to {output_file}"
        )
        return orderbook_records

    def export_to_csv(self, output_dir: str = "data") -> None:
        """Export collected data to CSV files."""
        os.makedirs(output_dir, exist_ok=True)

        if self.markets_data:
            markets_df = pd.DataFrame(self.markets_data)
            markets_df.to_csv(os.path.join(output_dir, "markets.csv"), index=False)
            logger.info(f"Exported {len(markets_df)} markets to markets.csv")

        if self.prices_data:
            prices_df = pd.DataFrame(self.prices_data)
            prices_df.to_csv(os.path.join(output_dir, "prices.csv"), index=False)
            logger.info(f"Exported {len(prices_df)} price records to prices.csv")

        if self.trades_data:
            trades_df = pd.DataFrame(self.trades_data)
            trades_df.to_csv(os.path.join(output_dir, "trades.csv"), index=False)
            logger.info(f"Exported {len(trades_df)} trades to trades.csv")

        if self.orderbook_data:
            orderbook_df = pd.DataFrame(self.orderbook_data)
            orderbook_df.to_csv(os.path.join(output_dir, "orderbook.csv"), index=False)
            logger.info(
                f"Exported {len(orderbook_df)} orderbook snapshots to orderbook.csv"
            )


class PolymarketRTDS:
    """Real-Time Data Socket client for Polymarket prices.

    WebSocket endpoint: wss://ws-live-data.polymarket.com
    """

    def __init__(self):
        self.ws: Optional[websocket.WebSocketApp] = None
        self._running = False
        self._thread = None
        self._lock = __import__("threading").Lock()

        # Price data - keyed by asset (e.g., "BTC-USD", "ETH-USD")
        self._prices: Dict[str, Dict[str, Any]] = {}

        # Callbacks for price updates
        self._callbacks: List[callable] = []

    def start(self) -> bool:
        """Start the RTDS WebSocket connection."""
        if self._running:
            return True

        self._running = True
        self._thread = __import__("threading").Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("Polymarket RTDS WebSocket started")
        return True

    def stop(self):
        """Stop the WebSocket connection."""
        self._running = False
        if self.ws:
            self.ws.close()
        logger.info("Polymarket RTDS WebSocket stopped")

    def _run(self):
        """Run the WebSocket connection with auto-reconnect."""
        while self._running:
            try:
                self.ws = websocket.WebSocketApp(
                    "wss://ws-live-data.polymarket.com",
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                    on_open=self._on_open,
                )
                self.ws.run_forever(ping_interval=5)
            except Exception as e:
                logger.error(f"RTDS WebSocket error: {e}")

            if self._running:
                logger.info("RTDS WebSocket reconnecting in 5 seconds...")
                time.sleep(5)

    def _on_open(self, ws):
        """Subscribe to crypto prices."""
        ws.send(
            json.dumps(
                {
                    "action": "subscribe",
                    "subscriptions": [{"topic": "crypto_prices", "type": "update"}],
                }
            )
        )
        logger.debug("RTDS subscribed to crypto_prices")

    def _on_message(self, ws, message):
        """Handle incoming messages."""
        try:
            data = json.loads(message)
            if not isinstance(data, dict):
                return

            topic = data.get("topic")
            payload = data.get("payload", {})

            if topic == "crypto_prices" and isinstance(payload, dict):
                for asset, price_data in payload.items():
                    if isinstance(price_data, dict):
                        with self._lock:
                            self._prices[asset] = {
                                "price": price_data.get("price"),
                                "timestamp": price_data.get("timestamp"),
                                "source": price_data.get("source"),
                            }

                        for callback in self._callbacks:
                            try:
                                callback(asset, self._prices[asset])
                            except Exception as e:
                                logger.debug(f"Callback error: {e}")

        except json.JSONDecodeError:
            pass
        except Exception as e:
            logger.debug(f"RTDS message parse error: {e}")

    def _on_error(self, ws, error):
        logger.error(f"RTDS WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        logger.debug(f"RTDS WebSocket closed: {close_status_code} - {close_msg}")

    def get_price(self, asset: str = "BTC-USD") -> Optional[float]:
        """Get current price for an asset."""
        with self._lock:
            price_data = self._prices.get(asset)
            if price_data:
                return price_data.get("price")

    def get_all_prices(self) -> Dict[str, Dict[str, Any]]:
        """Get all cached prices."""
        with self._lock:
            return dict(self._prices)

    def add_callback(self, callback: callable):
        """Add a callback for price updates."""
        self._callbacks.append(callback)


class PolymarketCLOB:
    """CLOB WebSocket client for Polymarket orderbook.

    WebSocket endpoint: wss://ws-subscriptions-clob.polymarket.com/ws/market
    """

    def __init__(self):
        self.ws: Optional[websocket.WebSocketApp] = None
        self._running = False
        self._thread = None
        self._lock = __import__("threading").Lock()

        # Orderbook data - keyed by token_id
        self._orderbooks: Dict[str, Dict[str, Any]] = {}

        # Subscribed tokens
        self._subscribed_tokens: set = set()

        # Last update timestamp for staleness detection
        self._last_update_time: float = 0

    def start(self) -> bool:
        """Start the CLOB WebSocket connection."""
        if self._running:
            return True

        self._running = True
        self._thread = __import__("threading").Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("Polymarket CLOB WebSocket started")
        return True

    def stop(self):
        """Stop the WebSocket connection."""
        self._running = False
        if self.ws:
            self.ws.close()
        logger.info("Polymarket CLOB WebSocket stopped")

    def subscribe(self, token_ids: List[str]):
        """Subscribe to orderbook updates for token IDs."""
        with self._lock:
            new_tokens = set(token_ids) - self._subscribed_tokens
            self._subscribed_tokens.update(new_tokens)

            logger.info(
                f"CLOB subscribe: adding {len(new_tokens)} tokens, total: {len(self._subscribed_tokens)}"
            )

            if new_tokens and self._running and self.ws:
                logger.info(
                    f"CLOB subscribing to {len(new_tokens)} new tokens, reconnecting..."
                )
                self.ws.close()

    def unsubscribe(self, token_ids: List[str]):
        """Unsubscribe from orderbook updates for token IDs."""
        with self._lock:
            removed_tokens = set(token_ids) & self._subscribed_tokens
            self._subscribed_tokens -= removed_tokens

            # Also clean up orderbook data for unsubscribed tokens
            for token_id in removed_tokens:
                if token_id in self._orderbooks:
                    del self._orderbooks[token_id]

            logger.info(
                f"CLOB unsubscribe: removing {len(removed_tokens)} tokens, total: {len(self._subscribed_tokens)}"
            )

            if removed_tokens and self._running and self.ws:
                logger.info(
                    f"CLOB unsubscribing from {len(removed_tokens)} tokens, reconnecting..."
                )
                self.ws.close()

    def unsubscribe_all(self):
        """Unsubscribe from all tokens and clear subscription list."""
        with self._lock:
            token_ids = list(self._subscribed_tokens)
            self._subscribed_tokens.clear()

            # Clean up all orderbook data
            self._orderbooks.clear()

            logger.info(f"CLOB unsubscribe_all: clearing {len(token_ids)} tokens")

            if token_ids and self._running and self.ws:
                logger.info(f"CLOB clearing all subscriptions, reconnecting...")
                self.ws.close()

    def _run(self):
        """Run the WebSocket connection with auto-reconnect."""
        while self._running:
            try:
                self.ws = websocket.WebSocketApp(
                    "wss://ws-subscriptions-clob.polymarket.com/ws/market",
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                    on_open=self._on_open,
                )
                self.ws.run_forever(ping_interval=5)

            except Exception as e:
                logger.error(f"CLOB WebSocket error: {e}")

            if self._running:
                logger.info("CLOB WebSocket reconnecting in 5 seconds...")
                time.sleep(5)

    def _on_open(self, ws):
        """Subscribe to orderbooks."""
        token_ids = list(self._subscribed_tokens)
        logger.info(
            f"CLOB WebSocket opened, subscribing to {len(token_ids)} tokens: {token_ids}"
        )

        msg = json.dumps(
            {
                "assets_ids": token_ids,
                "type": "market",
                "custom_feature_enabled": True,
            }
        )
        ws.send(msg)
        logger.debug(f"CLOB sent subscription message")

    def _on_message(self, ws, message):
        """Handle incoming messages."""
        try:
            data = json.loads(message)
            if not isinstance(data, dict):
                logger.debug(f"CLOB received non-dict message: {type(data)}")
                return

            event_type = data.get("event_type")

            if event_type == "book":
                asset_id = data.get("asset_id")
                if asset_id:
                    with self._lock:
                        self._orderbooks[asset_id] = {
                            "bids": data.get("bids", []),
                            "asks": data.get("asks", []),
                            "timestamp": data.get("timestamp", int(time.time() * 1000)),
                        }
                        self._last_update_time = time.time()
                    logger.log(TRACE, f"CLOB received orderbook for {asset_id[:20]}...")

            elif event_type == "price_change":
                asset_id = data.get("asset_id")
                price = data.get("price")
                if asset_id:
                    with self._lock:
                        if asset_id in self._orderbooks:
                            self._orderbooks[asset_id]["last_price"] = price
                            self._last_update_time = time.time()

            elif event_type == "best_bid_ask":
                asset_id = data.get("asset_id")
                best_bid = data.get("best_bid")
                best_ask = data.get("best_ask")
                if asset_id:
                    with self._lock:
                        if asset_id in self._orderbooks:
                            self._orderbooks[asset_id]["best_bid"] = best_bid
                            self._orderbooks[asset_id]["best_ask"] = best_ask
                            self._last_update_time = time.time()
                    logger.log(
                        TRACE,
                        f"CLOB best_bid_ask for {asset_id[:20]}...: {best_bid}/{best_ask}",
                    )

            elif event_type == "last_trade_price":
                asset_id = data.get("asset_id")
                price = data.get("price")
                if asset_id:
                    with self._lock:
                        if asset_id in self._orderbooks:
                            self._orderbooks[asset_id]["last_price"] = price

            elif event_type == "new_market":
                question = data.get("question", "")
                slug = data.get("slug", "")
                assets_ids = data.get("assets_ids", [])
                timestamp = data.get("timestamp", "")
                logger.debug(
                    f"CLOB new_market: {question} | slug: {slug} | tokens: {assets_ids} | ts: {timestamp}"
                )

            elif data.get("type") == "market":
                logger.debug(f"CLOB market subscription confirmed")

            else:
                logger.debug(
                    f"CLOB unknown event_type: {event_type}, keys: {list(data.keys())}"
                )

        except json.JSONDecodeError:
            logger.debug(f"CLOB received non-JSON message: {message[:200]}")
        except Exception as e:
            logger.debug(f"CLOB message parse error: {e}")

    def _on_error(self, ws, error):
        logger.error(f"CLOB WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        logger.debug(f"CLOB WebSocket closed: {close_status_code} - {close_msg}")

    def get_orderbook(self, token_id: str) -> Optional[Dict[str, Any]]:
        """Get current orderbook for a token."""
        with self._lock:
            return self._orderbooks.get(token_id)

    def get_all_orderbooks(self) -> Dict[str, Dict[str, Any]]:
        """Get all cached orderbooks."""
        with self._lock:
            return dict(self._orderbooks)

    def get_subscription_count(self) -> int:
        """Get number of active subscriptions."""
        with self._lock:
            return len(self._subscribed_tokens)

    def get_last_update_time(self) -> float:
        """Get timestamp of last orderbook update."""
        with self._lock:
            return self._last_update_time
