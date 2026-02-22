"""Polymarket collector for orderbook data."""

import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import websocket

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
