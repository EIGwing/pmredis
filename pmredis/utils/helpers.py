"""Utility functions for PMDownloader."""

import logging
from datetime import datetime
from typing import List, Optional

logger = logging.getLogger(__name__)


def format_timestamp(ts: int) -> str:
    """Format Unix timestamp to human-readable string."""
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def get_price_at_cumulative_size(orders: List, target_size: float) -> Optional[float]:
    """
    Get the price at which cumulative size reaches target.

    Args:
        orders: List of [price, size] pairs
        target_size: Target cumulative size

    Returns:
        Price at target cumulative size, or None if not reached
    """
    if not orders:
        return None

    cumsum = 0.0
    for price, size in orders:
        cumsum += float(size)
        if cumsum >= target_size:
            return float(price)
    return float(orders[-1][0]) if orders else None


def get_impact_price_at_size(orders: List, target_btc: float) -> Optional[float]:
    """
    Get the worst price after absorbing target BTC from orderbook.

    For asks ( sells), this walks from lowest ask upward.
    For bids, this walks from highest bid downward.

    Args:
        orders: List of [price, size] pairs (should be sorted: asks low->high, bids high->low)
        target_btc: Target BTC size to absorb

    Returns:
        Worst price after absorbing target BTC, or None if orderbook is too thin
    """
    if not orders:
        return None

    cumsum = 0.0
    last_price = None
    for price, size in orders:
        cumsum += float(size)
        last_price = float(price)
        if cumsum >= target_btc:
            return last_price

    # Not enough liquidity
    return None if cumsum < target_btc else last_price


def get_cumulative_size_at_price_offset_bps(
    orders: List, mid_price: float, bps: float, direction: str = "up"
) -> float:
    """
    Get cumulative size from mid price out to ±bps%.

    Args:
        orders: List of [price, size] pairs (Binance: bids high->low, asks low->high)
        mid_price: The reference mid price
        bps: Basis points to move from mid (e.g., 0.01 = 1%)
        direction: "up" for asks (price increases), "down" for bids (price decreases)

    Returns:
        Cumulative BTC size from mid to target price level
    """
    if not orders or mid_price <= 0:
        return 0.0

    price_offset = mid_price * bps
    if direction == "up":
        # For asks: sorted low->high, iterate from lowest ask upward
        target_price = mid_price + price_offset
        cumsum = 0.0
        for price, size in orders:
            p = float(price)
            if p <= target_price:
                cumsum += float(size)
            else:
                break
        return cumsum
    else:
        # For bids: already sorted high->low (best first), iterate in order
        target_price = mid_price - price_offset
        cumsum = 0.0
        for price, size in orders:
            p = float(price)
            if p >= target_price:
                cumsum += float(size)
            else:
                break
        return cumsum


BTC_SIZE_BUCKETS = [1, 2, 3, 5, 7, 10, 15, 20, 30, 50]

BPS_BUCKETS = [
    0.001,
    0.005,
    0.01,
    0.02,
    0.03,
    0.04,
    0.05,
]

BTC_IMPACT_SIZES = [0.1, 0.5, 1, 2, 5, 10, 20, 50, 100, 200]

BPS_LEVELS = [1, 5, 10, 20, 30, 50, 75, 100, 150, 200]

BPS_DECIMALS = [b / 10000 for b in BPS_LEVELS]


SIZE_BUCKETS = [
    0.1,
    0.2,
    0.3,
    0.4,
    0.5,
    0.6,
    0.7,
    0.8,
    0.9,
    1.0,
    2.0,
    3.0,
    4.0,
    5.0,
    6.0,
    7.0,
    8.0,
    9.0,
    10,
    15,
    20,
    25,
    30,
    35,
    40,
    45,
    50,
    60,
    70,
    80,
    90,
    100,
    200,
    300,
    400,
    500,
    float("inf"),
]
