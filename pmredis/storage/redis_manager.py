"""Redis manager for stream storage with 10-minute rolling window."""

import json
import logging
import time
from typing import Any, Dict, List, Optional

import redis

logger = logging.getLogger(__name__)

DEFAULT_STREAM_KEY = "data:{table_name}"
TEN_MINUTES_MS = 15 * 60 * 1000  # 15 minutes in milliseconds (for export buffer)


class RedisManager:
    """Manages Redis connections and stream operations."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        max_memory_mb: int = 256,
        rolling_window_ms: int = TEN_MINUTES_MS,
        dry_run: bool = False,
    ):
        self.host = host
        self.port = port
        self.db = db
        self.rolling_window_ms = rolling_window_ms
        self._client: Optional[redis.Redis] = None
        self._max_memory_mb = max_memory_mb
        self.dry_run = dry_run

    def connect(self) -> bool:
        """Connect to Redis server."""
        if self.dry_run:
            logger.info(
                f"[DRY-RUN] Skipping Redis connection to {self.host}:{self.port}"
            )
            return True

        try:
            self._client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                decode_responses=True,
            )
            self._client.ping()
            logger.info(f"Connected to Redis at {self.host}:{self.port}")
            self._configure()
            return True
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self._client = None
            return False

    def _configure(self) -> None:
        """Configure Redis for memory management."""
        if self._client is None:
            return
        try:
            self._client.config_set("maxmemory", f"{self._max_memory_mb}mb")
            self._client.config_set("maxmemory-policy", "allkeys-lru")
        except redis.RedisError as e:
            logger.warning(f"Failed to configure Redis: {e}")

    def is_connected(self) -> bool:
        """Check if Redis is connected."""
        if self.dry_run:
            return True

        if self._client is None:
            return False
        try:
            self._client.ping()
            return True
        except redis.ConnectionError:
            return False

    def reconnect(self) -> bool:
        """Attempt to reconnect to Redis."""
        return self.connect()

    def xadd(self, stream_key: str, data: Dict[str, Any]) -> Optional[str]:
        """Add a record to a stream.

        Args:
            stream_key: Redis stream key (e.g., "data:polymarket_orderbook")
            data: Dictionary of field-value pairs

        Returns:
            Message ID if successful, None otherwise
        """
        if self.dry_run:
            fields = " | ".join(f"{k}: {v}" for k, v in data.items())
            logger.info(f"[DRY-RUN] {stream_key} | {fields}")
            return "dry-run-msg-id"

        if not self.is_connected():
            if not self.reconnect():
                return None

        try:
            msg_id = self._client.xadd(stream_key, data)
            return msg_id
        except redis.RedisError as e:
            logger.error(f"Failed to add to stream {stream_key}: {e}")
            return None

    def xtrim(self, stream_key: str, max_len: int = None) -> int:
        """Trim stream to keep only recent entries.

        Args:
            stream_key: Redis stream key
            max_len: Maximum number of entries (default: calculate from rate)

        Returns:
            Number of entries removed, or -1 on error
        """
        if self.dry_run:
            return 0

        if not self.is_connected():
            return -1

        if max_len is None:
            # Estimate based on 10-minute window and expected rates
            # polymarket_orderbook: 4Hz * 600s = 2400
            # polymarket_orderbook_next: 2Hz * 600s = 1200
            # binance_orderbook: 4Hz * 600s = 2400
            # etc.
            max_len = 3000  # Safe upper bound

        try:
            # Use ~ to allow some overage before trimming
            return self._client.xtrim(stream_key, maxlen=max_len, limit=max_len // 10)
        except redis.RedisError as e:
            logger.error(f"Failed to trim stream {stream_key}: {e}")
            return -1

    def trim_older_than(self, stream_key: str, window_seconds: int = 900) -> int:
        """Trim entries older than window_seconds using time-based MINID.

        Args:
            stream_key: Redis stream key
            window_seconds: Keep entries newer than this (default: 900s = 15min)

        Returns:
            Number of entries removed, or -1 on error
        """
        if self.dry_run:
            return 0

        if not self.is_connected():
            return -1

        cutoff_ms = (time.time() - window_seconds) * 1000
        cutoff_id = f"{int(cutoff_ms)}-0"

        try:
            removed = self._client.xtrim(stream_key, minid=cutoff_id)
            return removed
        except redis.RedisError as e:
            logger.error(f"Failed to trim stream {stream_key}: {e}")
            return -1

    def xlen(self, stream_key: str) -> int:
        """Get the number of entries in a stream."""
        if self.dry_run:
            return 0

        if not self.is_connected():
            return 0
        try:
            return self._client.xlen(stream_key)
        except redis.RedisError:
            return 0

    def xread(
        self, stream_key: str, count: int = 100, last_id: str = "0"
    ) -> List[Dict[str, Any]]:
        """Read entries from a stream.

        Args:
            stream_key: Redis stream key
            count: Maximum number of entries to read
            last_id: Last entry ID to read from (default: "0" for new entries)

        Returns:
            List of entries as dictionaries
        """
        if self.dry_run:
            return []

        if not self.is_connected():
            return []

        try:
            result = self._client.xread({stream_key: last_id}, count=count)
            if not result:
                return []

            entries = []
            for stream, messages in result:
                for msg_id, fields in messages:
                    entry = {"_id": msg_id}
                    entry.update(fields)
                    entries.append(entry)
            return entries
        except redis.RedisError as e:
            logger.error(f"Failed to read from stream {stream_key}: {e}")
            return []

    def xrange(
        self, stream_key: str, start: str = "-", end: str = "+", count: int = 100
    ) -> List[Dict[str, Any]]:
        """Get range of entries from a stream.

        Args:
            stream_key: Redis stream key
            start: Start ID (default: "-" for earliest)
            end: End ID (default: "+" for latest)
            count: Maximum entries to return

        Returns:
            List of entries as dictionaries
        """
        if self.dry_run:
            return []

        if not self.is_connected():
            return []

        try:
            result = self._client.xrange(stream_key, start, end, count)
            entries = []
            for msg_id, fields in result:
                entry = {"_id": msg_id}
                entry.update(fields)
                entries.append(entry)
            return entries
        except redis.RedisError as e:
            logger.error(f"Failed to range stream {stream_key}: {e}")
            return []

    def get_stream_info(self, stream_key: str) -> Dict[str, Any]:
        """Get information about a stream."""
        if not self.is_connected():
            return {}

        try:
            info = self._client.xinfo_stream(stream_key)
            return {
                "length": info.get("length", 0),
                "first_entry": info.get("first-entry", []),
                "last_entry": info.get("last-entry", []),
                "radix-tree-keys": info.get("radix-tree-keys", 0),
            }
        except redis.RedisError:
            return {}

    def close(self) -> None:
        """Close Redis connection."""
        if self.dry_run:
            logger.info("[DRY-RUN] Skipping Redis close")
            return

        if self._client:
            try:
                self._client.close()
            except redis.RedisError:
                pass
            finally:
                self._client = None


def get_stream_key(table_name: str) -> str:
    """Get the Redis stream key for a table."""
    return f"data:{table_name}"
