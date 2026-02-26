"""Parquet exporter for Redis stream data with Hive-style partitioning."""

import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class ParquetExporter:
    """Exports Redis stream data to partitioned Parquet files."""

    def __init__(self, output_dir: str = "./data"):
        self.output_dir = output_dir

    def export_table(
        self,
        redis_manager,
        table_name: str,
        minute_key: int,
    ) -> bool:
        """Export a table's data for a specific minute to Parquet.

        Args:
            redis_manager: RedisManager instance
            table_name: Table/stream name (e.g., "polymarket_prices")
            minute_key: Minute key (hour * 60 + minute) - exports data from (minute_key-1) to minute_key

        Returns:
            True if export succeeded, False otherwise
        """
        stream_key = f"data:{table_name}"

        # Calculate 10-minute window
        # minute_key = hour * 60 + minute
        # At minute 10, export minutes 0-9 (10 minutes)
        # At minute 20, export minutes 10-19 (10 minutes)
        # start of 10-minute window: ((minute_key - 1) // 10) * 10
        export_start_minute = ((minute_key - 1) // 10) * 10
        if export_start_minute < 0:
            export_start_minute = 1439  # Previous day minute 23:59

        export_hour = export_start_minute // 60
        export_min = export_start_minute % 60

        # Get current date from localtime
        current_struct = time.localtime()
        current_hour = current_struct.tm_hour
        current_min = current_struct.tm_min

        # If we're exporting a minute from a previous hour/day, adjust accordingly
        if export_hour > current_hour:
            # Previous day
            yesterday = datetime.now(timezone.utc) - timedelta(days=1)
            start_dt = datetime(
                yesterday.year,
                yesterday.month,
                yesterday.day,
                export_hour,
                export_min,
                0,
                tzinfo=timezone.utc,
            )
        else:
            # Same day
            today = datetime.now(timezone.utc).date()
            start_dt = datetime(
                today.year,
                today.month,
                today.day,
                export_hour,
                export_min,
                0,
                tzinfo=timezone.utc,
            )

        # End time is start + 10 minutes
        end_dt = start_dt + timedelta(minutes=10)

        epoch_start_ms = int(start_dt.timestamp() * 1000)
        epoch_end_ms = int(end_dt.timestamp() * 1000)

        logger.debug(
            f"Exporting {table_name} for {export_hour:02d}:{export_min:02d}-{export_min + 10:02d} - "
            f"range: {epoch_start_ms} to {epoch_end_ms}"
        )

        entries = self._read_stream_range(
            redis_manager, stream_key, epoch_start_ms, epoch_end_ms
        )

        if not entries:
            logger.info(f"No data to export for {table_name} minute {minute_key}")
            return True

        df = self._to_dataframe(entries, minute_key)
        if df.empty:
            logger.warning(f"DataFrame empty for {table_name} minute {minute_key}")
            return True

        return self._write_partitioned(df, table_name, start_dt)

    def _read_stream_range(
        self,
        redis_manager,
        stream_key: str,
        start_ms: int,
        end_ms: int,
    ) -> List[Dict[str, Any]]:
        """Read entries from a stream within a time range."""
        start_id = f"({start_ms}-0"
        end_id = f"({end_ms}-0"

        try:
            entries = redis_manager.xrange(stream_key, start_id, end_id, count=10000)
        except Exception as e:
            logger.error(f"Error reading stream {stream_key}: {e}")
            return []

        logger.debug(f"Read {len(entries)} entries from {stream_key}")
        return entries

    def _to_dataframe(
        self, entries: List[Dict[str, Any]], minute_key: int
    ) -> pd.DataFrame:
        """Convert stream entries to DataFrame with partition columns."""
        if not entries:
            return pd.DataFrame()

        df = pd.DataFrame(entries)

        if "_id" in df.columns:
            df = df.drop(columns=["_id"])

        # Ensure timestamp_utc exists - try different sources
        timestamp_value = None
        if "timestamp" in df.columns:
            timestamp_value = df["timestamp"]
        elif "source_timestamp" in df.columns:
            timestamp_value = df["source_timestamp"]

        if timestamp_value is not None:
            df["timestamp_utc"] = pd.to_numeric(
                timestamp_value, errors="coerce"
            ).astype("int64")
            dt = pd.to_datetime(df["timestamp_utc"], unit="ms", utc=True)
            df["year"] = dt.dt.year
            df["month"] = dt.dt.month
            df["day"] = dt.dt.day
            df["hour"] = dt.dt.hour
            df["minute"] = dt.dt.minute
        else:
            # No timestamp found - create dummy partition columns
            df["year"] = 0
            df["month"] = 1
            df["day"] = 1
            df["hour"] = 0
            df["minute"] = 0
            df["timestamp_utc"] = 0

        # Handle chart_timestamp_utc for polymarket_stream (already in ms)
        if "chart_timestamp_utc" in df.columns:
            df["chart_timestamp_utc"] = pd.to_numeric(
                df["chart_timestamp_utc"], errors="coerce"
            ).astype("int64")

        # Drop the original timestamp string column - we have timestamp_utc now
        df = df.drop(columns=["timestamp"], errors="ignore")

        # Convert all remaining string columns to numeric
        skip_cols = {
            "year",
            "month",
            "day",
            "hour",
            "minute",
            "minute_key",
            "timestamp_utc",
            "chart_timestamp_utc",
        }
        # Columns that should be int64 (not float64)
        int64_keywords = ["epoch", "timestamp", "direction"]
        for col in df.columns:
            if col in skip_cols:
                continue
            if df[col].dtype == object or df[col].dtype == "string":
                numeric_col = pd.to_numeric(df[col], errors="coerce")
                if not numeric_col.isna().all():
                    col_lower = col.lower()
                    # Use int64 for timestamp/epoch/direction columns, float64 for prices/sizes
                    if any(kw in col_lower for kw in int64_keywords):
                        df[col] = numeric_col.astype("int64")
                    else:
                        df[col] = numeric_col.astype("float64")

        # Remove partition columns and minute_key - not needed in final output
        cols_to_remove = {"year", "month", "day", "hour", "minute", "minute_key"}
        df = df.drop(
            columns=[c for c in cols_to_remove if c in df.columns], errors="ignore"
        )

        return df

    def _write_partitioned(
        self, df: pd.DataFrame, table_name: str, start_dt: datetime
    ) -> bool:
        """Write DataFrame to partitioned Parquet file."""
        try:
            year = start_dt.year
            month = start_dt.month
            day = start_dt.day
            hour = start_dt.hour

            partition_path = os.path.join(
                self.output_dir,
                table_name,
                f"year={year}",
                f"month={month:02d}",
                f"day={day:02d}",
                f"hour={hour:02d}",
            )

            os.makedirs(partition_path, exist_ok=True)

            # Filename uses START minute: _HH00, _HH10, _HH20, etc.
            filename = f"{table_name}_{start_dt.strftime('%Y-%m-%d_%H')}{start_dt.minute:02d}.parquet"
            filepath = os.path.join(partition_path, filename)

            table = pa.Table.from_pandas(df)
            pq.write_table(
                table,
                filepath,
                compression="snappy",
            )
            logger.info(f"Exported {len(df)} rows to {filepath}")
            return True
        except Exception as e:
            logger.error(f"Failed to write Parquet file: {e}")
            return False

    def export_all_tables(
        self,
        redis_manager,
        table_names: List[str],
        minute_key: int,
    ) -> Dict[str, bool]:
        """Export all tables for a specific minute."""
        results = {}
        for table_name in table_names:
            success = self.export_table(redis_manager, table_name, minute_key)
            results[table_name] = success
        return results
