"""High-throughput Ingestion and Schema Validation for DNS Telemetry Data."""

import logging
import time
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.config import DNS_SCHEMA_DTYPES

logger = logging.getLogger("dns_pipeline.loader")


def load_dns_telemetry(
    file_path: Path, sample_size: int | None = None
) -> tuple[pd.DataFrame, dict]:
    """Load raw DNS telemetry CSV into memory using vectorized C-engine parser.

    Applies memory-efficient schema types and preserves literal domain strings.

    Args:
        file_path: Path to the DNS CSV trace file.
        sample_size: Optional row limit for fast sampling or integration tests.

    Returns:
        Tuple of (Loaded DataFrame, loading metadata dictionary).
    """
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"DNS telemetry file not found: {path}")

    start_time = time.perf_counter()
    file_size_gb = path.stat().st_size / (1024**3)
    logger.info(f"Ingesting {path.name} ({file_size_gb:.2f} GB)...")

    # keep_default_na=False avoids literal domains like 'nan' turning into NaN
    df = pd.read_csv(
        path,
        engine="c",
        dtype=cast(Any, DNS_SCHEMA_DTYPES),
        keep_default_na=False,
        na_values=[""],
        nrows=sample_size,
    )

    # Parse timestamps to UTC: numeric epoch 'ts' with unit='s' is ~50x faster than ISO string parsing
    if "ts" in df.columns:
        df["dt"] = pd.to_datetime(df["ts"], unit="s", utc=True)
    elif "ts_iso" in df.columns:
        df["dt"] = pd.to_datetime(df["ts_iso"], format="ISO8601", utc=True)
    else:
        raise ValueError("Dataset missing 'ts' and 'ts_iso' timestamp columns.")

    elapsed = time.perf_counter() - start_time
    mem_usage_bytes = df.memory_usage(deep=True).sum()
    mem_usage_gib = mem_usage_bytes / (1024**3)
    throughput_rps = len(df) / elapsed if elapsed > 0 else 0

    stats = {
        "rows": len(df),
        "columns": len(df.columns),
        "elapsed_seconds": elapsed,
        "throughput_records_per_sec": throughput_rps,
        "memory_usage_gib": mem_usage_gib,
        "source_file_gb": file_size_gb,
    }

    logger.info(
        f"Loaded {len(df):,} records in {elapsed:.2f}s "
        f"({throughput_rps:,.0f} records/s) | Memory footprint: {mem_usage_gib:.2f} GiB"
    )

    return df, stats
