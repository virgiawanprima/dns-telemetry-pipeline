"""DuckDB Engine & Columnar Parquet Processing for DNS Telemetry."""

import logging
import time
from pathlib import Path
from typing import Any

import duckdb

logger = logging.getLogger("dns_pipeline.duck_engine")


def convert_csv_to_parquet(
    csv_path: Path, parquet_path: Path, compression: str = "zstd"
) -> dict[str, Any]:
    """Convert raw DNS telemetry CSV to optimized compressed Parquet via DuckDB.

    Streams data through DuckDB's vectorized parser, reducing disk space and
    providing 10-50x faster columnar scans.
    """
    csv_file = Path(csv_path)
    parquet_file = Path(parquet_path)
    parquet_file.parent.mkdir(parents=True, exist_ok=True)

    if not csv_file.is_file():
        raise FileNotFoundError(f"Source CSV file not found: {csv_file}")

    start_time = time.perf_counter()
    raw_size_mb = csv_file.stat().st_size / (1024**2)
    logger.info(f"Converting {csv_file.name} ({raw_size_mb:.1f} MB) to Parquet ({compression})...")

    con = duckdb.connect(database=":memory:")
    # Stream CSV to Parquet using DuckDB's multi-threaded COPY
    copy_sql = f"""
    COPY (
        SELECT *
        FROM read_csv(
            '{csv_file.as_posix()}',
            header = true,
            auto_detect = true,
            null_padding = true
        )
    ) TO '{parquet_file.as_posix()}' (
        FORMAT PARQUET,
        COMPRESSION '{compression}'
    );
    """
    con.execute(copy_sql)
    con.close()

    elapsed = time.perf_counter() - start_time
    parquet_size_mb = parquet_file.stat().st_size / (1024**2)
    compression_ratio = (1.0 - (parquet_size_mb / raw_size_mb)) * 100.0

    stats = {
        "raw_size_mb": raw_size_mb,
        "parquet_size_mb": parquet_size_mb,
        "compression_ratio_pct": compression_ratio,
        "elapsed_seconds": elapsed,
        "output_path": str(parquet_file),
    }

    logger.info(
        f"Parquet conversion complete in {elapsed:.2f}s: "
        f"{raw_size_mb:.1f} MB -> {parquet_size_mb:.1f} MB "
        f"({compression_ratio:.1f}% space savings)"
    )
    return stats


def run_duckdb_aggregations(source_path: Path) -> dict[str, Any]:
    """Execute high-speed columnar analytical queries on DNS data via DuckDB."""
    src = Path(source_path)
    read_fn = (
        f"read_parquet('{src.as_posix()}')"
        if src.suffix == ".parquet"
        else f"read_csv('{src.as_posix()}', auto_detect=true)"
    )

    start_time = time.perf_counter()
    con = duckdb.connect(database=":memory:")

    # 1. Global KPIs
    kpi_sql = f"""
    SELECT
        count(*) as total_packets,
        count(CASE WHEN qr = 0 THEN 1 END) as total_queries,
        count(CASE WHEN qr = 1 THEN 1 END) as total_responses,
        count(CASE WHEN rcode = 3 THEN 1 END) as total_nxdomain,
        count(CASE WHEN proto = 'udp' AND qr = 1 THEN 1 END) as udp_responses,
        count(CASE WHEN proto = 'udp' AND qr = 1 AND tc = 1 THEN 1 END) as udp_truncated
    FROM {read_fn};
    """
    kpis = con.execute(kpi_sql).df().iloc[0].to_dict()

    # 2. Minute Aggregation
    minute_sql = f"""
    SELECT
        date_trunc('minute', to_timestamp(ts)) as minute,
        count(*) as packets,
        count(CASE WHEN qr = 0 THEN 1 END) as queries,
        count(CASE WHEN qr = 1 THEN 1 END) as responses,
        count(CASE WHEN rcode = 3 THEN 1 END) as nx,
        count(CASE WHEN proto = 'udp' AND qr = 1 THEN 1 END) as udp_resp,
        count(CASE WHEN proto = 'udp' AND qr = 1 AND tc = 1 THEN 1 END) as udp_tc
    FROM {read_fn}
    GROUP BY 1
    ORDER BY 1;
    """
    temporal_df = con.execute(minute_sql).df()

    # 3. Top Subnet Prefixes
    prefix_sql = f"""
    SELECT
        regexp_extract(src_ip, '^([0-9a-fA-F:]+:[0-9a-fA-F:]+:[0-9a-fA-F:]+)', 1) as prefix_48,
        count(*) as query_count
    FROM {read_fn}
    WHERE qr = 0 AND src_ip IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10;
    """
    prefix_df = con.execute(prefix_sql).df()

    # 4. Truncation per QTYPE
    trunc_sql = f"""
    SELECT
        qtype_name,
        count(*) as total_resp,
        count(CASE WHEN tc = 1 THEN 1 END) as tc_count,
        round(count(CASE WHEN tc = 1 THEN 1 END) * 100.0 / count(*), 2) as tc_rate_pct
    FROM {read_fn}
    WHERE proto = 'udp' AND qr = 1
    GROUP BY 1
    HAVING count(*) >= 500
    ORDER BY tc_rate_pct DESC;
    """
    trunc_df = con.execute(trunc_sql).df()

    con.close()
    elapsed = time.perf_counter() - start_time

    return {
        "kpis": kpis,
        "temporal_df": temporal_df,
        "prefix_df": prefix_df,
        "truncation_df": trunc_df,
        "query_time_seconds": elapsed,
    }
