"""Unit tests for DuckDB engine and parquet aggregations."""

from pathlib import Path

import pandas as pd

from src.duck_engine import convert_csv_to_parquet, run_duckdb_aggregations


def test_duckdb_parquet_conversion_and_aggregations(sample_dns_df: pd.DataFrame, tmp_path: Path):
    """Verify DuckDB CSV-to-Parquet conversion and columnar analytical queries."""
    csv_file = tmp_path / "test_dns.csv"
    parquet_file = tmp_path / "test_dns.parquet"

    # Write test CSV
    sample_dns_df.to_csv(csv_file, index=False)
    assert csv_file.exists()

    # Convert to Parquet via DuckDB engine
    stats = convert_csv_to_parquet(csv_file, parquet_file, compression="zstd")
    assert parquet_file.exists()
    assert stats["output_path"] == str(parquet_file)
    assert "parquet_size_mb" in stats

    # Run aggregations on generated Parquet
    results = run_duckdb_aggregations(parquet_file)
    assert "kpis" in results
    assert "temporal_df" in results
    assert "prefix_df" in results
    assert "query_time_seconds" in results

    kpis = results["kpis"]
    assert kpis["total_packets"] == len(sample_dns_df)
    assert kpis["total_queries"] > 0
    assert kpis["total_responses"] > 0
