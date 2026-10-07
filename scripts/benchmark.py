import argparse
import sys
import time
from pathlib import Path
from typing import Any, cast

import psutil

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import duckdb
import pandas as pd

from src.config import DEFAULT_DATA_PATH, DNS_SCHEMA_DTYPES


def get_current_memory_mb():
    process = psutil.Process()
    return process.memory_info().rss / (1024**2)


def benchmark_engine(name, run_fn):
    print(f"\n--- Benchmarking: {name} ---")
    mem_before = get_current_memory_mb()
    t0 = time.perf_counter()
    result_count = run_fn()
    elapsed = time.perf_counter() - t0
    mem_after = get_current_memory_mb()
    peak_mem_mb = max(mem_after - mem_before, 0)
    print(
        f"[{name}] Finished in {elapsed:.2f}s | Delta RAM: {peak_mem_mb:.1f} MB | Rows: {result_count:,}"
    )
    return {"engine": name, "time_s": elapsed, "delta_ram_mb": peak_mem_mb, "rows": result_count}


def main():
    parser = argparse.ArgumentParser(description="DNS Telemetry Engine Benchmark")
    parser.add_argument("--csv", type=Path, default=DEFAULT_DATA_PATH, help="Input CSV path")
    parser.add_argument(
        "--parquet",
        type=Path,
        default=Path("data/sample-dns-30min.parquet"),
        help="Input Parquet path",
    )
    parser.add_argument("--sample", type=int, default=500000, help="Row count for benchmark")
    args = parser.parse_args()

    csv_path = args.csv
    parquet_path = args.parquet
    limit = args.sample

    print(f"DNS Engine Benchmark Suite (Sample: {limit:,} records)")

    # 1. Pandas C-Engine
    def run_pandas():
        df = pd.read_csv(
            csv_path,
            engine="c",
            dtype=cast(Any, DNS_SCHEMA_DTYPES),
            nrows=limit,
            keep_default_na=False,
            na_values=[""],
        )
        # Aggregation: count by rcode
        df.groupby("rcode").size()
        return len(df)

    res_pandas = benchmark_engine("Pandas (C-Engine CSV)", run_pandas)

    # 2. DuckDB CSV Direct Scan
    def run_duckdb_csv():
        con = duckdb.connect(database=":memory:")
        sql = f"""
        SELECT rcode, count(*) as cnt
        FROM (
            SELECT rcode FROM read_csv('{csv_path.as_posix()}', auto_detect=true)
            LIMIT {limit}
        )
        GROUP BY rcode;
        """
        con.execute(sql).df()
        return limit

    res_duck_csv = benchmark_engine("DuckDB (Direct CSV Scan)", run_duckdb_csv)

    # 3. DuckDB Parquet Scan (if parquet exists, or create a small parquet sample)
    if not parquet_path.is_file():
        print(f"\nGenerating benchmark sample Parquet ({parquet_path})...")
        con = duckdb.connect(database=":memory:")
        con.execute(f"""
        COPY (
            SELECT * FROM read_csv('{csv_path.as_posix()}', auto_detect=true)
            LIMIT {limit}
        ) TO '{parquet_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)

    def run_duckdb_parquet():
        con = duckdb.connect(database=":memory:")
        sql = f"""
        SELECT rcode, count(*) as cnt
        FROM read_parquet('{parquet_path.as_posix()}')
        GROUP BY rcode;
        """
        con.execute(sql).df()
        return limit

    res_duck_parquet = benchmark_engine("DuckDB (Columnar Parquet)", run_duckdb_parquet)

    print("\nBenchmark Results Summary:")
    results = [res_pandas, res_duck_csv, res_duck_parquet]
    baseline_time = res_pandas["time_s"]

    print("| Engine | Waktu (Detik) | Delta RAM (MB) | Kecepatan Relatif |")
    print("|---|---|---|---|")
    for r in results:
        speedup = baseline_time / r["time_s"] if r["time_s"] > 0 else 1.0
        print(
            f"| {r['engine']} | {r['time_s']:.2f}s | {r['delta_ram_mb']:.1f} MB | **{speedup:.1f}x lebih cepat** |"
        )


if __name__ == "__main__":
    main()
