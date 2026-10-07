"""Generate a minimal synthetic Parquet dataset for CI environments and local testing.

Ensures CI runners without access to the full 2.28 GB raw dataset can execute
dbt run, dbt test, and analytics benchmarks seamlessly in sub-seconds.
"""

import argparse
import logging
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("generate_sample")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_PARQUET = PROJECT_ROOT / "data" / "sample-dns-30min.parquet"


def generate_sample_parquet(output_path: Path, n_rows: int = 2000, force: bool = False):
    """Generate synthetic DNS telemetry and save as compressed Parquet."""
    out_file = Path(output_path)
    if out_file.exists() and not force:
        logger.info(f"Target file already exists: {out_file} (skipping generation).")
        return

    out_file.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Generating {n_rows:,} synthetic DNS telemetry records...")

    rng = np.random.default_rng(42)
    base_ts = 1787126400  # 2026-08-19 08:00:00 UTC
    timestamps = [base_ts + int(rng.uniform(0, 1800)) for _ in range(n_rows)]

    # Subnets: S1 dominant (50%), S2 (20%), others
    subnets = [
        "2001:db8:aaaa",
        "2001:db8:bbbb",
        "2001:db8:cccc",
        "2001:db8:dddd",
    ]
    subnet_choices = rng.choice(subnets, size=n_rows, p=[0.5, 0.25, 0.15, 0.10])
    src_ips = [f"{s}:{rng.integers(1, 9999):x}::1" for s in subnet_choices]
    dst_ips = [f"2001:db8:ffff::{rng.integers(1, 10):x}" for _ in range(n_rows)]

    qtypes = rng.choice([1, 28, 255], size=n_rows, p=[0.7, 0.25, 0.05])
    qtype_map = {1: "A", 28: "AAAA", 255: "ANY"}
    qtype_names = [qtype_map[q] for q in qtypes]

    qrs = rng.choice([0, 1], size=n_rows, p=[0.5, 0.5])
    rcodes = []
    for qr in qrs:
        if qr == 0:
            rcodes.append(None)
        else:
            rcodes.append(rng.choice([0, 3, 2], p=[0.88, 0.10, 0.02]))

    protos = rng.choice(["udp", "tcp"], size=n_rows, p=[0.95, 0.05])
    frame_lens = rng.integers(64, 1400, size=n_rows)
    dns_lens = [max(fl - 40, 20) for fl in frame_lens]

    # Truncation if UDP response and large frame
    tcs = [
        1 if (qr == 1 and proto == "udp" and fl > 1232 and rng.random() < 0.6) else 0
        for qr, proto, fl in zip(qrs, protos, frame_lens, strict=False)
    ]

    qnames = [
        "example.com." if rc == 0 else f"nonexistent-{i}.example.com."
        for i, rc in enumerate(rcodes)
    ]

    df = pd.DataFrame(
        {
            "ts": timestamps,
            "ip_ver": [6] * n_rows,
            "proto": protos,
            "src_ip": src_ips,
            "dst_ip": dst_ips,
            "src_port": rng.integers(1024, 65535, size=n_rows),
            "dst_port": [53] * n_rows,
            "frame_len": frame_lens,
            "dns_len": dns_lens,
            "dns_id": rng.integers(1, 65535, size=n_rows),
            "qr": qrs,
            "opcode": [0] * n_rows,
            "aa": [0] * n_rows,
            "tc": tcs,
            "rd": [1] * n_rows,
            "ra": [1] * n_rows,
            "ad": [0] * n_rows,
            "cd": [0] * n_rows,
            "rcode": rcodes,
            "qdcount": [1] * n_rows,
            "ancount": [
                1 if rc == 0 and qr == 1 else 0 for qr, rc in zip(qrs, rcodes, strict=False)
            ],
            "nscount": [0] * n_rows,
            "arcount": [1] * n_rows,
            "qname": qnames,
            "qtype": qtypes,
            "qtype_name": qtype_names,
            "qclass": [1] * n_rows,
            "edns": [1] * n_rows,
            "edns_udpsize": [1232] * n_rows,
            "do": [0] * n_rows,
        }
    )

    con = duckdb.connect(database=":memory:")
    con.register("df_view", df)
    con.execute(f"COPY df_view TO '{out_file.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);")
    con.close()

    size_kb = out_file.stat().st_size / 1024
    logger.info(f"Sample Parquet dataset written to {out_file} ({size_kb:.1f} KB).")


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic DNS Parquet for testing & CI")
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT_PARQUET, help="Output Parquet path"
    )
    parser.add_argument("--rows", type=int, default=2000, help="Number of synthetic records")
    parser.add_argument("--force", action="store_true", help="Force overwrite existing dataset")
    args = parser.parse_args()

    generate_sample_parquet(args.output, n_rows=args.rows, force=args.force)


if __name__ == "__main__":
    main()
