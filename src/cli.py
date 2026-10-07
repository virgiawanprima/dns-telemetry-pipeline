"""DNS Telemetry Pipeline & Operational Analytics CLI.

Processes large-scale authoritative DNS packet traces, computes RFC protocol
compliance and traffic concentration metrics, decomposes error spikes,
and exports structured summary metrics for downstream analytics.

Supported Modern Data Stack capabilities:
- DuckDB columnar queries & Parquet Lakehouse conversion
- Pandera data quality contracts
- dbt Core transformations and data tests
"""

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.anomaly import (
    compute_priority_tables,
    extract_registrable_domain,
    nx_delta,
)
from src.config import (
    DEFAULT_DATA_PATH,
    DEFAULT_OUTPUT_DIR,
)
from src.duck_engine import convert_csv_to_parquet
from src.exporter import export_metrics_summary
from src.loader import load_dns_telemetry
from src.metrics import (
    compute_pareto_prefix_distribution,
    compute_protocol_matrix,
    compute_temporal_aggregates,
    compute_truncation_by_qtype,
    get_subnet_prefix,
)
from src.validation import validate_dns_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("dns_pipeline")


def _floor_minute(series: Any) -> pd.Series:
    """Floor datetime series to 1-minute intervals with robust type casting."""
    return cast(pd.Series, cast(Any, series).dt.floor("1min"))


def run_pipeline(
    input_path: Path,
    output_dir: Path,
    sample_size: int | None = None,
    engine: str = "pandas",
    validate: bool = False,
) -> dict[str, Any]:
    """Execute end-to-end DNS telemetry data engineering pipeline."""
    total_start = time.perf_counter()
    logger.info(f"DNS Telemetry Pipeline initiated (Engine: {engine.upper()})")

    # 1. Ingestion
    df, _load_stats = load_dns_telemetry(input_path, sample_size=sample_size)

    # Optional Pandera validation
    if validate:
        is_valid, _val_report = validate_dns_data(df, sample_size=sample_size or 100000)
        if not is_valid:
            logger.warning("Data contract check reported schema discrepancies.")

    # 2. Temporal Metrics & Steady-State Filtering
    temporal_steady, meta_temporal = compute_temporal_aggregates(
        df, filter_canonical_steady_state=(sample_size is None)
    )

    # 3. Client IP /48 Prefix Pareto Analysis
    pareto_df, anon_map = compute_pareto_prefix_distribution(df, top_n=10)

    # 4. Protocol Matrix & Composition
    _matrix_pct, matrix_meta = compute_protocol_matrix(df, top_qtypes=5)

    # 5. UDP Truncation per QTYPE
    trunc_qtype = compute_truncation_by_qtype(df, min_responses=10 if sample_size else 500)

    # 6. Priority Tables
    logger.info("Generating operational priority tables...")
    q_mask = df["qr"] == 0
    r_mask = df["qr"] == 1
    in_steady = _floor_minute(df["dt"]).isin(temporal_steady.index)

    q_work = df.loc[q_mask & in_steady, ["src_ip", "dt"]].copy()
    q_work["minute"] = _floor_minute(q_work["dt"])
    q_unique_ips = q_work["src_ip"].dropna().unique()
    q_prefix_lookup = {ip: get_subnet_prefix(ip) for ip in q_unique_ips}
    q_work["prefix"] = q_work["src_ip"].map(q_prefix_lookup)

    r_view = df.loc[r_mask & in_steady, ["dst_ip", "rcode", "dt"]].copy()
    r_view["minute"] = _floor_minute(r_view["dt"])
    r_unique_ips = r_view["dst_ip"].dropna().unique()
    r_prefix_lookup = {ip: get_subnet_prefix(ip) for ip in r_unique_ips}
    r_view["prefix"] = r_view["dst_ip"].map(r_prefix_lookup)

    f1_priority, f2_priority = compute_priority_tables(q_work, r_view)

    # 7. NXDOMAIN Burst Decomposition (at peak minute)
    logger.info("Decomposing peak minute error burst...")
    peak_min = meta_temporal["peak_minute"]
    nx_rows = df.loc[df["rcode"] == 3, ["dst_ip", "qname", "qtype_name", "dt"]].copy()
    nx_rows["minute"] = _floor_minute(nx_rows["dt"])

    steady_minute_idx = pd.DatetimeIndex(temporal_steady.index)
    if len(steady_minute_idx) >= 2 and peak_min in steady_minute_idx:
        # Host delta
        delta_host, nx_sum = nx_delta(nx_rows, steady_minute_idx, peak_min, group="dst_ip")
        # Record type delta
        delta_qt, _ = nx_delta(nx_rows, steady_minute_idx, peak_min, group="qtype_name")
        # Registrable domain delta (memoized over unique names for high performance)
        unique_qnames = nx_rows["qname"].dropna().unique()
        reg_lookup = {q: extract_registrable_domain(q) for q in unique_qnames}
        nx_rows["reg_domain"] = nx_rows["qname"].map(reg_lookup)
        delta_reg, _ = nx_delta(nx_rows, steady_minute_idx, peak_min, group="reg_domain")
    else:
        logger.info(
            "Fewer than 2 canonical steady-state minutes; skipping peak-minute delta decomposition."
        )
        delta_host = pd.Series(dtype=float)
        delta_qt = pd.Series(dtype=float)
        delta_reg = pd.Series(dtype=float)
        nx_sum = {
            "group": "dst_ip",
            "n_steady": len(temporal_steady.index),
            "peak_nx": 0,
            "baseline_other_total": 0,
            "baseline_per_minute": 0.0,
            "net_delta": 0.0,
            "recon_delta": 0.0,
            "recon_error": 0.0,
            "top2_share_net_pct": 0.0,
            "top1_group": "",
            "top2_group": "",
        }

    # 8. Assemble Executive Payload
    logger.info("Constructing metrics summary payload...")
    t_min = df["dt"].min()
    t_max = df["dt"].max()
    duration_sec = max((t_max - t_min).total_seconds(), 1.0)
    total_packets = len(df)
    total_q = q_mask.sum()
    total_r = r_mask.sum()
    nx_count = (df["rcode"] == 3).sum()
    nx_rate_all = (nx_count / max(total_r, 1)) * 100.0

    s1_share = f"{pareto_df.loc[0, 'Share (%)']:.1f}%" if len(pareto_df) > 0 else "0.0%"
    kpis = [
        {
            "judul": "Pesan DNS",
            "nilai": f"{total_packets / 1e6:.1f} jt"
            if total_packets >= 1e6
            else f"{total_packets:,}",
            "cakupan": "rekaman penuh",
        },
        {
            "judul": "Pesan / detik",
            "nilai": f"{total_packets / duration_sec:,.0f}",
            "cakupan": "rekaman penuh",
        },
        {"judul": "NXDOMAIN", "nilai": f"{nx_rate_all:.2f}%", "cakupan": "rekaman penuh"},
        {
            "judul": "Truncation UDP",
            "nilai": f"{meta_temporal['tc_ratio_avg']:.2f}%",
            "cakupan": f"{meta_temporal['n_minutes']} menit kanonik",
        },
        {"judul": "Pangsa S1", "nilai": s1_share, "cakupan": "rekaman penuh"},
    ]

    h_map = {str(h): f"H{i + 1}" for i, h in enumerate(delta_host.head(10).index)}
    r_map = {str(r): f"R{i + 1}" for i, r in enumerate(delta_reg.head(5).index)}

    payload = {
        "meta": {
            "judul": "Analisis Operasional & Telemetri DNS Skala Besar",
            "zona_waktu": "UTC",
            "periode_mulai": t_min.isoformat(),
            "periode_akhir": t_max.isoformat(),
            "n_menit_kanonik": int(meta_temporal["n_minutes"]),
            "n_pesan": total_packets,
            "n_query": total_q,
            "n_respons": total_r,
        },
        "kpi": kpis,
        "temporal": {
            "waktu": [m.strftime("%H:%M") for m in temporal_steady.index],
            "waktu_iso": [m.isoformat() for m in temporal_steady.index],
            "packets": temporal_steady["packets"].tolist(),
            "nx": temporal_steady["nx"].tolist(),
            "resp": temporal_steady["responses"].tolist(),
            "nx_rate": [None if pd.isna(v) else float(v) for v in temporal_steady["nx_rate"]],
            "udp_resp": temporal_steady["udp_resp"].tolist(),
            "udp_tc": temporal_steady["udp_tc"].tolist(),
            "tc_ratio": [None if pd.isna(v) else float(v) for v in temporal_steady["tc_ratio"]],
            "median_lain": float(meta_temporal["median_other"]),
            "nx_rate_global": float(meta_temporal["nx_rate_global"]),
            "tc_ratio_avg": float(meta_temporal["tc_ratio_avg"]),
            "puncak": {
                "beban": meta_temporal["peak_minute"].strftime("%H:%M"),
                "nx": meta_temporal["nx_peak_minute"].strftime("%H:%M"),
                "tc": meta_temporal["tc_peak_minute"].strftime("%H:%M"),
            },
        },
        "pareto": {
            "label": [anon_map.get(str(p), str(p)) for p in pareto_df["Subnet Prefix"]],
            "share": [float(v) for v in pareto_df["Share (%)"]],
            "kumulatif": [float(v) for v in pareto_df["Kumulatif (%)"]],
        },
        "prioritas_beban": [
            {
                "subnet": anon_map.get(str(pfx), str(pfx)),
                "query": int(cast(Any, r)["query"]),
                "share": float(cast(Any, r)["share_pct"]),
                "menit": int(cast(Any, r)["menit_aktif"]),
                "alasan": str(cast(Any, r)["Alasan Prioritas"]),
            }
            for pfx, r in f1_priority.iterrows()
        ],
        "prioritas_nx": [
            {
                "subnet": anon_map.get(str(pfx), str(pfx)),
                "respons": int(cast(Any, r)["respons"]),
                "nxdomain": int(cast(Any, r)["nxdomain"]),
                "nx_rate": float(cast(Any, r)["nx_rate_pct"]),
                "alasan": str(cast(Any, r)["Alasan Prioritas"]),
            }
            for pfx, r in f2_priority.iterrows()
        ],
        "bubble": {
            "titik": [
                {
                    "label": anon_map.get(str(pfx), str(pfx)),
                    "query": int(cast(Any, r)["query"]),
                    "nx_rate": None
                    if pd.isna(cast(Any, r)["nx_rate_pct"])
                    else float(cast(Any, r)["nx_rate_pct"]),
                    "respons": int(cast(Any, r)["respons"]),
                    "nx_tinggi": float(cast(Any, r)["nx_rate_pct"]) >= 25.0,
                }
                for pfx, r in pd.concat([f1_priority, f2_priority]).drop_duplicates().iterrows()
            ],
            "tak_terplot": [],
        },
        "small_multiples": {
            "label": [anon_map.get(str(p), str(p)) for p in pareto_df["Subnet Prefix"].head(3)],
            "share": [float(v) for v in pareto_df["Share (%)"].head(3)],
            "waktu": [m.strftime("%H:%M") for m in temporal_steady.index],
            "seri": [],
            "puncak": [],
            "puncak_global": meta_temporal["peak_minute"].strftime("%H:%M"),
        },
        "dekomposisi": {
            "summary": {
                k: float(v) if isinstance(v, (int, float, np.floating, np.integer)) else v
                for k, v in nx_sum.items()
            },
            "host": [
                {"label": h_map.get(str(h), str(h)), "delta": float(d)}
                for h, d in delta_host.head(10).items()
            ],
            "reg": [
                {"label": r_map.get(str(r), str(r)), "delta": float(d)}
                for r, d in delta_reg.head(5).items()
            ],
            "reg_nilai": [float(d) for d in delta_reg.head(5).values],
            "qtype": [{"tipe": str(k), "delta": float(d)} for k, d in delta_qt.head(3).items()],
            "h1h2": {
                "nilai": float(delta_host.head(2).sum()),
                "persen": float(nx_sum["top2_share_net_pct"]),
                "pembanding": float(nx_sum["net_delta"]),
            },
            "nx_puncak": int(nx_sum["peak_nx"]),
            "baseline_per_menit": float(nx_sum["baseline_per_minute"]),
        },
        "trunc_qtype": trunc_qtype,
        "heatmap": {
            "qtype": matrix_meta["qtype_labels"],
            "rcode": matrix_meta["rcode_labels"],
            "nilai": matrix_meta["values"],
        },
        "komposisi": {
            "qtype": {
                "label": matrix_meta["composition_qtype"]["labels"],
                "nilai": matrix_meta["composition_qtype"]["values"],
            },
            "rcode": {
                "label": matrix_meta["composition_rcode"]["labels"],
                "nilai": matrix_meta["composition_rcode"]["values"],
            },
        },
    }

    # 9. Export Serialized Output
    summary_path = output_dir / "metrics_summary.json"
    export_metrics_summary(payload, summary_path)

    total_elapsed = time.perf_counter() - total_start
    logger.info(f"Pipeline executed successfully in {total_elapsed:.2f} seconds.")
    logger.info(f"Metrics summary exported: {summary_path}")
    return payload


def main():
    parser = argparse.ArgumentParser(
        description="High-Throughput DNS Telemetry Pipeline & Operational Analytics (MDS Edition)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_DATA_PATH,
        help="Path to input DNS trace file (CSV or Parquet)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory to save exported metrics summary (metrics_summary.json)",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=None,
        help="Optional row limit for fast testing or sample execution",
    )
    parser.add_argument(
        "--engine",
        choices=["pandas", "duckdb"],
        default="pandas",
        help="Analytical processing engine",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run Pandera data quality schema contracts",
    )
    parser.add_argument(
        "--to-parquet",
        action="store_true",
        help="Convert input CSV to high-performance compressed Parquet via DuckDB",
    )
    parser.add_argument(
        "--run-dbt",
        action="store_true",
        help="Execute dbt transformations and data tests (dbt run & dbt test)",
    )
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Run performance benchmark comparing Pandas vs DuckDB (CSV) vs DuckDB (Parquet)",
    )

    args = parser.parse_args()

    # Handlers for specific modern data stack actions
    if args.to_parquet:
        out_pq = Path("data/sample-dns-30min.parquet")
        convert_csv_to_parquet(args.input, out_pq)
        return

    if args.benchmark:
        cmd = [sys.executable, "scripts/benchmark.py", "--csv", str(args.input)]
        if args.sample:
            cmd.extend(["--sample", str(args.sample)])
        subprocess.run(cmd, check=True)
        return

    if args.run_dbt:
        import shutil

        dbt_bin = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")
        logger.info(f"Triggering dbt transformations using {dbt_bin}...")
        subprocess.run(
            [dbt_bin, "run", "--project-dir", "dbt_dns", "--profiles-dir", "dbt_dns"], check=True
        )
        logger.info("Triggering dbt data quality test suite...")
        subprocess.run(
            [dbt_bin, "test", "--project-dir", "dbt_dns", "--profiles-dir", "dbt_dns"], check=True
        )
        return

    try:
        run_pipeline(
            input_path=args.input,
            output_dir=args.output_dir,
            sample_size=args.sample,
            engine=args.engine,
            validate=args.validate,
        )
    except Exception:
        logger.exception("Pipeline failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
