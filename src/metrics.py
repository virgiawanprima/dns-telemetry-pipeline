"""DNS Metrics Computation: Temporal Aggregation, KPIs, Pareto, and Protocol Distributions."""

import ipaddress
import logging
from collections import Counter
from typing import Any

import numpy as np
import pandas as pd

from src.config import RCODE_NAMES

logger = logging.getLogger("dns_pipeline.metrics")


def get_subnet_prefix(ip_str: str) -> str:
    """Extract /48 prefix for IPv6 or /24 prefix for IPv4."""
    try:
        addr = ipaddress.ip_address(ip_str)
        prefix_len = 48 if addr.version == 6 else 24
        return str(ipaddress.ip_network(f"{addr}/{prefix_len}", strict=False))
    except (ValueError, TypeError):
        return "UNKNOWN"


def compute_temporal_aggregates(
    df: pd.DataFrame, filter_canonical_steady_state: bool = True
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Compute 1-minute bucket aggregates for network telemetry.

    Calculates packet counts, response counts, NXDOMAIN errors,
    UDP truncation events, and truncation ratios.
    """
    logger.info("Computing temporal aggregates...")
    # Add minute bucket
    df_temp = df.copy()
    df_temp["minute"] = df_temp["dt"].dt.floor("1min")

    # Group metrics by minute
    grouped = df_temp.groupby("minute")

    packets = grouped.size().rename("packets")
    queries = grouped.apply(lambda g: (g["qr"] == 0).sum(), include_groups=False).rename("queries")
    responses = grouped.apply(lambda g: (g["qr"] == 1).sum(), include_groups=False).rename(
        "responses"
    )
    nx = grouped.apply(lambda g: (g["rcode"] == 3).sum(), include_groups=False).rename("nx")
    udp_resp = grouped.apply(
        lambda g: ((g["proto"] == "udp") & (g["qr"] == 1)).sum(), include_groups=False
    ).rename("udp_resp")
    udp_tc = grouped.apply(
        lambda g: ((g["proto"] == "udp") & (g["qr"] == 1) & (g["tc"] == 1)).sum(),
        include_groups=False,
    ).rename("udp_tc")

    temporal = pd.concat([packets, queries, responses, nx, udp_resp, udp_tc], axis=1)

    # Derived rates
    temporal["nx_rate"] = np.where(
        temporal["responses"] > 0, (temporal["nx"] / temporal["responses"]) * 100.0, 0.0
    )
    temporal["tc_ratio"] = np.where(
        temporal["udp_resp"] > 0, (temporal["udp_tc"] / temporal["udp_resp"]) * 100.0, 0.0
    )

    # In 30-min capture, boundary minutes (first & last) are usually partial
    all_minutes = temporal.index
    if filter_canonical_steady_state and len(all_minutes) > 2:
        # Keep canonical intermediate minutes
        canonical_minutes = all_minutes[1:-1]
        temporal_steady = temporal.loc[canonical_minutes].copy()
    else:
        temporal_steady = temporal.copy()

    # Peak minutes
    peak_minute = temporal_steady["packets"].idxmax()
    nx_peak_minute = temporal_steady["nx"].idxmax()
    tc_peak_minute = temporal_steady["udp_tc"].idxmax()

    meta_temporal = {
        "n_minutes": len(temporal_steady),
        "peak_minute": peak_minute,
        "nx_peak_minute": nx_peak_minute,
        "tc_peak_minute": tc_peak_minute,
        "nx_rate_global": float(
            (temporal_steady["nx"].sum() / max(temporal_steady["responses"].sum(), 1)) * 100.0
        ),
        "tc_ratio_avg": float(
            (temporal_steady["udp_tc"].sum() / max(temporal_steady["udp_resp"].sum(), 1)) * 100.0
        ),
        "median_other": float(
            temporal_steady.loc[temporal_steady.index != peak_minute, "packets"].median()
        )
        if len(temporal_steady) > 1
        else float(temporal_steady["packets"].median()),
    }

    return temporal_steady, meta_temporal


def compute_pareto_prefix_distribution(
    df: pd.DataFrame, top_n: int = 10
) -> tuple[pd.DataFrame, dict[str, str]]:
    """Perform Pareto 80/20 concentration analysis on client IPv6 /48 prefixes."""
    logger.info("Computing prefix Pareto distribution...")
    q_mask = df["qr"] == 0
    unique_q_ips = df.loc[q_mask, "src_ip"].dropna().unique()

    prefix_map = {ip: get_subnet_prefix(ip) for ip in unique_q_ips}
    ip_counts = df.loc[q_mask, "src_ip"].value_counts()

    prefix_counts: Counter = Counter()
    for ip, count in ip_counts.items():
        prefix_counts[prefix_map[ip]] += count

    total_q = df.loc[q_mask].shape[0]
    top_prefixes = prefix_counts.most_common(top_n)

    pareto_df = pd.DataFrame(top_prefixes, columns=["Subnet Prefix", "Total Query"])
    pareto_df["Share (%)"] = (pareto_df["Total Query"] / max(total_q, 1)) * 100.0
    pareto_df["Kumulatif (%)"] = pareto_df["Share (%)"].cumsum()

    # Create anonymous label mapping for presentation
    anon_map = {p: f"S{i + 1}" for i, p in enumerate(pareto_df["Subnet Prefix"])}

    return pareto_df, anon_map


def compute_protocol_matrix(
    df: pd.DataFrame, top_qtypes: int = 5
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Compute cross-tabulation matrix of QTYPE vs RCODE response distribution."""
    logger.info("Computing QTYPE x RCODE cross-tabulation...")
    resp_df = df[df["qr"] == 1].copy()

    # Normalize qtype labels
    qtype_counts = resp_df["qtype_name"].value_counts()
    top_qtype_names = list(qtype_counts.head(top_qtypes).index)

    # Cross-tabulation normalized by row (percentage)
    crosstab_raw = pd.crosstab(resp_df["qtype_name"], resp_df["rcode"])
    crosstab_top = crosstab_raw.reindex(top_qtype_names).fillna(0)
    row_sums = crosstab_top.sum(axis=1).replace(0, 1)
    matrix_pct = crosstab_top.div(row_sums, axis=0) * 100.0

    # Human-readable column names
    col_names = [RCODE_NAMES.get(int(c), str(c)) for c in matrix_pct.columns]

    # Global compositions
    q_mask = df["qr"] == 0
    qtype_all = df.loc[q_mask, "qtype_name"].value_counts()
    top_q_disp = list(qtype_all.head(5).items())
    other_q = qtype_all.iloc[5:].sum()
    qt_labels = [k for k, _ in top_q_disp] + ["Lainnya"]
    qt_vals = [int(v) for _, v in top_q_disp] + [int(other_q)]

    rcode_all = resp_df["rcode"].value_counts()
    top_rc_disp = [(RCODE_NAMES.get(int(k), str(k)), v) for k, v in rcode_all.head(4).items()]
    other_rc = rcode_all.iloc[4:].sum()
    rc_labels = [k for k, _ in top_rc_disp] + ["Lainnya"]
    rc_vals = [int(v) for _, v in top_rc_disp] + [int(other_rc)]

    matrix_meta = {
        "qtype_labels": [str(x) for x in matrix_pct.index],
        "rcode_labels": col_names,
        "values": [[float(v) for v in row] for row in matrix_pct.values],
        "composition_qtype": {"labels": qt_labels, "values": qt_vals},
        "composition_rcode": {"labels": rc_labels, "values": rc_vals},
    }

    return matrix_pct, matrix_meta


def compute_truncation_by_qtype(df: pd.DataFrame, min_responses: int = 500) -> list[dict[str, Any]]:
    """Compute UDP response truncation rate (TC bit) per QTYPE."""
    udp_resp = df[(df["proto"] == "udp") & (df["qr"] == 1)]
    grouped = udp_resp.groupby("qtype_name", observed=True)

    counts = grouped.size()
    tc_counts = grouped["tc"].apply(lambda s: (s == 1).sum())

    filtered_counts = counts[counts >= min_responses]
    rates = (tc_counts[filtered_counts.index] / filtered_counts) * 100.0
    rates_sorted = rates.sort_values(ascending=False)

    results = []
    for qtype, rate in rates_sorted.items():
        results.append(
            {
                "tipe": str(qtype),
                "rate": float(rate),
                "resp": int(filtered_counts[qtype]),
            }
        )

    return results
