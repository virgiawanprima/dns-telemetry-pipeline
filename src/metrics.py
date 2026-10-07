"""DNS Metrics Computation: Temporal Aggregation, KPIs, Pareto, and Protocol Distributions."""

import ipaddress
import logging
from collections import Counter
from typing import Any, cast

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
    df_temp["minute"] = cast(Any, df_temp["dt"].dt).floor("1min")

    # Group metrics by minute
    grouped = df_temp.groupby("minute")
    packets = grouped.size()
    queries = df_temp[df_temp["qr"] == 0].groupby("minute").size()
    responses = df_temp[df_temp["qr"] == 1].groupby("minute").size()
    nx = df_temp[df_temp["rcode"] == 3].groupby("minute").size()
    udp_resp = df_temp[(df_temp["proto"] == "udp") & (df_temp["qr"] == 1)].groupby("minute").size()
    udp_tc = (
        df_temp[(df_temp["proto"] == "udp") & (df_temp["qr"] == 1) & (df_temp["tc"] == 1)]
        .groupby("minute")
        .size()
    )

    temporal = pd.concat(
        {
            "packets": packets,
            "queries": queries,
            "responses": responses,
            "nx": nx,
            "udp_resp": udp_resp,
            "udp_tc": udp_tc,
        },
        axis=1,
    ).fillna(0)

    # Reindex to continuous 1-minute intervals to handle minutes without messages
    if not temporal.empty:
        start_min = temporal.index.min()
        end_min = temporal.index.max()
        tz = getattr(start_min, "tz", None)
        full_index = pd.date_range(start=start_min, end=end_min, freq="1min", tz=tz)
        temporal = temporal.reindex(full_index, fill_value=0)
        temporal.index.name = "minute"

    # Derived rates
    resp = temporal["responses"]
    temporal["nx_rate"] = (temporal["nx"] / resp.replace(0, np.nan) * 100.0).fillna(0.0)
    udp_r = temporal["udp_resp"]
    temporal["tc_ratio"] = (temporal["udp_tc"] / udp_r.replace(0, np.nan) * 100.0).fillna(0.0)

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
            cast(
                Any,
                temporal_steady.loc[temporal_steady.index != peak_minute, "packets"],
            ).median()
        )
        if len(temporal_steady) > 1
        else float(cast(Any, temporal_steady["packets"]).median()),
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
    qtype_counts = df.loc[df["qr"] == 1, "qtype_name"].value_counts()
    top_qtype_names = list(qtype_counts.head(top_qtypes).index)

    # Cross-tabulation normalized by row (percentage)
    crosstab_raw = pd.crosstab(resp_df["qtype_name"], resp_df["rcode"])
    crosstab_top = crosstab_raw.reindex(top_qtype_names).fillna(0)
    row_sums = crosstab_top.sum(axis=1).replace(0, 1)
    matrix_pct = crosstab_top.div(row_sums, axis=0) * 100.0

    # Human-readable column names
    col_names = [RCODE_NAMES.get(int(cast(Any, c)), f"{c}") for c in matrix_pct.columns]

    # Global compositions
    q_mask = df["qr"] == 0
    qtype_all = df.loc[q_mask, "qtype_name"].value_counts()
    top_q_disp = list(qtype_all.head(5).items())
    other_q = qtype_all.iloc[5:].sum()
    qt_labels = [k for k, _ in top_q_disp] + ["Lainnya"]
    qt_vals = [v for _, v in top_q_disp] + [other_q]

    rcode_all = df.loc[df["qr"] == 1, "rcode"].value_counts()
    top_rc_disp = [
        (RCODE_NAMES.get(int(cast(Any, k)), f"{k}"), v) for k, v in rcode_all.head(4).items()
    ]
    other_rc = rcode_all.iloc[4:].sum()
    rc_labels = [k for k, _ in top_rc_disp] + ["Lainnya"]
    rc_vals = [v for _, v in top_rc_disp] + [other_rc]

    matrix_meta = {
        "qtype_labels": [f"{x}" for x in matrix_pct.index],
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
    counts = dict(grouped.size())

    tc_resp = df[(df["proto"] == "udp") & (df["qr"] == 1) & (df["tc"] == 1)]
    tc_grouped = tc_resp.groupby("qtype_name", observed=True)
    tc_counts = dict(tc_grouped.size())

    results: list[dict[str, Any]] = []
    for qtype, total in counts.items():
        if total >= min_responses:
            tc_count = tc_counts.get(qtype, 0)
            rate = (tc_count / total) * 100.0
            results.append(
                {
                    "tipe": str(qtype),
                    "rate": float(rate),
                    "resp": total,
                }
            )

    results.sort(key=lambda x: x["rate"], reverse=True)
    return results
