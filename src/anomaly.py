"""Anomaly Detection and Root Cause Analysis for DNS Telemetry."""

import logging
import math
from typing import Any

import numpy as np
import pandas as pd
import publicsuffix2

logger = logging.getLogger("dns_pipeline.anomaly")


def extract_registrable_domain(qname_str: str) -> str:
    """Extract registered domain / SLD (e.g. example.co.id from sub.example.co.id.).

    Uses Public Suffix List rules via publicsuffix2.
    """
    if not isinstance(qname_str, str) or not qname_str:
        return "UNKNOWN"
    clean_name = qname_str.rstrip(".").lower()
    try:
        suffix = publicsuffix2.get_sld(clean_name)
    except AttributeError:
        suffix = publicsuffix2.get_public_suffix(clean_name)
    return suffix if suffix else clean_name


def calculate_entropy(s: str) -> float:
    """Calculate Shannon entropy for a given domain string."""
    if not s:
        return 0.0
    prob = [float(s.count(c)) / len(s) for c in set(s)]
    return -sum(p * math.log2(p) for p in prob)


def nx_delta(
    nx_rows: pd.DataFrame, minute_index: pd.DatetimeIndex, peak: pd.Timestamp, group: str = "dst_ip"
) -> tuple[pd.Series, dict[str, Any]]:
    """Decompose peak minute NXDOMAIN increase against other steady-state minutes.

    Computes net excess count per group (host, domain, or record type).
    Preserves negative deltas and validates strict arithmetic reconciliation.
    """
    idx = pd.DatetimeIndex(minute_index)
    if idx.has_duplicates or peak not in idx or len(idx) < 2:
        raise ValueError("Minute index must be unique, contain peak, and have at least 2 minutes.")

    work = nx_rows.loc[nx_rows["minute"].isin(idx)]
    if work[group].isna().any():
        raise ValueError(f"Group column '{group}' contains NaN values.")

    at_peak = work["minute"].eq(peak)
    n_other = len(idx) - 1

    a = work.loc[at_peak].groupby(group, observed=True).size()
    b = work.loc[~at_peak].groupby(group, observed=True).size() / n_other
    union = a.index.union(b.index)

    delta = (a.reindex(union, fill_value=0) - b.reindex(union, fill_value=0)).sort_values(
        ascending=False
    )
    peak_total = int(at_peak.sum())
    baseline = float((~at_peak).sum()) / n_other
    net = float(peak_total - baseline)

    if not np.isclose(delta.sum(), net, rtol=1e-10, atol=1e-8):
        raise ValueError("Reconciliation failed: sum of deltas does not match net difference.")

    top2_share = float(100.0 * delta.head(2).sum() / net) if net > 0 else 0.0

    summary = {
        "peak_nx": peak_total,
        "baseline_per_minute": baseline,
        "net_delta": net,
        "top2_share_net_pct": top2_share,
        "n_other_minutes": n_other,
    }

    return delta, summary


def compute_priority_tables(
    q_work: pd.DataFrame, r_view: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Identify subnets requiring operational focus for traffic load vs error rate.

    - Load priority: highest query volume and steady activity.
    - Anomaly priority: high NXDOMAIN counts and high NX failure percentage (>25%).
    """
    qn = q_work.groupby("prefix", observed=True).size().rename("query")
    active = q_work.groupby("prefix", observed=True)["minute"].nunique().rename("menit_aktif")
    rn = r_view.groupby("prefix", observed=True).size().rename("respons")
    nx = (
        r_view.loc[r_view["rcode"].eq(3)].groupby("prefix", observed=True).size().rename("nxdomain")
    )

    out = pd.concat([qn, active, rn, nx], axis=1).fillna(0).astype("int64")
    out.index.name = "prefix"
    out["nx_rate_pct"] = 100.0 * out["nxdomain"] / out["respons"].replace(0, np.nan)
    total_q = out["query"].sum()
    out["share_pct"] = 100.0 * out["query"] / total_q if total_q else np.nan

    # Priority 1: Query load
    f1 = out.sort_values(by=["query", "menit_aktif"], ascending=[False, False]).head(10).copy()
    f1["Alasan Prioritas"] = np.where(
        f1["query"] >= 40000,
        "volume terbesar; aktif hampir sepanjang periode",
        "aktif hampir sepanjang periode",
    )

    # Priority 2: NXDOMAIN errors
    f2_candidates = out[out["respons"] >= 1000].copy()
    f2 = (
        f2_candidates.sort_values(by=["nxdomain", "nx_rate_pct"], ascending=[False, False])
        .head(10)
        .copy()
    )
    f2["Alasan Prioritas"] = np.where(
        f2["nx_rate_pct"] >= 75.0,
        "volume NX terbesar; NX rate tinggi",
        np.where(f2["nx_rate_pct"] >= 25.0, "NX rate tinggi", "rasio perlu dipantau"),
    )

    return f1, f2
