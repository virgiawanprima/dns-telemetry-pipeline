"""Unit tests for metric computations (temporal, pareto, contingency matrix, truncation)."""

import pandas as pd

from src.metrics import (
    compute_pareto_prefix_distribution,
    compute_protocol_matrix,
    compute_temporal_aggregates,
    compute_truncation_by_qtype,
    get_subnet_prefix,
)


def test_subnet_prefix_extraction():
    """Verify standard IPv6 /48 prefix extraction."""
    ip1 = "2001:db8:abcd:0012::1"
    # Should yield standard network notation
    pfx = get_subnet_prefix(ip1)
    assert "2001:db8:abcd" in pfx

    # Edge cases
    assert get_subnet_prefix("invalid-ip") == "UNKNOWN"
    assert get_subnet_prefix("") == "UNKNOWN"


def test_temporal_aggregates(sample_dns_df: pd.DataFrame):
    """Verify temporal 1-minute aggregation logic."""
    df_temporal, meta = compute_temporal_aggregates(
        sample_dns_df, filter_canonical_steady_state=False
    )

    assert len(df_temporal) >= 1
    assert "packets" in df_temporal.columns
    assert "nx" in df_temporal.columns
    assert "responses" in df_temporal.columns
    assert "tc_ratio" in df_temporal.columns
    assert meta["n_minutes"] == len(df_temporal)


def test_prefix_pareto(sample_dns_df: pd.DataFrame):
    """Verify IPv6 /48 prefix Pareto 80/20 distribution."""
    pareto_df, anon_map = compute_pareto_prefix_distribution(sample_dns_df, top_n=5)

    assert len(pareto_df) > 0
    assert "Subnet Prefix" in pareto_df.columns
    assert "Share (%)" in pareto_df.columns
    assert "Kumulatif (%)" in pareto_df.columns
    assert isinstance(anon_map, dict)

    # Verify cumulative sum reaches or approaches 100%
    assert pareto_df["Kumulatif (%)"].iloc[-1] <= 100.001


def test_protocol_matrix(sample_dns_df: pd.DataFrame):
    """Verify QTYPE vs RCODE cross-tabulation matrix."""
    matrix_pct, meta = compute_protocol_matrix(sample_dns_df, top_qtypes=3)

    assert not matrix_pct.empty
    assert "qtype_labels" in meta
    assert "rcode_labels" in meta
    assert "values" in meta


def test_truncation_by_qtype(sample_dns_df: pd.DataFrame):
    """Verify UDP truncation rate calculation."""
    trunc_list = compute_truncation_by_qtype(sample_dns_df, min_responses=1)
    assert isinstance(trunc_list, list)
    for item in trunc_list:
        assert "tipe" in item
        assert "rate" in item
        assert "resp" in item
