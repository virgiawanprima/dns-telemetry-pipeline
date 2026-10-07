"""Analytical Truth & Metric Reconciliation Tests.

Verifies mathematical correctness of metrics, boundary conditions,
denominator logic, cross-engine consistency (Pandas vs DuckDB), and
data quality contracts using independently calculated expected values.
"""

import json
import math
from pathlib import Path

import pandas as pd

from src.anomaly import nx_delta
from src.cli import run_pipeline
from src.duck_engine import convert_csv_to_parquet, run_duckdb_aggregations
from src.metrics import compute_temporal_aggregates
from src.validation import validate_dns_data


def test_nxdomain_rate_uses_responses_denominator():
    """Verify that NXDOMAIN rate uses total responses (qr=1) as denominator,

    NOT total packets or queries.
    Independent calculation:
    - 60 queries (qr=0)
    - 40 responses (qr=1) of which 10 have rcode=3 (NXDOMAIN)
    - Expected rate = 10 / 40 * 100.0 = 25.0%
    (If total packets were denominator: 10 / 100 = 10.0%, which is incorrect).
    """
    ts_base = 1787126400  # 08:00:00 UTC
    dt_base = pd.to_datetime(ts_base, unit="s", utc=True)

    qrs = [0] * 60 + [1] * 40
    rcodes = [None] * 60 + [3] * 10 + [0] * 30

    df = pd.DataFrame(
        {
            "ts": [ts_base] * 100,
            "dt": [dt_base] * 100,
            "qr": pd.Series(qrs, dtype="int8"),
            "rcode": pd.Series(rcodes, dtype="Int8"),
            "proto": pd.Series(["udp"] * 100, dtype="category"),
            "tc": pd.Series([0] * 100, dtype="int8"),
        }
    )

    temporal, meta = compute_temporal_aggregates(df, filter_canonical_steady_state=False)

    assert len(temporal) == 1
    row = temporal.iloc[0]
    assert row["packets"] == 100
    assert row["queries"] == 60
    assert row["responses"] == 40
    assert row["nx"] == 10

    # Strict mathematical truth assertion
    expected_rate = (10 / 40) * 100.0  # 25.0%
    assert math.isclose(row["nx_rate"], expected_rate, rel_tol=1e-9)
    assert not math.isclose(row["nx_rate"], (10 / 100) * 100.0, rel_tol=1e-9)


def test_truncation_ratio_uses_udp_responses_denominator():
    """Verify that truncation ratio uses total UDP responses as denominator.

    Independent calculation:
    - 30 UDP queries (proto='udp', qr=0)
    - 20 TCP responses (proto='tcp', qr=1, tc=0)
    - 50 UDP responses (proto='udp', qr=1) of which 5 have tc=1
    - Expected truncation ratio = 5 / 50 * 100.0 = 10.0%
    """
    ts_base = 1787126400
    dt_base = pd.to_datetime(ts_base, unit="s", utc=True)

    records = []
    # 30 UDP queries
    for _ in range(30):
        records.append({"proto": "udp", "qr": 0, "rcode": None, "tc": 0})
    # 20 TCP responses
    for _ in range(20):
        records.append({"proto": "tcp", "qr": 1, "rcode": 0, "tc": 0})
    # 45 non-truncated UDP responses
    for _ in range(45):
        records.append({"proto": "udp", "qr": 1, "rcode": 0, "tc": 0})
    # 5 truncated UDP responses
    for _ in range(5):
        records.append({"proto": "udp", "qr": 1, "rcode": 0, "tc": 1})

    df = pd.DataFrame(records)
    df["ts"] = ts_base
    df["dt"] = dt_base
    df["qr"] = df["qr"].astype("int8")
    df["tc"] = df["tc"].astype("int8")
    df["proto"] = df["proto"].astype("category")
    df["rcode"] = pd.Series(df["rcode"], dtype="Int8")

    temporal, meta = compute_temporal_aggregates(df, filter_canonical_steady_state=False)

    row = temporal.iloc[0]
    assert row["udp_resp"] == 50
    assert row["udp_tc"] == 5

    expected_ratio = (5 / 50) * 100.0  # 10.0%
    assert math.isclose(row["tc_ratio"], expected_ratio, rel_tol=1e-9)
    assert not math.isclose(row["tc_ratio"], (5 / 100) * 100.0, rel_tol=1e-9)


def test_baseline_nxdomain_excludes_peak_minute():
    """Verify that baseline NXDOMAIN excludes the peak minute from the baseline average."""
    m1 = pd.Timestamp("2026-08-19 08:00:00", tz="UTC")
    m2 = pd.Timestamp("2026-08-19 08:01:00", tz="UTC")  # Peak
    m3 = pd.Timestamp("2026-08-19 08:02:00", tz="UTC")
    minute_idx = pd.DatetimeIndex([m1, m2, m3])

    # M1: 2 NXDOMAIN, M2 (peak): 10 NXDOMAIN, M3: 4 NXDOMAIN
    # Baseline average of non-peak minutes = (2 + 4) / 2 = 3.0
    data = (
        [{"minute": m1, "dst_ip": "host-1"}] * 2
        + [{"minute": m2, "dst_ip": "host-1"}] * 10
        + [{"minute": m3, "dst_ip": "host-1"}] * 4
    )
    nx_df = pd.DataFrame(data)

    _, summary = nx_delta(nx_df, minute_idx, peak=m2, group="dst_ip")

    assert summary["peak_nx"] == 10
    # Strict exclusion: baseline must be exactly 3.0, NOT (2+10+4)/3 = 5.33
    assert math.isclose(summary["baseline_per_minute"], 3.0, rel_tol=1e-9)
    assert math.isclose(summary["net_delta"], 7.0, rel_tol=1e-9)  # 10 - 3.0 = 7.0


def test_global_net_delta_strictly_equals_sum_of_host_deltas_with_negative_contributions():
    """Verify that global net delta equals sum of all host deltas,

    including hosts whose error rates dropped during peak minute (negative delta).
    """
    m1 = pd.Timestamp("2026-08-19 08:00:00", tz="UTC")
    m2 = pd.Timestamp("2026-08-19 08:01:00", tz="UTC")  # Peak
    minute_idx = pd.DatetimeIndex([m1, m2])

    # Host-A: Baseline=1, Peak=5 -> delta = +4
    # Host-B: Baseline=3, Peak=1 -> delta = -2 (negative contribution)
    # Host-C: Baseline=2, Peak=4 -> delta = +2
    # Baseline sum = 6, Peak sum = 10, Net delta = +4
    # Host deltas sum = (+4) + (-2) + (+2) = +4
    data = [
        {"minute": m1, "dst_ip": "host-A"},
        {"minute": m1, "dst_ip": "host-B"},
        {"minute": m1, "dst_ip": "host-B"},
        {"minute": m1, "dst_ip": "host-B"},
        {"minute": m1, "dst_ip": "host-C"},
        {"minute": m1, "dst_ip": "host-C"},
        # Peak minute
        {"minute": m2, "dst_ip": "host-A"},
        {"minute": m2, "dst_ip": "host-A"},
        {"minute": m2, "dst_ip": "host-A"},
        {"minute": m2, "dst_ip": "host-A"},
        {"minute": m2, "dst_ip": "host-A"},
        {"minute": m2, "dst_ip": "host-B"},
        {"minute": m2, "dst_ip": "host-C"},
        {"minute": m2, "dst_ip": "host-C"},
        {"minute": m2, "dst_ip": "host-C"},
        {"minute": m2, "dst_ip": "host-C"},
    ]
    nx_df = pd.DataFrame(data)

    delta_series, summary = nx_delta(nx_df, minute_idx, peak=m2, group="dst_ip")

    assert delta_series["host-A"] == 4.0
    assert delta_series["host-B"] == -2.0
    assert delta_series["host-C"] == 2.0

    # Arithmetic reconciliation
    assert math.isclose(delta_series.sum(), summary["net_delta"], rel_tol=1e-9)
    assert summary["net_delta"] == 4.0


def test_h1_h2_share_handles_zero_and_negative_net_delta():
    """Verify that H1/H2 share calculation cleanly handles non-positive net delta without error."""
    m1 = pd.Timestamp("2026-08-19 08:00:00", tz="UTC")
    m2 = pd.Timestamp("2026-08-19 08:01:00", tz="UTC")
    minute_idx = pd.DatetimeIndex([m1, m2])

    # No surge (peak minute has fewer errors than baseline)
    data = [
        {"minute": m1, "dst_ip": "host-A"},
        {"minute": m1, "dst_ip": "host-A"},
        {"minute": m2, "dst_ip": "host-A"},
    ]
    nx_df = pd.DataFrame(data)

    _, summary = nx_delta(nx_df, minute_idx, peak=m2, group="dst_ip")

    # Net delta is negative (-1.0), top2 share must be 0.0 without division by zero
    assert summary["net_delta"] < 0
    assert summary["top2_share_net_pct"] == 0.0


def test_temporal_aggregation_handles_empty_minute_gap():
    """Verify temporal aggregation fills empty minute gap with 0 packets and 0.0 rates."""
    m_start = pd.Timestamp("2026-08-19 08:00:00", tz="UTC")
    m_end = pd.Timestamp("2026-08-19 08:02:00", tz="UTC")

    df = pd.DataFrame(
        {
            "ts": [1787126400, 1787126520],
            "dt": [m_start, m_end],
            "qr": pd.Series([0, 1], dtype="int8"),
            "rcode": pd.Series([None, 0], dtype="Int8"),
            "proto": pd.Series(["udp", "udp"], dtype="category"),
            "tc": pd.Series([0, 0], dtype="int8"),
        }
    )

    temporal, meta = compute_temporal_aggregates(df, filter_canonical_steady_state=False)

    # Must contain 3 minutes: 08:00, 08:01, 08:02
    assert len(temporal) == 3
    empty_min = pd.Timestamp("2026-08-19 08:01:00", tz="UTC")
    assert empty_min in temporal.index

    row_empty = temporal.loc[empty_min]
    assert row_empty["packets"] == 0
    assert row_empty["queries"] == 0
    assert row_empty["responses"] == 0
    assert row_empty["nx_rate"] == 0.0
    assert row_empty["tc_ratio"] == 0.0


def test_pandas_and_duckdb_numerical_consistency(sample_dns_df: pd.DataFrame, tmp_path: Path):
    """Verify that Pandas and DuckDB produce consistent aggregation results within tolerance."""
    csv_file = tmp_path / "consistency_test.csv"
    parquet_file = tmp_path / "consistency_test.parquet"

    sample_dns_df.to_csv(csv_file, index=False)
    convert_csv_to_parquet(csv_file, parquet_file, compression="zstd")

    # 1. DuckDB metrics
    duck_results = run_duckdb_aggregations(parquet_file)
    duck_kpis = duck_results["kpis"]

    # 2. Pandas ground truth
    pandas_total = len(sample_dns_df)
    pandas_queries = (sample_dns_df["qr"] == 0).sum()
    pandas_responses = (sample_dns_df["qr"] == 1).sum()
    pandas_nx = (sample_dns_df["rcode"] == 3).sum()
    pandas_udp_resp = ((sample_dns_df["proto"] == "udp") & (sample_dns_df["qr"] == 1)).sum()
    pandas_udp_tc = (
        (sample_dns_df["proto"] == "udp") & (sample_dns_df["qr"] == 1) & (sample_dns_df["tc"] == 1)
    ).sum()

    # Cross-engine assertions
    assert duck_kpis["total_packets"] == pandas_total
    assert duck_kpis["total_queries"] == pandas_queries
    assert duck_kpis["total_responses"] == pandas_responses
    assert duck_kpis["total_nxdomain"] == pandas_nx
    assert duck_kpis["udp_responses"] == pandas_udp_resp
    assert duck_kpis["udp_truncated"] == pandas_udp_tc


def test_pandera_accepts_valid_and_rejects_each_invalid_category(sample_dns_df: pd.DataFrame):
    """Verify Pandera contract validation:

    - Accepts conforming data
    - Rejects RFC protocol violations (QR, proto, rcode)
    - Rejects project data quality rule violations (negative length, qdcount > 20)
    """
    # 1. Valid data passes
    is_valid, _ = validate_dns_data(sample_dns_df, sample_size=len(sample_dns_df))
    assert is_valid is True

    # 2. Protocol limit: QR must be 0 or 1 (RFC 1035 1-bit QR field)
    df_bad_qr = sample_dns_df.copy()
    df_bad_qr.loc[0, "qr"] = 2
    is_valid, _ = validate_dns_data(df_bad_qr, sample_size=len(df_bad_qr))
    assert is_valid is False

    # 3. Protocol limit: Transport protocol must be udp or tcp (RFC 1035 Section 4.2)
    df_bad_proto = sample_dns_df.copy()
    df_bad_proto["proto"] = df_bad_proto["proto"].astype(str)
    df_bad_proto.loc[0, "proto"] = "icmp"
    is_valid, _ = validate_dns_data(df_bad_proto, sample_size=len(df_bad_proto))
    assert is_valid is False

    # 4. Protocol limit: RCODE must be within RFC range 0..23 (RFC 1035 & RFC 6895)
    df_bad_rcode = sample_dns_df.copy()
    df_bad_rcode.loc[0, "rcode"] = 99
    is_valid, _ = validate_dns_data(df_bad_rcode, sample_size=len(df_bad_rcode))
    assert is_valid is False

    # 5. Project data quality policy: Frame length must be strictly positive (> 0)
    df_bad_len = sample_dns_df.copy()
    df_bad_len["frame_len"] = df_bad_len["frame_len"].astype("int64")
    df_bad_len.loc[0, "frame_len"] = 0
    is_valid, _ = validate_dns_data(df_bad_len, sample_size=len(df_bad_len))
    assert is_valid is False

    # 6. Project data quality policy: QDCOUNT reasonable upper bound (<= 20)
    df_bad_qdcount = sample_dns_df.copy()
    df_bad_qdcount["qdcount"] = df_bad_qdcount["qdcount"].astype("int64")
    df_bad_qdcount.loc[0, "qdcount"] = 50
    is_valid, _ = validate_dns_data(df_bad_qdcount, sample_size=len(df_bad_qdcount))
    assert is_valid is False


def test_cli_synthetic_pipeline_produces_clean_json_no_nan_no_real_identities(
    sample_dns_df: pd.DataFrame, tmp_path: Path
):
    """Verify that CLI pipeline produces a clean JSON payload without NaN/inf or un-anonymized identities."""
    csv_file = tmp_path / "synthetic_trace.csv"
    output_dir = tmp_path / "output"
    sample_dns_df.to_csv(csv_file, index=False)

    _ = run_pipeline(
        input_path=csv_file,
        output_dir=output_dir,
        sample_size=None,
        engine="pandas",
        validate=True,
    )

    out_file = output_dir / "metrics_summary.json"
    assert out_file.exists()

    with open(out_file, "r", encoding="utf-8") as f:
        content = f.read()

    # Must be valid standard JSON without NaN or Infinity strings
    assert "NaN" not in content
    assert "Infinity" not in content

    parsed = json.loads(content)
    assert "kpi" in parsed
    assert "temporal" in parsed
    assert "pareto" in parsed
    assert "dekomposisi" in parsed

    # Verify no real/un-anonymized public IPv6 addresses in presentation labels
    for p in parsed["pareto"]["label"]:
        assert not p.startswith("2001:0db8") or p.startswith("S")
