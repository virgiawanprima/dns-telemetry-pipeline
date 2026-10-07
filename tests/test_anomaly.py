"""Unit tests for anomaly decomposition and registrable domain extraction."""

import pandas as pd

from src.anomaly import extract_registrable_domain, nx_delta


def test_extract_registrable_domain():
    """Verify SLD extraction using PublicSuffix2."""
    assert extract_registrable_domain("api.google.com.") == "google.com"
    assert extract_registrable_domain("sub.service.co.id") == "service.co.id"
    assert extract_registrable_domain("") == "UNKNOWN"
    assert extract_registrable_domain("localhost.") == "localhost"


def test_nx_delta_reconciliation():
    """Verify mathematical reconciliation of peak-minute NXDOMAIN burst."""
    # Create two minutes of data
    m1 = pd.Timestamp("2026-08-19 08:00:00", tz="UTC")
    m2 = pd.Timestamp("2026-08-19 08:01:00", tz="UTC")
    assert isinstance(m2, pd.Timestamp)
    minute_idx = pd.DatetimeIndex([m1, m2])

    data = [
        {"minute": m1, "dst_ip": "host-A"},
        {"minute": m1, "dst_ip": "host-A"},
        {"minute": m1, "dst_ip": "host-B"},
        # Peak minute (m2) has burst on host-A
        {"minute": m2, "dst_ip": "host-A"},
        {"minute": m2, "dst_ip": "host-A"},
        {"minute": m2, "dst_ip": "host-A"},
        {"minute": m2, "dst_ip": "host-A"},
        {"minute": m2, "dst_ip": "host-B"},
    ]
    nx_df = pd.DataFrame(data)

    delta_series, summary = nx_delta(nx_df, minute_idx, peak=m2, group="dst_ip")

    assert summary["peak_nx"] == 5
    assert summary["baseline_per_minute"] == 3.0
    assert summary["net_delta"] == 2.0
    # Reconciliation error must be zero (strict equality)
    assert abs(delta_series.sum() - summary["net_delta"]) < 1e-9
    assert "host-A" in delta_series
    assert delta_series["host-A"] == 2.0  # (4 at peak - 2 baseline = +2)
