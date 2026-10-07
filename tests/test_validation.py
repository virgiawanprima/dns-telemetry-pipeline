"""Unit tests for Pandera schema validation and data contracts."""

import pandas as pd

from src.validation import validate_dns_data


def test_valid_telemetry_schema(sample_dns_df: pd.DataFrame):
    """Verify that conforming synthetic DNS telemetry passes Pandera validation."""
    is_valid, report = validate_dns_data(sample_dns_df, sample_size=len(sample_dns_df))
    assert is_valid is True
    assert report["status"] == "PASSED"
    assert len(report["errors"]) == 0


def test_invalid_qr_flag_fails(sample_dns_df: pd.DataFrame):
    """Verify that invalid QR bit (not 0 or 1) triggers validation error."""
    corrupted_df = sample_dns_df.copy()
    corrupted_df.loc[0, "qr"] = 99

    is_valid, report = validate_dns_data(corrupted_df, sample_size=len(corrupted_df))
    assert is_valid is False
    assert report["status"] == "FAILED"


def test_invalid_protocol_fails(sample_dns_df: pd.DataFrame):
    """Verify that non-standard transport protocol (not udp or tcp) triggers validation error."""
    corrupted_df = sample_dns_df.copy()
    corrupted_df["proto"] = corrupted_df["proto"].astype(str)
    corrupted_df.loc[0, "proto"] = "icmp"

    is_valid, report = validate_dns_data(corrupted_df, sample_size=len(corrupted_df))
    assert is_valid is False
    assert report["status"] == "FAILED"


def test_negative_length_fails(sample_dns_df: pd.DataFrame):
    """Verify that packet with non-positive frame length violates contract."""
    corrupted_df = sample_dns_df.copy()
    corrupted_df["frame_len"] = corrupted_df["frame_len"].astype("int64")
    corrupted_df.loc[0, "frame_len"] = -50

    is_valid, report = validate_dns_data(corrupted_df, sample_size=len(corrupted_df))
    assert is_valid is False
    assert report["status"] == "FAILED"
