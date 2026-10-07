"""Test fixtures and mock DNS telemetry dataset generation."""

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def sample_dns_df() -> pd.DataFrame:
    """Generate a deterministic synthetic DNS telemetry DataFrame spanning 2 canonical minutes."""
    n_rows = 120
    # Two minutes: 2026-08-19 08:00:00 and 08:01:00 UTC
    base_ts = 1787126400  # 2026-08-19 08:00:00 UTC
    timestamps = [base_ts + (i % 120) for i in range(n_rows)]
    dt_series = pd.to_datetime(timestamps, unit="s", utc=True)

    rng = np.random.default_rng(42)

    src_ips = [
        "2001:db8:1111:0000::1" if i % 2 == 0 else "2001:db8:2222:0000::2" for i in range(n_rows)
    ]
    dst_ips = [
        "2001:db8:aaaa:0000::10" if i % 3 == 0 else "2001:db8:bbbb:0000::20" for i in range(n_rows)
    ]

    qnames = ["api.example.com." if i % 4 != 0 else f"nx-host-{i}.invalid." for i in range(n_rows)]

    qtypes = [1 if i % 3 == 0 else (28 if i % 3 == 1 else 255) for i in range(n_rows)]
    qtype_names = ["A" if q == 1 else ("AAAA" if q == 28 else "ANY") for q in qtypes]

    # Half queries (qr=0), half responses (qr=1)
    qrs = [0 if i % 2 == 0 else 1 for i in range(n_rows)]

    # For responses, set RCODE (0=NOERROR, 3=NXDOMAIN if invalid name)
    rcodes = []
    rcode_names = []
    for i, qr in enumerate(qrs):
        if qr == 0:
            rcodes.append(None)
            rcode_names.append(None)
        else:
            if "invalid" in qnames[i]:
                rcodes.append(3)
                rcode_names.append("NXDOMAIN")
            else:
                rcodes.append(0)
                rcode_names.append("NOERROR")

    protos = ["udp" if i % 5 != 0 else "tcp" for i in range(n_rows)]
    tcs = [1 if (qrs[i] == 1 and protos[i] == "udp" and i % 10 == 0) else 0 for i in range(n_rows)]
    frame_lens = rng.integers(64, 1400, size=n_rows)
    dns_lens = [fl - 40 for fl in frame_lens]
    qdcounts = [1 for _ in range(n_rows)]

    df = pd.DataFrame(
        {
            "ts": timestamps,
            "ts_iso": [dt.isoformat() for dt in dt_series],
            "dt": dt_series,
            "src_ip": src_ips,
            "dst_ip": dst_ips,
            "proto": protos,
            "frame_len": frame_lens,
            "dns_len": dns_lens,
            "qdcount": qdcounts,
            "qr": qrs,
            "qtype": qtypes,
            "qtype_name": qtype_names,
            "rcode": rcodes,
            "rcode_name": rcode_names,
            "qname": qnames,
            "tc": tcs,
        }
    )

    # Cast to optimized nullable categories/types
    df["proto"] = df["proto"].astype("category")
    df["qtype_name"] = df["qtype_name"].astype("category")
    df["rcode_name"] = df["rcode_name"].astype("category")
    df["qr"] = df["qr"].astype("int8")
    df["tc"] = df["tc"].astype("int8")
    df["frame_len"] = df["frame_len"].astype("uint32")
    df["dns_len"] = df["dns_len"].astype("uint32")
    df["qdcount"] = df["qdcount"].astype("uint16")
    df["qtype"] = df["qtype"].astype("uint16")
    df["rcode"] = pd.Series(df["rcode"], dtype="Int8")

    return df
