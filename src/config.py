"""Configuration and Protocol Specifications for DNS Telemetry Pipeline."""

from pathlib import Path

# Base project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_PATH = PROJECT_ROOT / "data" / "sample-dns-30min.csv"
DEFAULT_PARQUET_PATH = PROJECT_ROOT / "data" / "sample-dns-30min.parquet"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "output"

# High-efficiency pandas nullable & category dtypes for fast C-engine loading
DNS_SCHEMA_DTYPES: dict[str, str] = {
    "ip_ver": "Int8",
    "proto": "category",
    "src_port": "UInt16",
    "dst_port": "UInt16",
    "frame_len": "UInt32",
    "dns_len": "UInt32",
    "dns_id": "UInt16",
    "qr": "Int8",  # 0: Query, 1: Response
    "opcode": "Int8",
    "aa": "Int8",
    "tc": "Int8",
    "rd": "Int8",
    "ra": "Int8",
    "ad": "Int8",
    "cd": "Int8",
    "rcode": "Int16",
    "qdcount": "UInt16",
    "ancount": "UInt16",
    "nscount": "UInt16",
    "arcount": "UInt16",
    "qtype": "UInt16",
    "qtype_name": "category",
    "qclass": "UInt16",
    "edns": "Int8",
    "edns_udpsize": "UInt16",
    "do": "Int8",
}

# DNS RFC 1035 / RFC 8914 Response Codes
RCODE_NAMES: dict[int, str] = {
    0: "NOERROR",
    1: "FORMERR",
    2: "SERVFAIL",
    3: "NXDOMAIN",
    4: "NOTIMP",
    5: "REFUSED",
    6: "YXDOMAIN",
    7: "YXRRSET",
    8: "NXRRSET",
    9: "NOTAUTH",
    10: "NOTZONE",
    16: "BADVERS",
    17: "BADKEY",
    18: "BADTIME",
    19: "BADMODE",
    20: "BADNAME",
    21: "BADALG",
    22: "BADTRUNC",
    23: "BADCOOKIE",
}

# Standard EDNS0 buffer threshold (DNS Flag Day 2020 recommended limit)
EDNS0_RECOMMENDED_UDP_SIZE = 1232
