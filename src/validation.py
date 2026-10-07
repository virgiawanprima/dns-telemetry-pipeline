import logging
from typing import Any

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors
from pandera.pandas import Check, Column, DataFrameSchema

logger = logging.getLogger("dns_pipeline.validation")

# Pandera Schema Contract for DNS Telemetry Packets
DNS_PACKET_SCHEMA = DataFrameSchema(
    columns={
        "qr": Column(
            pa.Int,
            checks=[Check.isin([0, 1])],
            description="DNS Message Type: 0 for Query, 1 for Response",
            coerce=True,
        ),
        "proto": Column(
            pa.String,
            checks=[Check.isin(["udp", "tcp"])],
            description="Transport layer protocol",
            coerce=True,
        ),
        "rcode": Column(
            checks=[Check.in_range(0, 23)],
            description="DNS RFC 1035 / 8914 Response Code (0=NOERROR, 3=NXDOMAIN, etc.)",
            nullable=True,
        ),
        "qdcount": Column(
            pa.Int,
            checks=[Check.in_range(0, 20)],
            description="Question record count in DNS header",
            coerce=True,
        ),
        "frame_len": Column(
            pa.Int,
            checks=[Check.greater_than(0)],
            description="Captured network frame length in bytes",
            coerce=True,
        ),
        "dns_len": Column(
            pa.Int,
            checks=[Check.greater_than_or_equal_to(0)],
            description="Length of payload DNS layer",
            coerce=True,
        ),
        "tc": Column(
            pa.Int,
            checks=[Check.isin([0, 1])],
            description="Truncation bit flag",
            coerce=True,
        ),
    },
    strict=False,  # Allow other dataset columns without failing validation
    coerce=True,
)


def validate_dns_data(df: pd.DataFrame, sample_size: int = 100000) -> tuple[bool, dict[str, Any]]:
    """Validate DNS telemetry records against protocol schema contracts.

    Args:
        df: Input DNS telemetry DataFrame.
        sample_size: Number of records to validate for fast contract verification.

    Returns:
        Tuple of (is_valid: bool, validation_report: dict).
    """
    logger.info(
        f"Running Pandera data quality validation on sample ({min(len(df), sample_size):,} rows)..."
    )
    eval_df = df.head(sample_size).copy()

    try:
        DNS_PACKET_SCHEMA.validate(eval_df, lazy=True)
        logger.info("Data quality validation PASSED: All protocol contracts satisfied.")
        report = {
            "status": "PASSED",
            "validated_rows": len(eval_df),
            "errors": [],
            "schema_columns": list(DNS_PACKET_SCHEMA.columns.keys()),
        }
        return True, report
    except SchemaErrors as err:
        failure_cases = err.failure_cases
        logger.warning(
            f"Data quality validation detected {len(failure_cases)} contract violation(s)."
        )
        report = {
            "status": "FAILED",
            "validated_rows": len(eval_df),
            "failure_cases": failure_cases.to_dict(orient="records")
            if hasattr(failure_cases, "to_dict")
            else str(failure_cases),
            "errors": [str(e) for e in err.schema_errors],
        }
        return False, report
