"""Exporting Analytical Metrics and Telemetry Summaries."""

import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger("dns_pipeline.exporter")


def sanitize_for_json(obj: Any) -> Any:
    """Recursively convert NumPy/Pandas objects, NaNs, and datetimes into JSON-safe types."""
    if isinstance(obj, dict):
        return {str(k): sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [sanitize_for_json(v) for v in obj]
    try:
        if pd.isna(obj):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(obj, (bool, np.bool_)):
        return bool(obj)
    if isinstance(obj, (np.integer, int)):
        return int(obj)
    if isinstance(obj, (np.floating, float)):
        return None if float(obj) != float(obj) else float(obj)
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return obj


def export_metrics_summary(payload: dict[str, Any], json_path: Path) -> None:
    """Save metrics payload to JSON file."""
    clean_data = sanitize_for_json(payload)

    json_file = Path(json_path)
    json_file.parent.mkdir(parents=True, exist_ok=True)

    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(clean_data, f, ensure_ascii=False, indent=2)

    logger.info(f"Exported metrics summary: {json_file.name} ({json_file.stat().st_size:,} bytes)")
