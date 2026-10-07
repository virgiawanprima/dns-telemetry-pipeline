"""Integration tests for CLI entrypoint and pipeline execution."""

from pathlib import Path

import pandas as pd

from src.cli import run_pipeline


def test_cli_run_pipeline_end_to_end(sample_dns_df: pd.DataFrame, tmp_path: Path):
    """Verify end-to-end pipeline execution from CSV input to output JSON."""
    csv_file = tmp_path / "test_telemetry.csv"
    output_dir = tmp_path / "output"

    # Export sample dataframe to CSV
    sample_dns_df.to_csv(csv_file, index=False)

    summary = run_pipeline(
        input_path=csv_file,
        output_dir=output_dir,
        sample_size=None,
        engine="pandas",
        validate=True,
    )

    assert summary is not None
    assert "meta" in summary
    assert "kpi" in summary
    assert "temporal" in summary
    assert "pareto" in summary
    assert (output_dir / "metrics_summary.json").exists()
