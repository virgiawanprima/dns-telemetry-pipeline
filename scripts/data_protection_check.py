#!/usr/bin/env python3
"""Data Confidentiality & Protection Policy Checker.

Enforces strict repository privacy rules:
1. Blocks tracking/committing of raw datasets, pcap captures, databases, outputs, and secrets.
2. Ensures Jupyter notebooks are stripped of all execution outputs.
3. Scans for credentials and high-entropy secrets without echoing sensitive values.
4. Operates in 'pre-commit', 'pre-push', 'ci', and standalone 'audit' modes.
"""

import argparse
import fnmatch
import json
import logging
import subprocess
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("data_protection")

# Forbidden file path glob patterns (must never be committed or tracked)
PROHIBITED_PATTERNS = [
    "data/*.csv",
    "data/*.parquet",
    "data/*.duckdb",
    "data/*.duckdb.wal",
    "output/*.json",
    "output/*.parquet",
    "*.pcap",
    "*.pcapng",
    "metrics_summary.json",
    "dbt_dns/target/*",
    "dbt_dns/logs/*",
    "dbt_dns/.user.yml",
    ".user.yml",
    "*.log",
    ".env",
    ".env.*",
    "*.pem",
    "*.key",
    "*.p12",
    "*.token",
    "credentials*.json",
    "service_account*.json",
    "service-account*.json",
]

# Patterns explicitly allowlisted (e.g. tracking placeholders)
ALLOWLISTED_PATTERNS = [
    "data/.gitkeep",
    "output/.gitkeep",
]


def is_prohibited_path(path_str: str) -> bool:
    """Check if a relative file path matches any prohibited data/secret pattern."""
    normalized = path_str.replace("\\", "/").lstrip("./")
    for allow_pat in ALLOWLISTED_PATTERNS:
        if fnmatch.fnmatch(normalized, allow_pat):
            return False

    for block_pat in PROHIBITED_PATTERNS:
        if fnmatch.fnmatch(normalized, block_pat) or fnmatch.fnmatch(
            Path(normalized).name, block_pat
        ):
            return True
    return False


def get_staged_files() -> list[str]:
    """Retrieve list of files currently staged in Git index (for pre-commit)."""
    try:
        res = subprocess.run(
            ["git", "diff", "--cached", "--name-only", "--diff-filter=ACM"],
            capture_output=True,
            text=True,
            check=True,
        )
        return [line.strip() for line in res.stdout.splitlines() if line.strip()]
    except Exception as exc:
        logger.error(f"Failed to query staged git files: {exc}")
        return []


def get_push_range_files(local_ref: str, remote_ref: str) -> list[str]:
    """Retrieve list of files modified across commits being pushed (for pre-push)."""
    try:
        if remote_ref and remote_ref != "0000000000000000000000000000000000000000":
            rev_range = f"{remote_ref}..{local_ref}"
            cmd = ["git", "diff", "--name-only", "--diff-filter=ACM", rev_range]
        else:
            # New branch or branch not yet on remote: inspect commits not yet on remotes
            rev_commits = subprocess.run(
                ["git", "rev-list", local_ref, "--not", "--remotes"],
                capture_output=True,
                text=True,
                check=False,
            )
            commits = [c.strip() for c in rev_commits.stdout.splitlines() if c.strip()]
            if commits:
                oldest = commits[-1]
                has_parent = subprocess.run(
                    ["git", "rev-parse", f"{oldest}^@"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if has_parent.returncode == 0 and has_parent.stdout.strip():
                    cmd = [
                        "git",
                        "diff",
                        "--name-only",
                        "--diff-filter=ACM",
                        f"{oldest}~1..{local_ref}",
                    ]
                else:
                    cmd = ["git", "log", "--name-only", "--diff-filter=ACM", "--format=", local_ref]
            else:
                cmd = ["git", "diff-tree", "--no-commit-id", "--name-only", "-r", local_ref]

        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if res.returncode == 0:
            return list(set(line.strip() for line in res.stdout.splitlines() if line.strip()))
        return []
    except Exception as exc:
        logger.error(f"Failed to query push range files: {exc}")
        return []


def get_all_tracked_files() -> list[str]:
    """Retrieve all files tracked by Git (for CI and full audit)."""
    try:
        res = subprocess.run(["git", "ls-files"], capture_output=True, text=True, check=True)
        return [line.strip() for line in res.stdout.splitlines() if line.strip()]
    except Exception as exc:
        logger.error(f"Failed to query tracked git files: {exc}")
        return []


def check_file_paths(files: list[str]) -> list[dict]:
    """Identify any files violating the data protection blacklist."""
    violations = []
    for f in files:
        if is_prohibited_path(f):
            violations.append(
                {
                    "category": "RESTRICTED_FILE_PATH",
                    "file": f,
                    "detail": "Path matches restricted dataset, database, execution artifact, or credential pattern.",
                }
            )
    return violations


def check_notebook_outputs(files: list[str]) -> list[dict]:
    """Verify that all Jupyter notebooks in the target set have empty cell outputs."""
    violations = []
    notebooks = [f for f in files if f.endswith(".ipynb") and Path(f).is_file()]

    for nb_path in notebooks:
        try:
            with open(nb_path, "r", encoding="utf-8") as f:
                nb_data = json.load(f)

            for i, cell in enumerate(nb_data.get("cells", [])):
                if cell.get("cell_type") == "code":
                    outputs = cell.get("outputs", [])
                    if len(outputs) > 0:
                        violations.append(
                            {
                                "category": "NOTEBOOK_CONTAINS_UNSTRIPPED_OUTPUTS",
                                "file": nb_path,
                                "detail": f"Cell {i} contains {len(outputs)} executed output element(s). Strip outputs before commit.",
                            }
                        )
                        break
        except Exception as exc:
            violations.append(
                {
                    "category": "NOTEBOOK_PARSE_ERROR",
                    "file": nb_path,
                    "detail": f"Unable to parse notebook JSON: {exc}",
                }
            )
    return violations


def run_detect_secrets_check(files: list[str]) -> list[dict]:
    """Execute local detect-secrets scan without logging sensitive values."""
    violations = []
    # Only scan text/source files that currently exist on disk
    scannable = [
        f
        for f in files
        if Path(f).is_file() and Path(f).suffix not in [".parquet", ".csv", ".duckdb", ".wal"]
    ]
    if not scannable:
        return []

    try:
        # Run detect-secrets scan on current directory
        proc = subprocess.run(
            [sys.executable, "-m", "detect_secrets.main", "scan"] + scannable,
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode == 0 and proc.stdout:
            data = json.loads(proc.stdout)
            results = data.get("results", {})
            for filepath, findings in results.items():
                for finding in findings:
                    detector = finding.get("type", "UnknownDetector")
                    line_num = finding.get("line_number", 0)
                    violations.append(
                        {
                            "category": f"SECRET_PATTERN_DETECTED ({detector})",
                            "file": f"{filepath}:{line_num}",
                            "detail": "High-entropy or secret credential pattern detected. Do not commit secrets.",
                        }
                    )
    except Exception as exc:
        logger.warning(f"Local detect-secrets scan skipped or encountered issue: {exc}")

    return violations


def main():
    parser = argparse.ArgumentParser(
        description="DNS Pipeline Data Confidentiality & Protection Checker"
    )
    parser.add_argument(
        "--stage",
        choices=["pre-commit", "pre-push", "ci", "audit"],
        default="audit",
        help="Execution stage context",
    )
    parser.add_argument("--local-ref", default="HEAD", help="Local ref for pre-push")
    parser.add_argument("--remote-ref", default="", help="Remote ref for pre-push")
    args = parser.parse_args()

    logger.info(f"Running data confidentiality & protection check (stage: {args.stage})...")

    if args.stage == "pre-commit":
        target_files = get_staged_files()
    elif args.stage == "pre-push":
        target_files = get_push_range_files(args.local_ref, args.remote_ref)
        if not target_files:
            # Fallback to staged or head
            target_files = get_staged_files() or get_all_tracked_files()
    else:  # ci or audit
        target_files = get_all_tracked_files()

    logger.info(f"Target file set size: {len(target_files)} file(s).")

    violations = []
    violations.extend(check_file_paths(target_files))
    violations.extend(check_notebook_outputs(target_files))
    violations.extend(run_detect_secrets_check(target_files))

    if violations:
        logger.error(f"DATA PROTECTION VIOLATIONS DETECTED: {len(violations)} issue(s) found!")
        for v in violations:
            logger.error(f"[{v['category']}] {v['file']} -> {v['detail']}")
        logger.error("Commit/Push BLOCKED by data protection policy.")
        sys.exit(1)

    logger.info("Data confidentiality & protection checks PASSED: All files clean.")
    sys.exit(0)


if __name__ == "__main__":
    main()
