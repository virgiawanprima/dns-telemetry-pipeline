"""Entrypoint wrapper for DNS Telemetry Pipeline.

This file provides a lightweight root-level runner that forwards
directly to the modular `src.cli` package.
"""

import sys
from pathlib import Path

# Ensure project root is available on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.cli import main

if __name__ == "__main__":
    main()
