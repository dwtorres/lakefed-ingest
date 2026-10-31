#!/usr/bin/env bash
# Deploy gate (constitution IX, FR-038): the name test, then the bundle validation.
# Usage: scripts/validate.sh -t <target> -p <profile>
set -euo pipefail
cd "$(dirname "$0")/.."
pytest tests/job_parameters_test.py -q
databricks bundle validate "$@"
