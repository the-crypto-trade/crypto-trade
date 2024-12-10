#!/usr/bin/env bash

set -e
set -o pipefail
echo "Running lint.sh..."

echo "Running flake8..."
find . -type f -not -path "*/.venv*/*" -not -path "*/__pycache__/*" -not -path "*/build/*" -name "*.py" -exec flake8 --max-line-length 160 --extend-ignore E203 {} \+

echo "Running mypy..."
find . -type f -not -path "*/.venv*/*" -not -path "*/__pycache__/*" -not -path "*/build/*" -name "*.py" -exec mypy {} \+

echo "Running pylint..."
find . -type f -not -path "*/.venv*/*" -not -path "*/__pycache__/*" -not -path "*/build/*" -name "*.py" -exec pylint --errors-only {} \+

echo "Done."
