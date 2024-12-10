#!/usr/bin/env bash

set -e
set -o pipefail

echo "Running format.sh..."

echo "Running black..."
find . -type f -not -path "*/.venv*/*" -not -path "*/__pycache__/*" -not -path "*/build/*" -name "*.py" -exec black --line-length 160 {} \+

echo "Running isort..."
find . -type f -not -path "*/.venv*/*" -not -path "*/__pycache__/*" -not -path "*/build/*" -name "*.py" -exec isort --profile black {} \+

echo "Running autoflake..."
find . -type f -not -path "*/.venv*/*" -not -path "*/__pycache__/*" -not -path "*/build/*" -name "*.py" -exec autoflake --in-place --remove-all-unused-imports --remove-unused-variables {} \+

echo "Done."
