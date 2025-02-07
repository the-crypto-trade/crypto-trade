#!/usr/bin/env bash

set -e
set -o pipefail

echo "Running format.sh..."

echo "Running black..."
find . -type f \( -path "./src/*" -o -path "./tests/*" -o -path "./examples/*" \) -not -path "*/__pycache__/*" -not -path "*/crypto_trade.egg-info/*" -name "*.py" -print -exec black --line-length 160 {} \+

echo "Running isort..."
find . -type f \( -path "./src/*" -o -path "./tests/*" -o -path "./examples/*" \) -not -path "*/__pycache__/*" -not -path "*/crypto_trade.egg-info/*" -name "*.py" -print -exec isort --profile black {} \+

echo "Running autoflake..."
find . -type f \( -path "./src/*" -o -path "./tests/*" -o -path "./examples/*" \) -not -path "*/__pycache__/*" -not -path "*/crypto_trade.egg-info/*" -name "*.py" -print -exec autoflake --in-place --remove-all-unused-imports --remove-unused-variables {} \+

echo "Running autopep8..."
find . -type f \( -path "./src/*" -o -path "./tests/*" -o -path "./examples/*" \) -not -path "*/__pycache__/*" -not -path "*/crypto_trade.egg-info/*" -name "*.py" -print -exec autopep8 --in-place --max-line-length 160  --select=E231 {} \+

echo "Done."
