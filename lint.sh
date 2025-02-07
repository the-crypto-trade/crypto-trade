#!/usr/bin/env bash

set -e
set -o pipefail
echo "Running lint.sh..."

echo "Running flake8..."
find . -type f \( -path "./src/*" -o -path "./tests/*" -o -path "./examples/*" \) -not -path "*/__pycache__/*" -not -path "*/crypto_trade.egg-info/*" -name "*.py" -not -name "bybit.py" -print -exec flake8 --max-line-length 160 --extend-ignore E203 {} \+

echo "Running mypy..."
find . -type f \( -path "./src/*" -o -path "./tests/*" -o -path "./examples/*" \) -not -path "*/__pycache__/*" -not -path "*/crypto_trade.egg-info/*" -name "*.py" -not -name "bybit.py" -print -exec mypy {} \+

echo "Running pylint..."
find . -type f \( -path "./src/*" -o -path "./tests/*" -o -path "./examples/*" \) -not -path "*/__pycache__/*" -not -path "*/crypto_trade.egg-info/*" -name "*.py" -not -name "bybit.py" -print -exec pylint --errors-only {} \+

echo "Done."
