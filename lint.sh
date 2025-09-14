#!/usr/bin/env bash

set -e
set -o pipefail
echo "Running lint.sh..."

VERBOSE=false

# Check for -v flag
while getopts "v" opt; do
  case $opt in
    v)
      VERBOSE=true
      ;;
  esac
done

echo "Running lint.sh..."

# Choose find print behavior
PRINT_OPTION=""
if [ "$VERBOSE" = true ]; then
  PRINT_OPTION="-print"
fi

echo "Running flake8..."
find . -type f \( -path "./src/*" -o -path "./tests/*" -o -path "./examples/*" -o -path "./applications/*" \) -not -path "*/__pycache__/*" -not -path "*/crypto_trade.egg-info/*" -name "*.py" $PRINT_OPTION -exec flake8 --max-line-length 160 --extend-ignore E203 {} \+

echo "Running mypy..."
find . -type f \( -path "./src/*" -o -path "./tests/*" -o -path "./applications/*" \) -not -path "*/__pycache__/*" -not -path "*/crypto_trade.egg-info/*" -name "*.py" $PRINT_OPTION -exec mypy {} \+

echo "Running pylint..."
find . -type f \( -path "./src/*" -o -path "./tests/*" -o -path "./applications/*" \) -not -path "*/__pycache__/*" -not -path "*/crypto_trade.egg-info/*" -name "*.py" $PRINT_OPTION -exec pylint --errors-only {} \+

echo "Done."
