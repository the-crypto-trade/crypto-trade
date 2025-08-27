#!/usr/bin/env bash

set -e
set -o pipefail

echo "Running format.sh..."

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

echo "Running black..."
find . -type f \( -path "./src/*" -o -path "./tests/*" -o -path "./examples/*" -o -path "./applications/*" \) -not -path "*/__pycache__/*" -not -path "*/crypto_trade.egg-info/*" -name "*.py" $PRINT_OPTION -exec black --line-length 160 {} \+

echo "Running isort..."
find . -type f \( -path "./src/*" -o -path "./tests/*" -o -path "./examples/*" -o -path "./applications/*" \) -not -path "*/__pycache__/*" -not -path "*/crypto_trade.egg-info/*" -name "*.py" $PRINT_OPTION -exec isort --profile black {} \+

echo "Running autoflake..."
find . -type f \( -path "./src/*" -o -path "./tests/*" -o -path "./examples/*" -o -path "./applications/*" \) -not -path "*/__pycache__/*" -not -path "*/crypto_trade.egg-info/*" -name "*.py" $PRINT_OPTION -exec autoflake --in-place --remove-all-unused-imports --remove-unused-variables {} \+

echo "Running autopep8..."
find . -type f \( -path "./src/*" -o -path "./tests/*" -o -path "./examples/*" -o -path "./applications/*" \) -not -path "*/__pycache__/*" -not -path "*/crypto_trade.egg-info/*" -name "*.py" $PRINT_OPTION -exec autopep8 --in-place --max-line-length 160  --select=E231 {} \+

echo "Done."
