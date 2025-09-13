#!/usr/bin/env bash

set -e
set -o pipefail

VERBOSE=""

# Check for -v flag
while getopts "v" opt; do
  case $opt in
    v)
      VERBOSE="-v"
      ;;
  esac
done

./format.sh $VERBOSE

format_result=$(git ls-files -m)
staged_files=$(git diff --name-only --cached)

IFS=$'\n'
set -f
intersection=($(comm -12 <(
    printf '%s\n' "${format_result[@]}" | sort
) <(
    printf '%s\n' "${staged_files[@]}" | sort
)))

if [ -z "${intersection}" ]; then
    echo "Passed format."
else
    echo "Failed format due to ${intersection}."
    exit 1
fi

./lint.sh $VERBOSE

if ./lint.sh; then
    echo "Passed lint."
else
    echo "Failed lint."
    exit 1
fi
