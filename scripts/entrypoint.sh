#!/usr/bin/env bash
set -euo pipefail

# Ensure at least one argument is provided
if [ "$#" -eq 0 ]; then
  echo "Usage: $0 {all_the_bacteria|ncbi_rest_api|uniprot|uniref|xml_split|test} [args...]"
  exit 1
fi

cmd="$1"
shift

case "$cmd" in
  all_the_bacteria)
    # All the Bacteria file importer
    exec /usr/bin/tini -- uv run --no-sync all_the_bacteria "$@"
    ;;
  ncbi_rest_api)
    # Run the NCBI datasets API importer
    exec /usr/bin/tini -- uv run --no-sync ncbi_rest_api "$@"
    ;;
  uniprot)
    # Run the uniprot pipeline with any additional arguments
    exec /usr/bin/tini -- uv run --no-sync uniprot "$@"
    ;;
  uniref)
    # Run the uniref pipeline with any additional arguments
    exec /usr/bin/tini -- uv run --no-sync uniref "$@"
    ;;
  xml_split)
    # Run the xml_file_splitter app
    exec /usr/bin/tini -- xml_file_splitter "$@"
    ;;
  test)
    # run the tests
    exec /usr/bin/tini -- uv run --no-sync pytest -m "not requires_spark"
    ;;
  bash)
    exec /usr/bin/tini -- /bin/bash
    ;;
  *)
    echo "Error: unknown command '$cmd'; valid commands are 'all_the_bacteria', 'ncbi_rest_api', 'uniprot', 'uniref', or 'xml_split'." >&2
    exit 1
    ;;
esac
