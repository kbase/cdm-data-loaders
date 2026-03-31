#!/usr/bin/env bash
set -euo pipefail

# Ensure at least one argument is provided
if [ "$#" -eq 0 ]; then
  echo "Usage: $0 {uniref|uniprot|xml_split|test} [args...]"
  exit 1
fi

cmd="$1"
shift

case "$cmd" in
  xml_split)
    # Run the xml_file_splitter app
    exec /usr/bin/tini -- xml_file_splitter "$@"
    ;;
  uniref)
    # Run the uniref pipeline with any additional arguments
    exec /usr/bin/tini -- uv run --no-sync uniref "$@"
    ;;
  uniprot)
    # Run the uniprot pipeline with any additional arguments
    exec /usr/bin/tini -- uv run --no-sync uniprot "$@"
    ;;
  # ncbi_rest_api)
  #   # Run the NCBI datasets API importer
  #   exec /usr/bin/tini -- uv run --no-sync ncbi_rest_api "$@"
  #   ;;
  test)
    # run the tests
    exec /usr/bin/tini -- uv run --no-sync pytest -m "not requires_spark"
    ;;
  bash)
    exec /usr/bin/tini -- /bin/bash
    ;;
  *)
    echo "Error: unknown command '$cmd'; valid commands are 'uniref', 'uniprot', or 'xml_split'." >&2
    exit 1
    ;;
esac
