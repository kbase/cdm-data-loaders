#!/usr/bin/env bash
set -euo pipefail

supported_commands="all_the_bacteria|xml_split|uniref|uniprot|ncbi_rest_api|ncbi_ftp_sync|test|integration-test|bash"

# Ensure at least one argument is provided
if [ "$#" -eq 0 ]; then
  echo "Usage: $0 {$supported_commands} [args...]"
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
  ncbi_ftp_sync)
    # Run the NCBI FTP assembly download pipeline (Phase 2)
    exec /usr/bin/tini -- uv run --no-sync ncbi_ftp_sync "$@"
    ;;
  test)
    # run the tests
    exec /usr/bin/tini -- uv run --no-sync pytest -m "not requires_spark"
    ;;
  integration-test)
    # run the integration tests (requires a running MinIO instance)
    exec /usr/bin/tini -- uv run --no-sync pytest -m "integration" -v "$@"
    ;;
  bash)
    exec /usr/bin/tini -- /bin/bash
    ;;
  *)
    echo "Error: unknown command '$cmd'; valid commands are {$supported_commands}." >&2
    exit 1
    ;;
esac
