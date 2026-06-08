#!/usr/bin/env bash
set -euo pipefail

VALID_COMMANDS=(all_the_bacteria ncbi_ftp_sync ncbi_rest_api uniprot uniref xml_split test ceph_integration_test bash)

usage() {
  local joined
  joined=$(IFS='|'; echo "${VALID_COMMANDS[*]}")
  echo "Usage: $0 {${joined}} [args...]" >&2
}

if [ "$#" -eq 0 ]; then
  usage
  exit 1
fi

cmd="$1"
shift

case "$cmd" in
  all_the_bacteria)
    exec /usr/bin/tini -- uv run --no-sync all_the_bacteria "$@"
    ;;
  ncbi_ftp_sync)
    # Run the NCBI FTP assembly download pipeline (Phase 2)
    exec /usr/bin/tini -- uv run --no-sync ncbi_ftp_sync "$@"
    ;;
  ncbi_rest_api)
    exec /usr/bin/tini -- uv run --no-sync ncbi_rest_api "$@"
    ;;
  uniprot)
    exec /usr/bin/tini -- uv run --no-sync uniprot "$@"
    ;;
  uniref)
    exec /usr/bin/tini -- uv run --no-sync uniref "$@"
    ;;
  xml_split)
    exec /usr/bin/tini -- xml_file_splitter "$@"
    ;;
  test)
    exec /usr/bin/tini -- uv run --no-sync pytest -m "not requires_spark and not requires_ceph"
    ;;
  ceph_integration_test)
    # run the CEPH-backed integration tests (requires a running CEPH instance)
    exec /usr/bin/tini -- uv run --no-sync pytest -m "requires_ceph" -v "$@"
    ;;
  bash)
    exec /usr/bin/tini -- /bin/bash
    ;;
  *)
    echo "Error: unknown command '$cmd'." >&2
    usage
    exit 1
    ;;
esac
