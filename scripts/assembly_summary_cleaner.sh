#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<EOF
Usage: $0 -d <directory> -p <pattern> -n <num_columns> [-r]

  -d  Directory to search for files
  -p  Filename pattern to match (quote it! e.g. '*.tsv', 'summary_*')
  -n  Expected number of columns
  -r  Recurse into subdirectories (optional, default: top-level only)

Example:
  $0 -d ./data -p '*.tsv' -n 38
EOF
    exit 1
}

recursive=0
while getopts ":d:p:n:r" opt; do
    case "$opt" in
        d) dir="$OPTARG" ;;
        p) pattern="$OPTARG" ;;
        n) n="$OPTARG" ;;
        r) recursive=1 ;;
        *) usage ;;
    esac
done

[[ -z "${dir:-}" || -z "${pattern:-}" || -z "${n:-}" ]] && usage
[[ -d "$dir" ]] || { echo "Error: '$dir' is not a directory" >&2; exit 1; }
[[ "$n" =~ ^[0-9]+$ ]] || { echo "Error: -n must be a positive integer" >&2; exit 1; }

# Whether to recurse into subdirectories
if [[ "$recursive" -eq 1 ]]; then
    depth_args=()
else
    depth_args=(-maxdepth 1)
fi

find "$dir" "${depth_args[@]}" -type f -name "$pattern" -print0 |
while IFS= read -r -d '' file; do
    echo "Processing: $file"

    # Strip existing extension and append suffix, e.g. data.tsv -> data-valid.tsv
    valid_file="${file%.*}-valid.tsv"
    error_file="${file%.*}-errors.tsv"

    # Truncate output files in case of re-run
    : > "$valid_file"
    : > "$error_file"

    awk -F'\t' -v OFS='\t' \
        -v ncols="$n" \
        -v valid="$valid_file" \
        -v errors="$error_file" '
        # step 1: drop the very first line entirely if it starts with "##"
        NR == 1 {
            if ($0 ~ /^##/) next
        }

        # step 2: strip a single leading "#" from whichever line ends up
        # being first after step 1 has been applied (i.e. original line 1
        # if it was not a "##" line, otherwise original line 2)
        !header_checked {
            header_checked = 1
            if (substr($0, 1, 1) == "#") $0 = substr($0, 2)
        }

        # Column-count validation (applies to every remaining line,
        # including the possibly-modified header line)
        {
            if (NF == ncols) {
                print > valid
            } else {
                print > errors
            }
        }
    ' "$file"

    echo "  -> Valid:  $valid_file"
    echo "  -> Errors: $error_file"
done
