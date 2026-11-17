"""Extract per-genome file paths for a given key from a genome index JSON file."""

import json
import sys
from pathlib import Path
from typing import Final

N_EXPECTED_ARGS: Final[int] = 3


def main() -> None:
    """Extract values associated with a target key from a JSON dictionary file and print one per line.

    Usage::

        python scripts/ops/parse_index.py <input_json_file> <key>
    """
    if len(sys.argv) < N_EXPECTED_ARGS:
        print("Usage: parse_index.py <input_json_file> <key>")
        sys.exit(1)

    input_file = sys.argv[1]
    target_key = sys.argv[2]

    try:
        with Path(input_file).open(encoding="utf-8") as file:
            data = json.load(file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error reading JSON file: {exc}")
        sys.exit(1)

    if not isinstance(data, dict):
        print("Error: expected JSON file to be a dictionary")
        sys.exit(1)

    extracted_paths = [entry[target_key] for entry in data.values() if isinstance(entry, dict) and target_key in entry]

    for path in extracted_paths:
        print(path)


if __name__ == "__main__":
    main()
