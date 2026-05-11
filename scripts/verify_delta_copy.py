"""Verify that a Delta Lake table was copied intact by comparing source and destination."""

import os
import sys

from deltalake import DeltaTable

SOURCE = "s3://cdm-lake/tenant-general-warehouse/kbase/datasets/uniprot/cdm_schema/2025_03/uniprot_kb/"
DEST = "s3://cdm-lake/tenant-general-warehouse/refdata/datasets/uniprot/2025_03/"


def get_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("MINIO_ENDPOINT_URL")
    if not endpoint:
        sys.exit("MINIO_ENDPOINT_URL is not set")
    return {"endpoint_url": endpoint}


def load_table(path: str, storage_options: dict[str, str]) -> DeltaTable:
    try:
        return DeltaTable(path, storage_options=storage_options)
    except Exception as e:
        sys.exit(f"Failed to open Delta table at {path!r}: {e}")


def check(label: str, src_val, dst_val) -> bool:
    if src_val == dst_val:
        print(f"  [OK]  {label}")
        return True
    print(f"  [FAIL] {label}")
    print(f"         source : {src_val}")
    print(f"         dest   : {dst_val}")
    return False


def main() -> None:
    storage_options = get_storage_options()

    print(f"Loading source: {SOURCE}")
    src = load_table(SOURCE, storage_options)
    print(f"Loading destination: {DEST}")
    dst = load_table(DEST, storage_options)

    print("\n--- Verification results ---")
    results = [
        check("version", src.version(), dst.version()),
        check("schema", src.schema(), dst.schema()),
        check("file count", len(src.files()), len(dst.files())),
        check("file list", sorted(src.files()), sorted(dst.files())),
    ]

    print()
    if all(results):
        print("All checks passed. Tables appear identical.")
    else:
        failed = sum(1 for r in results if not r)
        print(f"{failed} check(s) failed. Tables may differ.")
        sys.exit(1)


if __name__ == "__main__":
    main()
