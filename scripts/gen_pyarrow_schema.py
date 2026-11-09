"""Regenerate or check committed PyArrow schema modules from bundled XSD schemas.

Usage::

    uv run python -m scripts.gen_pyarrow_schema            # write every configured schema.py in CONFIGS
    uv run python -m scripts.gen_pyarrow_schema --check    # exit 1 if a committed schema.py is stale
"""

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Final

from cdm_data_loaders.converters.cli.xsd_to_pyarrow import (
    SchemaCodegenConfig,
    render_schema_module,
)
from scripts.gen_docs import REPO_ROOT, check_or_write_files, rel_posix

SCHEMAS_DIR: Final[Path] = REPO_ROOT / "src" / "cdm_data_loaders" / "schemas"

UNIREF_SCHEMA_CONFIG: Final[SchemaCodegenConfig] = SchemaCodegenConfig(
    xsd_file=SCHEMAS_DIR / "uniref" / "uniref.xsd",
    output=SCHEMAS_DIR / "uniref" / "schema.py",
    docstring="PyArrow schema and reusable struct components for UniRef (xml_tag='entry').",
    xml_level=1,
)

UNIPROT_SCHEMA_CONFIG: Final[SchemaCodegenConfig] = SchemaCodegenConfig(
    xsd_file=SCHEMAS_DIR / "uniprot" / "uniprot.xsd",
    output=SCHEMAS_DIR / "uniprot" / "schema.py",
    docstring="PyArrow schemas and reusable struct components for UniProt XML.",
    xml_level=1,
)

PUBMED_SCHEMA_CONFIG: Final[SchemaCodegenConfig] = SchemaCodegenConfig(
    xsd_file=SCHEMAS_DIR / "pubmed" / "pubmed.xsd",
    output=SCHEMAS_DIR / "pubmed" / "schema.py",
    docstring="PyArrow schemas and reusable struct components for PubMed XML.",
    xml_level=1,
    root_tag="PubmedArticleSet",
)

CONFIGS: Final[tuple[SchemaCodegenConfig, ...]] = (
    UNIREF_SCHEMA_CONFIG,
    UNIPROT_SCHEMA_CONFIG,
    PUBMED_SCHEMA_CONFIG,
)

_rel = rel_posix


def main(argv: Sequence[str] | None = None) -> int:
    """Write or check every configured PyArrow schema module in ``CONFIGS``.

    :param argv: Optional sequence of CLI arguments (defaults to ``sys.argv[1:]``).
    :type argv: Sequence[str] | None
    :return: Process exit code (``0`` on success, ``1`` if ``--check`` detects stale files).
    :rtype: int
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if a committed schema.py is out of date")
    args = parser.parse_args(argv)

    return check_or_write_files(
        ((config.output, render_schema_module(config)) for config in CONFIGS),
        check=args.check,
        repo_root=REPO_ROOT,
    )


if __name__ == "__main__":
    sys.exit(main())
