"""Render ``docs/<family>_specification.md`` from ``specs/pipelines/<family>`` manifests.

Usage::

    uv run python -m scripts.gen_pipeline_spec          # write all pipeline family specs
    uv run python -m scripts.gen_pipeline_spec --check  # exit 1 if any spec is stale
"""

import argparse
from collections.abc import Sequence

from scripts.gen_converter_spec import (
    PIPELINE_SPECS,
)
from scripts.gen_converter_spec import (
    render_pipeline_spec as render,
)
from scripts.gen_docs import REPO_ROOT, check_or_write_files


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for generating or checking pipeline specification Markdown files.

    :param argv: Optional sequence of CLI arguments (defaults to ``sys.argv[1:]``).
    :type argv: Sequence[str] | None
    :return: Process exit code (``0`` on success, ``1`` if ``--check`` detects stale files).
    :rtype: int
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pipeline",
        choices=[*PIPELINE_SPECS.keys(), "all"],
        default="all",
        help="Pipeline family specification to render (default: all).",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify the specification file(s) on disk match the rendered output.",
    )
    args = parser.parse_args(argv)

    selected = list(PIPELINE_SPECS.values()) if args.pipeline == "all" else [PIPELINE_SPECS[args.pipeline]]
    return check_or_write_files(
        ((spec.output_path, render(spec)) for spec in selected),
        check=args.check,
        repo_root=REPO_ROOT,
    )


if __name__ == "__main__":
    raise SystemExit(main())
