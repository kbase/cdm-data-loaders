# ruff: noqa: INP001
"""Render ``docs/<pipeline>_pipeline_specification.md`` from ``specs/pipelines/<pipeline>`` manifests.

Usage::

    uv run python -m scripts.gen_pipeline_spec          # write all pipeline specs
    uv run python -m scripts.gen_pipeline_spec --check  # exit 1 if any spec is stale
"""

import argparse
import gzip
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import yaml

from tests.pipeline_spec import (
    PipelineCase,
    PipelineRun,
    RuleCatalogue,
    Section,
    load_pipeline_cases,
    load_rules,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SPECS_ROOT = REPO_ROOT / "specs" / "pipelines"
DOCS_ROOT = REPO_ROOT / "docs"


@dataclass(frozen=True, slots=True)
class PipelineSpecConfig:
    """Configuration for generating a single pipeline specification Markdown document."""

    name: str
    data_dir: Path
    output_path: Path


PUBMED_PIPELINE_SPEC = PipelineSpecConfig(
    name="pubmed",
    data_dir=SPECS_ROOT / "pubmed",
    output_path=DOCS_ROOT / "pubmed_pipeline_specification.md",
)

NCBI_DATASETS_PIPELINE_SPEC = PipelineSpecConfig(
    name="ncbi_datasets",
    data_dir=SPECS_ROOT / "ncbi_datasets",
    output_path=DOCS_ROOT / "ncbi_datasets_pipeline_specification.md",
)

PIPELINE_SPECS: dict[str, PipelineSpecConfig] = {
    spec.name: spec
    for spec in (
        PUBMED_PIPELINE_SPEC,
        NCBI_DATASETS_PIPELINE_SPEC,
    )
}

FENCE_LANG = {
    ".json": "json",
    ".jsonl": "json",
    ".md5": "text",
    ".py": "python",
    ".toml": "toml",
    ".xml": "xml",
    ".yaml": "yaml",
    ".yml": "yaml",
}


def _fenced_code(content: str, lang: str) -> str:
    stripped = content.strip()
    fence = "````" if "```" in stripped else "```"
    return f"{fence}{lang}\n{stripped}\n{fence}"


def _fenced_file(path: Path) -> tuple[str, bool]:
    """Return ``(fenced_markdown, decompressed_flag)`` for ``path``."""
    raw = path.read_bytes()
    decompressed = False
    suffix = path.suffix.lower()
    if suffix == ".gz":
        raw = gzip.decompress(raw)
        decompressed = True
        inner_suffix = Path(path.stem).suffix.lower()
        lang = FENCE_LANG.get(inner_suffix, "text")
    else:
        lang = FENCE_LANG.get(suffix, "text")
    return _fenced_code(raw.decode("utf-8"), lang), decompressed


def _render_section(section: Section, cases_by_rule: dict[str, list[PipelineCase]]) -> str:
    parts: list[str] = [f"## {section.title}"]
    if section.intro:
        parts.append(section.intro)
    if section.rules:
        bullet_lines: list[str] = []
        for rule in section.rules:
            refs = ", ".join(f"[`{c.name}`](#{c.anchor})" for c in cases_by_rule.get(rule.id, []))
            suffix = f" *(tested in {refs})*" if refs else ""
            bullet_lines.append(f"- **`{rule.id}`** — {rule.statement}{suffix}")
        parts.append("\n".join(bullet_lines))
    return "\n\n".join(parts)


def _render_run_report_summary(run: PipelineRun) -> str:
    report_dict: dict[str, object] = {
        "status": run.expected_report.status,
        "pipeline_load_info_errs": run.expected_report.pipeline_load_info_errs,
    }
    if run.expected_report.ingested_files:
        report_dict["ingested_files"] = list(run.expected_report.ingested_files)
    if run.expected_report.load_errors:
        report_dict["load_errors"] = list(run.expected_report.load_errors)
    report_yaml = yaml.safe_dump(report_dict, sort_keys=False).strip()
    return f"**Expected Report (`report.txt` summary)**\n\n{_fenced_code(report_yaml, 'yaml')}"


def _render_case(case: PipelineCase, rules: RuleCatalogue) -> str:
    rule_badges = ", ".join(f"`{rid}`" for rid in case.rules if rid in rules.rules)
    parts: list[str] = [
        f'<a id="{case.anchor}"></a>\n### `{case.name}` — {case.title}',
        f"**Exercises:** {rule_badges}",
    ]
    if case.notes:
        parts.append(case.notes)

    multi_run = len(case.runs) > 1
    for run in case.runs:
        prefix = f"#### Run `{run.id}`\n\n" if multi_run else ""
        run_parts: list[str] = []

        if run.settings_file is not None and run.settings_file.is_file():
            settings_fence, _ = _fenced_file(run.settings_file)
            run_parts.append(
                f"{prefix}**Settings (`{run.settings_file.name}`)**\n\n{settings_fence}"
            )
            prefix = ""

        input_files = sorted(p for p in run.input_dir.rglob("*") if p.is_file())
        for in_file in input_files:
            rel_in = in_file.relative_to(case.directory).as_posix()
            in_fence, decompressed = _fenced_file(in_file)
            label_note = " (decompressed)" if decompressed else ""
            run_parts.append(f"{prefix}**Input (`{rel_in}`{label_note})**\n\n{in_fence}")
            prefix = ""

        run_parts.append(f"{prefix}{_render_run_report_summary(run)}")
        prefix = ""

        if run.expected_tables is not None and run.expected_tables.is_file():
            tables_fence, _ = _fenced_file(run.expected_tables)
            run_parts.append(
                f"**Expected PyArrow Tables (`{run.expected_tables.name}`)**\n\n{tables_fence}"
            )

        if run.expected_json is not None and run.expected_json.is_file():
            json_fence, _ = _fenced_file(run.expected_json)
            run_parts.append(
                f"**Expected JSON (`{run.expected_json.name}`, non-null fields)**\n\n{json_fence}"
            )

        parts.extend(run_parts)

    return "\n\n".join(parts)


def render(spec: PipelineSpecConfig = PUBMED_PIPELINE_SPEC) -> str:
    """Render the full Markdown specification document for ``spec``."""
    rules = load_rules(spec.data_dir / "rules.yaml")
    cases = load_pipeline_cases(spec.data_dir)

    cases_by_rule: dict[str, list[PipelineCase]] = defaultdict(list)
    for case in cases:
        for rid in case.rules:
            cases_by_rule[rid].append(case)

    rel_data_dir = spec.data_dir.relative_to(REPO_ROOT).as_posix()
    banner = (
        f"<!-- AUTO-GENERATED from {rel_data_dir} by scripts/gen_pipeline_spec.py; "
        "do not edit by hand. Run `uv run python -m scripts.gen_pipeline_spec` to regenerate. -->"
    )
    blocks: list[str] = [
        banner,
        f"# {rules.title}\n\n{rules.intro}",
        *(_render_section(section, cases_by_rule) for section in rules.sections),
        "## Smoke Test Cases\n\n" + "\n\n---\n\n".join(_render_case(c, rules) for c in cases),
    ]
    return "\n\n---\n\n".join(blocks) + "\n"


def main(argv: list[str] | None = None) -> int:
    """Write or check ``docs/<pipeline>_pipeline_specification.md`` for one or all pipeline specs."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pipeline",
        choices=[*PIPELINE_SPECS.keys(), "all"],
        default="all",
        help="Pipeline specification to render (default: all).",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify the specification file(s) on disk match the rendered output.",
    )
    args = parser.parse_args(argv)

    selected = list(PIPELINE_SPECS.values()) if args.pipeline == "all" else [PIPELINE_SPECS[args.pipeline]]
    stale = False
    for spec in selected:
        rendered = render(spec)
        if args.check:
            if not spec.output_path.exists() or spec.output_path.read_text(encoding="utf-8") != rendered:
                display_path = (
                    spec.output_path.relative_to(REPO_ROOT)
                    if spec.output_path.is_relative_to(REPO_ROOT)
                    else spec.output_path
                )
                sys.stderr.write(
                    f"{display_path} is out of date; "
                    "run `uv run python -m scripts.gen_pipeline_spec`.\n"
                )
                stale = True
        else:
            spec.output_path.write_text(rendered, encoding="utf-8")
    return 1 if stale else 0


if __name__ == "__main__":
    raise SystemExit(main())
