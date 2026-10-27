"""Generate converter specification documents from rule catalogues, case manifests and converter code.

Usage::

    uv run python -m scripts.gen_converter_spec            # write every docs/*_specification.md in CONFIGS
    uv run python -m scripts.gen_converter_spec --check    # exit 1 if a committed document is stale
"""

import argparse
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa

from cdm_data_loaders.converters.jsonschema_to_pyarrow import (
    JSONSCHEMA_TO_PYARROW_ERRORS,
    JSONSchemaToPyArrowError,
    resolve_scalar_type,
)
from cdm_data_loaders.readers.xsd import (
    BUILTIN_LIST_ITEM_TYPES,
    SCALAR_ARROW_TYPES,
    XSD_TO_PYARROW_ERRORS,
    XsdToPyArrowError,
)
from tests.converter_spec import Case, ErrorCase, RuleCatalogue, load_cases, load_error_cases, load_rules

REPO_ROOT = Path(__file__).resolve().parent.parent

GENERATED_BANNER = "<!-- GENERATED FILE - DO NOT EDIT. Source: {sources}. Regenerate with `uv run python -m scripts.gen_converter_spec`. -->"

_PA_CONSTRUCTORS: dict[str, str] = {
    "string": "pa.string()",
    "bool": "pa.bool_()",
    "int8": "pa.int8()",
    "int16": "pa.int16()",
    "int32": "pa.int32()",
    "int64": "pa.int64()",
    "uint8": "pa.uint8()",
    "uint16": "pa.uint16()",
    "uint32": "pa.uint32()",
    "uint64": "pa.uint64()",
    "double": "pa.float64()",
    "date32[day]": "pa.date32()",
    "binary": "pa.binary()",
}

_PY_TYPES: dict[str, str] = {
    "string": "`str`",
    "bool": "`bool`",
    "double": "`float`",
    "date32[day]": "`datetime.date`",
    "binary": "`bytes`",
}


@dataclass(frozen=True, slots=True)
class ConverterSpecConfig:
    """Everything needed to render one converter's specification."""

    data_dir: Path
    output: Path
    suites: Sequence[str]
    error_manifest: Path
    error_base: type[ValueError]
    error_classes: Sequence[type[ValueError]]
    schema_lang: str
    data_lang: str
    generated_tables: dict[str, Callable[[], str]]
    target_label: str | None = None


def _rel(path: Path, start: Path) -> str:
    return Path(os.path.relpath(path, start)).as_posix()


def _code_block(path: Path, lang: str) -> str:
    return f"```{lang}\n{path.read_text(encoding='utf-8').rstrip()}\n```"


def _pa_ctor(dtype: pa.DataType) -> str:
    return _PA_CONSTRUCTORS.get(str(dtype), f"`{dtype}`")


def _py_type(dtype: pa.DataType) -> str:
    if pa.types.is_integer(dtype):
        return "`int`"
    return _PY_TYPES.get(str(dtype), f"`{dtype}`")


def render_xsd_scalar_table() -> str:
    """Render the built-in XSD scalar mapping table from ``SCALAR_ARROW_TYPES`` and ``BUILTIN_LIST_ITEM_TYPES``."""
    grouped: dict[str, list[str]] = {}
    dtypes: dict[str, pa.DataType] = {}
    for xsd_name, dtype in SCALAR_ARROW_TYPES.items():
        key = str(dtype)
        grouped.setdefault(key, []).append(xsd_name)
        dtypes[key] = dtype
    lines = [
        "| XSD built-in type(s) | Python type (coerced) | PyArrow `DataType` |",
        "| :--- | :--- | :--- |",
    ]
    for key, names in grouped.items():
        dtype = dtypes[key]
        lines.append(f"| {', '.join(f'`{n}`' for n in names)} | {_py_type(dtype)} | `{_pa_ctor(dtype)}` |")
    list_names = ", ".join(f"`{n}` (items `{item}`)" for n, item in BUILTIN_LIST_ITEM_TYPES.items())
    lines.append(f"| {list_names} | `list[str]` | `pa.list_(pa.string())` |")
    return "\n".join(lines)


JSONSCHEMA_SCALAR_PROBES: tuple[Mapping[str, Any], ...] = (
    {"type": "boolean"},
    {"type": "integer"},
    {"type": "number"},
    {"type": "string"},
    {"type": "string", "format": "date"},
    {"type": "string", "format": "date-time"},
    {"type": "string", "format": "time"},
    {"type": "string", "format": "duration"},
    {"type": "string", "format": "uuid"},
    {"type": "string", "format": "uri"},
    {"type": "string", "format": "email"},
    {"type": "string", "contentEncoding": "base64"},
    {"type": "string", "contentEncoding": "base16"},
    {"enum": [1, 2]},
    {"enum": [1, 2.5]},
    {"enum": [1, "two", True]},
    {"type": ["integer", "number"]},
    {"type": ["integer", "string"]},
    {"type": ["string", "null"]},
)


def render_jsonschema_scalar_table() -> str:
    """Render the JSON Schema scalar mapping table by resolving probe schemas through the converter."""
    lines = [
        '| JSON Schema | Python type (coerced) | PyArrow `DataType` | `metadata["jsonschema_type"]` |',
        "| :--- | :--- | :--- | :--- |",
    ]
    for probe in JSONSCHEMA_SCALAR_PROBES:
        dtype, label = resolve_scalar_type(probe)
        cell_label = label.replace("|", "\\|")
        lines.append(f"| `{json.dumps(probe)}` | {_py_type(dtype)} | `{_pa_ctor(dtype)}` | `{cell_label}` |")
    return "\n".join(lines)


def _render_rules(catalogue: RuleCatalogue, cases: Sequence[Case], config: ConverterSpecConfig) -> list[str]:
    covering: dict[str, list[Case]] = {rule_id: [] for rule_id in catalogue.rules}
    for case in cases:
        for rule_id in case.rules:
            covering.setdefault(rule_id, []).append(case)

    out = ["## 1. Conversion Rules", ""]
    for number, section in enumerate(catalogue.sections, start=1):
        out += [f"### 1.{number} {section.title}", ""]
        if section.intro:
            out += [section.intro, ""]
        for rule in section.rules:
            links = ", ".join(f"[{c.name}](#{c.anchor})" for c in covering[rule.id])
            statement = rule.statement.replace("\n", "\n  ")
            out += [
                f'<a id="{rule.id.lower()}"></a>',
                "",
                f"- **{rule.id}**: {statement}",
                "",
                f"  *Cases:* {links}",
                "",
            ]
        if section.generated:
            out += [config.generated_tables[section.generated](), ""]
    return out


def _render_case(case: Case, catalogue: RuleCatalogue, config: ConverterSpecConfig) -> list[str]:
    doc_dir = config.output.parent
    out = [f'<a id="{case.anchor}"></a>', "", f"### {case.title} (`{case.suite}/{case.name}`)", ""]
    rules = ", ".join(f"[{r}](#{r.lower()})" for r in case.rules if r in catalogue.rules)
    out += [f"**Rules:** {rules}", ""]
    if config.target_label and case.xml_tag:
        out += [f"**Target:** `{config.target_label}={case.xml_tag!r}`", ""]
    if case.notes:
        out += [case.notes, ""]

    embed = case.suite == "spec"
    if embed:
        out += [f"#### Schema (`{case.schema.name}`)", "", _code_block(case.schema, config.schema_lang), ""]
    else:
        out += [f"**Schema:** [`{_rel(case.schema, REPO_ROOT)}`]({_rel(case.schema, doc_dir)})", ""]

    seen_data: set[Path] = set()
    for run in case.runs:
        params = ", ".join(f"`{k}={v}`" for k, v in run.params.items())
        suffix = f" ({params})" if params else ""
        if config.target_label and run.xml_tag != case.xml_tag:
            suffix += f" (`{config.target_label}={run.xml_tag!r}`)"
        if embed:
            if run.data not in seen_data:
                out += [f"#### Input (`{run.data.name}`)", "", _code_block(run.data, config.data_lang), ""]
                seen_data.add(run.data)
            out += [f"#### Converted rows: run `{run.id}`{suffix}", "", _code_block(run.expected_json, "json"), ""]
        else:
            files = [run.data, run.expected_json, run.expected_table]
            listed = ", ".join(f"[`{f.name}`]({_rel(f, doc_dir)})" for f in files)
            out += [f"- Run `{run.id}`{suffix}: {listed}"]
    if not embed:
        out.append("")

    schema_link = f"[`{case.expected_schema.name}`]({_rel(case.expected_schema, doc_dir)})"
    if embed:
        out += [f"#### PyArrow schema ({schema_link})", "", _code_block(case.expected_schema, "python"), ""]
    else:
        out += [f"**Expected PyArrow schema:** {schema_link}", ""]
    out += ["---", ""]
    return out


def _render_errors(errors: Sequence[ErrorCase], config: ConverterSpecConfig) -> list[str]:
    doc_dir = config.output.parent
    by_code: dict[str, list[ErrorCase]] = {}
    for err in errors:
        by_code.setdefault(err.code, []).append(err)

    out = [
        "## 3. Errors (Unsupported / Invalid Input)",
        "",
        f"Schema generation raises a subclass of `{config.error_base.__name__}` (itself a `ValueError`).",
        "",
        "| Code | Exception | Condition | Reason | Fixtures |",
        "| :--- | :--- | :--- | :--- | :--- |",
    ]
    for cls in config.error_classes:
        code: str = getattr(cls, "code", "")
        title: str = getattr(cls, "title", "")
        reason = " ".join((cls.__doc__ or "").split())
        fixtures = "<br>".join(
            f"[`{e.file.name}`]({_rel(e.file, doc_dir)}): {e.description}" for e in by_code.get(code, [])
        )
        out.append(f"| **{code}** | `{cls.__name__}` | {title} | {reason} | {fixtures} |")
    out.append("")
    return out


def _render_index(cases: Sequence[Case], config: ConverterSpecConfig) -> list[str]:
    doc_dir = config.output.parent
    out = [
        "## 4. Fixture Index",
        "",
        "Every case directory contains a `case.yaml` manifest; the test suite and this document are both driven by it.",
        "",
    ]
    target = config.target_label
    out += (
        [f"| Case | `{target}` | Schema | Runs |", "| :--- | :--- | :--- | :--- |"]
        if target
        else ["| Case | Schema | Runs |", "| :--- | :--- | :--- |"]
    )
    for case in cases:
        manifest = case.directory / "case.yaml"
        runs = ", ".join(r.id for r in case.runs)
        target_cell = f" `{case.xml_tag}` |" if target else ""
        out.append(
            f"| [`{case.suite}/{case.name}`]({_rel(manifest, doc_dir)}) |{target_cell} "
            f"[`{case.schema.name}`]({_rel(case.schema, doc_dir)}) | {runs} |"
        )
    out.append("")
    return out


def render(config: ConverterSpecConfig) -> str:
    """Render the full specification document as Markdown."""
    catalogue = load_rules(config.data_dir / "rules.yaml")
    cases = [case for suite in config.suites for case in load_cases(config.data_dir / suite)]
    errors = load_error_cases(config.error_manifest)

    sources = ", ".join(
        f"`{_rel(p, REPO_ROOT)}`"
        for p in (config.data_dir / "rules.yaml", *(config.data_dir / s for s in config.suites))
    )
    out = [GENERATED_BANNER.format(sources=sources), "", f"# {catalogue.title}", "", catalogue.intro, ""]
    contents = (
        "**Contents:** [1. Conversion Rules](#1-conversion-rules) · [2. Cases](#2-cases) · "
        "[3. Errors](#3-errors-unsupported--invalid-input) · [4. Fixture Index](#4-fixture-index)"
    )
    out += [contents, ""]
    out += _render_rules(catalogue, cases, config)
    out += ["## 2. Cases", ""]
    for case in cases:
        out += _render_case(case, catalogue, config)
    out += _render_errors(errors, config)
    out += _render_index(cases, config)
    text = "\n".join(out)
    while "\n\n\n" in text:
        text = text.replace("\n\n\n", "\n\n")
    return text.rstrip() + "\n"


XSD_TO_PYARROW = ConverterSpecConfig(
    data_dir=REPO_ROOT / "tests" / "data" / "xsd",
    output=REPO_ROOT / "docs" / "xsd_to_pyarrow_specification.md",
    suites=("spec", "examples"),
    error_manifest=REPO_ROOT / "tests" / "data" / "xsd" / "spec" / "errors" / "manifest.yaml",
    error_base=XsdToPyArrowError,
    error_classes=XSD_TO_PYARROW_ERRORS,
    schema_lang="xml",
    data_lang="xml",
    generated_tables={"scalar_table": render_xsd_scalar_table},
    target_label="xml_tag",
)

JSONSCHEMA_TO_PYARROW = ConverterSpecConfig(
    data_dir=REPO_ROOT / "tests" / "data" / "jsonschema",
    output=REPO_ROOT / "docs" / "jsonschema_to_pyarrow_specification.md",
    suites=("spec", "examples"),
    error_manifest=REPO_ROOT / "tests" / "data" / "jsonschema" / "spec" / "errors" / "manifest.yaml",
    error_base=JSONSchemaToPyArrowError,
    error_classes=JSONSCHEMA_TO_PYARROW_ERRORS,
    schema_lang="json",
    data_lang="json",
    generated_tables={"scalar_table": render_jsonschema_scalar_table},
)

CONFIGS: tuple[ConverterSpecConfig, ...] = (XSD_TO_PYARROW, JSONSCHEMA_TO_PYARROW)


def main(argv: Sequence[str] | None = None) -> int:
    """Write or check every configured specification document."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if a committed document is out of date")
    args = parser.parse_args(argv)

    stale: list[Path] = []
    for config in CONFIGS:
        text = render(config)
        current = config.output.read_text(encoding="utf-8") if config.output.exists() else None
        if current == text:
            continue
        if args.check:
            stale.append(config.output)
        else:
            config.output.write_text(text, encoding="utf-8")
    for path in stale:
        sys.stderr.write(f"stale: {_rel(path, REPO_ROOT)}\n")
    return 1 if stale else 0


if __name__ == "__main__":
    sys.exit(main())
