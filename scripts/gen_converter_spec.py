"""Generate converter and pipeline specification documents from rule catalogues, case manifests, and code.

Usage::

    uv run python -m scripts.gen_converter_spec            # write every docs/*_specification.md in CONFIGS
    uv run python -m scripts.gen_converter_spec --check    # exit 1 if a committed document is stale
"""

import argparse
import gzip
import inspect
import itertools
import json
import sys
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import IntEnum, StrEnum
from ipaddress import IPv4Address
from pathlib import Path
from typing import Annotated, Any, Final, Literal
from uuid import UUID

import pyarrow as pa
import yaml
from frozendict import frozendict
from jsonschema.validators import validator_for
from pydantic import Base64Bytes, Field, NaiveDatetime, PositiveFloat, PositiveInt

from cdm_data_loaders.coercion.pyarrow import pyarrow_to_python_type_label
from cdm_data_loaders.converters.emitters.pyarrow_codegen import render_scalar_type_expr
from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import (
    JSONSCHEMA_TO_PYARROW_ERRORS,
    JSONSchemaToPyArrowError,
    convert_xsv_schema_to_pyarrow,
    resolve_scalar_type,
)
from cdm_data_loaders.converters.facades.pydantic_to_pyarrow import (
    PYDANTIC_TO_PYARROW_ERRORS,
    PydanticToPyArrowError,
)
from cdm_data_loaders.converters.facades.pydantic_to_pyarrow import (
    resolve_scalar_type as resolve_pydantic_scalar_type,
)
from cdm_data_loaders.converters.facades.xsd_to_pyarrow import (
    SCALAR_ARROW_TYPES,
    XSD_TO_PYARROW_ERRORS,
    XsdToPyArrowError,
)
from cdm_data_loaders.converters.readers.json_schema_strict import (
    NON_EXTENDABLE_SCALAR_HINTS,
    STRING_CONTENT_ENCODING_SPECS,
    STRING_FORMAT_SPECS,
)
from cdm_data_loaders.converters.readers.xsd import (
    BUILTIN_LIST_ITEM_TYPES,
)
from cdm_data_loaders.pipelines.jsonlines.settings import (
    JSONLINES_JSONSCHEMA_ERRORS,
    JsonlJsonschemaError,
)
from cdm_data_loaders.pipelines.xmltopyarrow.settings import (
    XMLTOPYARROW_CONFIG_ERRORS,
    PubMedXmlToPyArrowSettings,
    UniProtXmlToPyArrowSettings,
    UniRef50XmlToPyArrowSettings,
    UniRef90XmlToPyArrowSettings,
    UniRef100XmlToPyArrowSettings,
    XmlToPyArrowConfigError,
    XmlToPyArrowSettings,
)
from cdm_data_loaders.pipelines.xsv.settings import (
    XSV_SCHEMA_ERRORS,
    XsvSchemaError,
)
from cdm_data_loaders.readers.jsonschema_xsv.xsv_reader import (
    PARSING_CONFIG_KEYS,
    resolve_duckdb_column_config,
)
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.helpers import CleanerValidatorArgs
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.schema_utils import ValidatedXsvSchema
from cdm_data_loaders.schemas.jsonschema import (
    CUSTOM_META_SCHEMA_URI,
    X_XSV_CONFIG_SCHEMA,
    dereference_schema,
    register_xsv_validator,
)
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from scripts.gen_docs import (
    DOCS_DIR,
    REPO_ROOT,
    check_or_write_files,
    escape_table_cell,
    format_value_code,
    mro_class_names,
    rel_posix,
    repo_blob_url,
)
from tests.cdm_data_loaders.pipelines.framework import resolve_pipeline
from tests.converter_spec import (
    Case,
    ErrorCase,
    RuleCatalogue,
    Run,
    load_cases,
    load_error_cases,
    load_rules,
)
from tests.pipeline_spec import PipelineCase, PipelineRun, load_pipeline_cases

GENERATED_BANNER: Final[str] = (
    "<!-- GENERATED FILE - DO NOT EDIT. Source: {sources}. "
    "Regenerate with `uv run python -m scripts.gen_converter_spec`. -->"
)
PIPELINE_SPECS_ROOT: Final[Path] = REPO_ROOT / "specs" / "pipelines"

CODE_LANG_BY_SUFFIX: Final[Mapping[str, str]] = {
    ".json": "json",
    ".jsonl": "json",
    ".md5": "text",
    ".py": "python",
    ".toml": "toml",
    ".txt": "text",
    ".xml": "xml",
    ".xsd": "xml",
    ".yaml": "yaml",
    ".yml": "yaml",
}


@dataclass(frozen=True, slots=True)
class ConverterSpecConfig:
    """Everything needed to render one converter's specification."""

    data_dir: Path
    output: Path
    suites: Sequence[str]
    error_manifest: Path
    error_base: type[Exception]
    error_classes: Sequence[type[Exception]]
    schema_lang: str
    data_lang: str
    generated_tables: dict[str, Callable[[], str]]
    target_label: str | None = None


@dataclass(frozen=True, slots=True)
class PipelineSpecConfig:
    """Configuration for generating a pipeline family's Markdown specification document."""

    name: str
    data_dir: Path
    output_path: Path
    presets: tuple[str, ...] = ()
    shared_files: tuple[Path, ...] = ()
    converter_config: ConverterSpecConfig | None = field(default=None)


def _infer_code_lang(path: Path, default_lang: str = "text") -> str:
    """Resolve the fenced-code-block language identifier for ``path``."""
    if path.suffix in {".md5", ".txt"}:
        return "text"
    if default_lang != "text":
        return default_lang
    inner_name = path.name.removesuffix(".gz")
    inner_suffix = Path(inner_name).suffix
    return CODE_LANG_BY_SUFFIX.get(inner_suffix, default_lang)


def _code_block(path: Path, lang: str | None = None) -> str:
    """Read ``path`` (decompressing ``.gz`` if needed) and wrap it in a Markdown fenced code block."""
    effective_lang = _infer_code_lang(path, lang or "text")
    raw = path.read_bytes()
    text = (gzip.decompress(raw) if path.name.endswith(".gz") else raw).decode("utf-8").rstrip()
    return f"```{effective_lang}\n{text}\n```"


def _pa_ctor(dtype: pa.DataType) -> str:
    """Format ``dtype`` as its ``pa.*`` constructor expression."""
    try:
        return render_scalar_type_expr(dtype)
    except KeyError:
        return f"`{dtype}`"


def render_xsd_scalar_table() -> str:
    """Render the built-in XSD scalar mapping table from ``SCALAR_ARROW_TYPES`` and ``BUILTIN_LIST_ITEM_TYPES``.

    :return: Markdown table of XSD built-in scalar and list type mappings.
    :rtype: str
    """
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
        lines.append(
            f"| {', '.join(f'`{n}`' for n in names)} | {pyarrow_to_python_type_label(dtype)} | `{_pa_ctor(dtype)}` |"
        )
    list_grouped: dict[str, list[str]] = {}
    list_item_dtypes: dict[str, pa.DataType] = {}
    for xsd_list_name, item_xsd_name in BUILTIN_LIST_ITEM_TYPES.items():
        item_dtype = SCALAR_ARROW_TYPES[item_xsd_name]
        key = str(item_dtype)
        list_grouped.setdefault(key, []).append(f"`{xsd_list_name}` (items `{item_xsd_name}`)")
        list_item_dtypes[key] = item_dtype
    for key, labels in list_grouped.items():
        item_dtype = list_item_dtypes[key]
        list_dtype = pa.list_(item_dtype)
        lines.append(
            f"| {', '.join(labels)} | {pyarrow_to_python_type_label(list_dtype)} | `pa.list_({_pa_ctor(item_dtype)})` |"
        )
    return "\n".join(lines)


def render_jsonschema_scalar_table() -> str:
    """Render the JSON Schema scalar mapping table by resolving probe schemas through the converter.

    :return: Markdown table of JSON Schema scalar probe mappings.
    :rtype: str
    """
    lines = [
        '| JSON Schema | Python type (coerced) | PyArrow `DataType` | `metadata["jsonschema_type"]` |',
        "| :--- | :--- | :--- | :--- |",
    ]
    for probe in JSONSCHEMA_SCALAR_PROBES:
        dtype, label = resolve_scalar_type(probe)
        cell_label = escape_table_cell(label)
        lines.append(
            f"| `{json.dumps(dict(probe))}` | {pyarrow_to_python_type_label(dtype)} | `{_pa_ctor(dtype)}` | `{cell_label}` |"
        )
    return "\n".join(lines)


class _SampleStrEnum(StrEnum):
    ALPHA = "alpha"
    BETA = "beta"


class _SampleIntEnum(IntEnum):
    ONE = 1
    TWO = 2


JSONSCHEMA_SCALAR_PROBES: Final[tuple[Mapping[str, Any], ...]] = (
    *(frozendict({"type": type_name}) for type_name in (*NON_EXTENDABLE_SCALAR_HINTS, "string")),
    *(frozendict({"type": "string", "format": fmt}) for fmt in STRING_FORMAT_SPECS),
    *(frozendict({"type": "string", "contentEncoding": enc}) for enc in STRING_CONTENT_ENCODING_SPECS),
    frozendict({"enum": [1, 2]}),
    frozendict({"enum": [1, 2.5]}),
    frozendict({"enum": [1, "two", True]}),
    frozendict({"type": ["integer", "number"]}),
    frozendict({"type": ["integer", "string"]}),
    frozendict({"type": ["string", "null"]}),
)

PYDANTIC_SCALAR_PROBES: Final[tuple[tuple[str, Any], ...]] = (
    ("`bool`, `StrictBool`", bool),
    (
        "`int`, `StrictInt`, `PositiveInt`, `NegativeInt`, `NonNegativeInt`, `NonPositiveInt`",
        PositiveInt,
    ),
    (
        "`float`, `StrictFloat`, `PositiveFloat`, `NegativeFloat`, `NonNegativeFloat`, `NonPositiveFloat`",
        PositiveFloat,
    ),
    (
        "`str`, `StrictStr`, `SecretStr`, `EmailStr`, `NameEmail`, `AnyUrl`, `HttpUrl`, `Base64Str`",
        str,
    ),
    ("`Decimal` (unconstrained)", Decimal),
    (
        "`Annotated[Decimal, Field(max_digits=10, decimal_places=2)]`",
        Annotated[Decimal, Field(max_digits=10, decimal_places=2)],
    ),
    ("`datetime.date`, `PastDate`, `FutureDate`", date),
    ("`datetime.datetime`, `AwareDatetime`, `PastDatetime`, `FutureDatetime`", datetime),
    ("`NaiveDatetime`", NaiveDatetime),
    ("`datetime.time`", time),
    ("`datetime.timedelta`", timedelta),
    ("`UUID`, `UUID1`..`UUID5`", UUID),
    ("`Path`, `PurePath`, `FilePath`, `DirectoryPath`, `NewPath`", Path),
    (
        "`IPv4Address`, `IPv6Address`, `IPv4Network`, `IPv6Network`, `IPvAnyAddress`, `IPvAnyInterface`, `IPvAnyNetwork`",
        IPv4Address,
    ),
    ("`bytes`, `bytearray`, `SecretBytes`", bytes),
    ("`Base64Bytes`", Base64Bytes),
    ("`StrEnum`, `Enum` (str values)", _SampleStrEnum),
    ("`IntEnum`, `Enum` (int values)", _SampleIntEnum),
    ("`Literal[1, 2]`", Literal[1, 2]),
    ("`Literal[1, 2.5]`", Literal[1, 2.5]),
    ('`Literal[1, "two", True]`', Literal[1, "two", True]),
    ("`int | float`", int | float),
    ("`int | str`", int | str),
    ("`str | None`", str | None),
)


def render_pydantic_scalar_table() -> str:
    """Render the Pydantic scalar mapping table by resolving probe annotations through the converter.

    :return: Markdown table of Pydantic scalar probe mappings.
    :rtype: str
    """
    lines = [
        '| Pydantic type / annotation | Python type (coerced) | PyArrow `DataType` | `metadata["pydantic_type"]` |',
        "| :--- | :--- | :--- | :--- |",
    ]
    for display_label, annotation in PYDANTIC_SCALAR_PROBES:
        dtype, label = resolve_pydantic_scalar_type(annotation)
        cell_display = escape_table_cell(display_label)
        cell_label = escape_table_cell(label)
        lines.append(
            f"| {cell_display} | {pyarrow_to_python_type_label(dtype)} | `{_pa_ctor(dtype)}` | `{cell_label}` |"
        )
    return "\n".join(lines)


def render_xsv_config_table() -> str:
    """Render the ``x-xsv-config`` parser configuration mapping table from live schema and model definitions.

    :return: Markdown table of ``x-xsv-config`` properties and ``CleanerValidatorArgs`` fields.
    :rtype: str
    """
    props: Mapping[str, Any] = X_XSV_CONFIG_SCHEMA["properties"]
    arg_fields = CleanerValidatorArgs.model_fields
    lines = [
        "| `x-xsv-config` key | JSON Schema constraint | `CleanerValidatorArgs` field | Default |",
        "| :--- | :--- | :--- | :--- |",
    ]
    for raw_key, prop_schema in props.items():
        stripped = raw_key.removeprefix("x-").replace("-", "_")
        if stripped == "has_header":
            arg_name = "`missing_header` (inverted: `not x-has-header`)"
            default_repr = "`False` (`x-has-header=True`)"
        elif stripped in PARSING_CONFIG_KEYS:
            target_field = PARSING_CONFIG_KEYS[stripped]
            arg_name = f"`{target_field}`"
            default_val = arg_fields[target_field].default
            default_repr = f"`{default_val!r}`"
        else:
            arg_name = "*(output metadata only; not passed to `CleanerValidatorArgs`)*"
            default_repr = "—"
        constraint_dict = {k: v for k, v in prop_schema.items() if k != "description"}
        constraint = escape_table_cell(json.dumps(constraint_dict))
        lines.append(f"| `{raw_key}` | `{constraint}` | {arg_name} | {default_repr} |")
    return "\n".join(lines)


def _build_xsv_column_probes() -> list[dict[str, Any] | None]:
    """Derive XSV column probes from scalar type hints and the XSV specification case schemas.

    :return: Ordered list of distinct column JSON Schema dicts (plus ``None`` for unlisted ``required`` columns).
    :rtype: list[dict[str, Any] | None]
    """
    seen: set[str] = set()
    probes: list[dict[str, Any] | None] = []

    def _add(probe: dict[str, Any]) -> None:
        key = json.dumps(probe, sort_keys=True)
        if key not in seen:
            seen.add(key)
            probes.append(probe)

    for scalar_type in (*NON_EXTENDABLE_SCALAR_HINTS, "string"):
        _add({"type": scalar_type})
        _add({"type": [scalar_type, "null"]})

    has_unlisted_required = False
    for case in load_cases(PIPELINE_SPECS_ROOT / "xsv" / "spec"):
        raw_schema = json.loads(case.schema.read_text(encoding="utf-8"))
        deref = dereference_schema(raw_schema)
        props = deref.get("properties", {})
        if any(col not in props for col in deref.get("required", ())):
            has_unlisted_required = True
        for prop_schema in props.values():
            cleaned = {k: v for k, v in prop_schema.items() if k not in {"title", "description"}}
            _add(cleaned)

    if has_unlisted_required:
        probes.append(None)
    return probes


def render_xsv_column_type_table() -> str:
    """Render the XSV column type & nullability mapping table from ``convert_xsv_schema_to_pyarrow`` and ``resolve_duckdb_column_config``.

    :return: Markdown table of XSV column type and nullability mappings.
    :rtype: str
    """
    lines = [
        "| Column JSON Schema | DuckDB CSV `dtype` | `force_not_null` | PyArrow `DataType` | `Field.nullable` |",
        "| :--- | :--- | :--- | :--- | :--- |",
    ]
    for probe in _build_xsv_column_probes():
        display_label = (
            f"`{json.dumps(dict(probe))}`"
            if probe is not None
            else "*(column in `required`, omitted from `properties`)*"
        )
        properties: dict[str, Any] = {"col": dict(probe)} if probe is not None else {}
        raw_schema: dict[str, Any] = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["col"],
            "properties": properties,
        }
        pa_schema = convert_xsv_schema_to_pyarrow(
            ValidatedXsvSchema(
                jsonschema=raw_schema,
                has_xsv_metaschema=False,
                has_xsv_parser_config=False,
                required_cols=["col"],
            )
        )
        pa_field = pa_schema.field("col")
        duckdb_cfg = resolve_duckdb_column_config(pa_schema if probe is not None else None)
        duckdb_dtype = str(duckdb_cfg.get("dtype", {}).get("col", "VARCHAR (auto)"))
        force_not_null = "col" in duckdb_cfg.get("force_not_null", [])
        cell_display = escape_table_cell(display_label)
        lines.append(
            f"| {cell_display} | `{duckdb_dtype}` | `{force_not_null}` | `{_pa_ctor(pa_field.type)}` | `{pa_field.nullable}` |"
        )
    return "\n".join(lines)


XMLTOPYARROW_SETTINGS_CLASSES: Final[tuple[type[XmlToPyArrowSettings], ...]] = (
    XmlToPyArrowSettings,
    UniProtXmlToPyArrowSettings,
    UniRef50XmlToPyArrowSettings,
    UniRef90XmlToPyArrowSettings,
    UniRef100XmlToPyArrowSettings,
    PubMedXmlToPyArrowSettings,
)


def render_xmltopyarrow_presets_table() -> str:
    """Render the XML-to-PyArrow settings & preset pipeline configuration table.

    :return: Markdown table of XML-to-PyArrow preset CLI and settings configurations.
    :rtype: str
    """
    lines = [
        "| CLI Command | Settings Class | `dataset_name` | Target Selection | `file_glob` | Default Pre-processors |",
        "| :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for cls in XMLTOPYARROW_SETTINGS_CLASSES:
        cli_name = str(cls.model_config.get("cli_prog_name", ""))
        dataset_default = getattr(cls, "dataset_name", None)
        dataset_cell = f"`{dataset_default!r}` (locked)" if isinstance(dataset_default, str) else "*(required)*"
        xml_level = getattr(cls, "xml_level", None)
        xml_tag = getattr(cls, "xml_tag", None)
        root_tag = getattr(cls, "root_tag", None)
        if isinstance(xml_level, int):
            target_cell = f"`xml_level={xml_level}`, `root_tag={root_tag!r}` (locked)"
        elif isinstance(xml_tag, str):
            table_name = getattr(cls, "table_name", None)
            target_cell = f"`xml_tag={xml_tag!r}`, `table_name={table_name!r}` (locked)"
        else:
            target_cell = "`xml_tag` + `table_name` **or** `xml_level` (+ optional `root_tag`)"
        glob_default = cls.model_fields["file_glob"].default
        preprocessors = getattr(cls, "_DEFAULT_FILE_PREPROCESSORS", ())
        prep_cell = ", ".join(f"`{fn.__name__}`" for fn in preprocessors) if preprocessors else "—"
        lines.append(
            f"| `{cli_name}` | `{cls.__name__}` | {dataset_cell} | {target_cell} | `{glob_default!r}` | {prep_cell} |"
        )
    return "\n".join(lines)


JSONL_JSONSCHEMA_DRAFT_URIS: Final[tuple[str, ...]] = (
    "https://json-schema.org/draft/2020-12/schema",
    "https://json-schema.org/draft/2019-09/schema",
    "http://json-schema.org/draft-07/schema#",
    "http://json-schema.org/draft-06/schema#",
    "http://json-schema.org/draft-04/schema#",
    CUSTOM_META_SCHEMA_URI,
)


def render_jsonlines_jsonschema_drafts_table() -> str:
    """Render the supported JSON Schema drafts table resolved via ``jsonschema.validators.validator_for``.

    :return: Markdown table of supported JSON Schema draft URIs and validator classes.
    :rtype: str
    """
    register_xsv_validator()
    lines = [
        "| Declared `$schema` URI | Validator Class | Format Checking |",
        "| :--- | :--- | :--- |",
    ]
    for uri in JSONL_JSONSCHEMA_DRAFT_URIS:
        validator_cls = validator_for({"$schema": uri}, default=None)  # type: ignore[arg-type]
        cls_name = validator_cls.__name__ if validator_cls else "None"
        lines.append(f"| `{uri}` | `{cls_name}` | `FormatChecker()` enabled |")
    return "\n".join(lines)


def render_rejected_schema_table() -> str:
    """Render the ``_rejected`` table PyArrow schema from ``REJECTED_PYARROW_SCHEMA``.

    :return: Markdown table describing ``REJECTED_PYARROW_SCHEMA`` columns.
    :rtype: str
    """
    lines = [
        "| Column | PyArrow `DataType` | `Field.nullable` | Description |",
        "| :--- | :--- | :--- | :--- |",
    ]
    descriptions = {
        "table_name": "Target entity table name (`None` for level-mode file pre-processor errors)",
        "source_file": "Input file path relative to `input_dir`",
        "line_no": "1-based physical line number in `source_file` (`None` for file-level errors)",
        "raw_record": "Stripped raw input line (`None` for file-level errors)",
        "error_detail": "Parser error message, JSON-encoded list of schema errors, or pre-processor error",
    }
    lines.extend(
        f"| `{field.name}` | `{_pa_ctor(field.type)}` | `{field.nullable}` | {descriptions.get(field.name, '—')} |"
        for field in REJECTED_PYARROW_SCHEMA
    )
    return "\n".join(lines)


def _format_rule_links(
    rule_ids: Sequence[str],
    catalogue: RuleCatalogue,
    *,
    code_wrap: bool = False,
) -> str:
    """Format ``rule_ids`` as comma-separated Markdown anchor links."""
    return ", ".join(
        f"[`{rid}`](#{rid.lower()})" if code_wrap else f"[{rid}](#{rid.lower()})"
        for rid in rule_ids
        if rid in catalogue.rules
    )


def _render_rule_sections(
    catalogue: RuleCatalogue,
    covering_links: Mapping[str, str],
    generated_tables: Mapping[str, Callable[[], str]],
) -> list[str]:
    """Render numbered ``### 1.N`` rule sections and any attached generated tables."""
    out: list[str] = []
    for number, section in enumerate(catalogue.sections, start=1):
        out += [f"### 1.{number} {section.title}", ""]
        if section.intro:
            out += [section.intro, ""]
        for rule in section.rules:
            links = covering_links.get(rule.id, "—")
            statement = rule.statement.replace("\n", "\n  ")
            out += [
                f'<a id="{rule.id.lower()}"></a>',
                "",
                f"- **{rule.id}**: {statement}",
                "",
                f"  *Cases:* {links}",
                "",
            ]
        if section.generated and section.generated in generated_tables:
            out += [generated_tables[section.generated](), ""]
    return out


def _render_rules(catalogue: RuleCatalogue, cases: Sequence[Case], config: ConverterSpecConfig) -> list[str]:
    """Render ``## 1. Conversion Rules`` for a converter specification."""
    covering: dict[str, list[Case]] = {rule_id: [] for rule_id in catalogue.rules}
    for case in cases:
        for rule_id in case.rules:
            covering.setdefault(rule_id, []).append(case)
    covering_links = {
        rule_id: ", ".join(f"[{c.name}](#{c.anchor})" for c in rule_cases) for rule_id, rule_cases in covering.items()
    }
    return [
        "## 1. Conversion Rules",
        "",
        *_render_rule_sections(catalogue, covering_links, config.generated_tables),
    ]


def _render_embedded_run(
    run: Run,
    case: Case,
    config: ConverterSpecConfig,
    suffix: str,
    seen_data: set[Path],
) -> list[str]:
    """Render embedded input and expected output code blocks for a single ``spec`` suite run."""
    out: list[str] = []
    if run.data not in seen_data:
        if run.data.is_dir():
            for data_file in sorted(p for p in run.data.rglob("*") if p.is_file()):
                rel_data = rel_posix(data_file, case.directory)
                out += [f"#### Input (`{rel_data}`)", "", _code_block(data_file, config.data_lang), ""]
        else:
            out += [f"#### Input (`{run.data.name}`)", "", _code_block(run.data, config.data_lang), ""]
        seen_data.add(run.data)
    out += [f"#### Converted rows: run `{run.id}`{suffix}", "", _code_block(run.expected_json, "json"), ""]
    for extra_table, extra_path in run.extra_expected_json.items():
        out += [
            f"#### Converted rows (`{extra_table}`): run `{run.id}`{suffix}",
            "",
            _code_block(extra_path, "json"),
            "",
        ]
    if run.expected_rejected is not None:
        out += [
            f"#### Rejected rows (`_rejected`): run `{run.id}`{suffix}",
            "",
            _code_block(run.expected_rejected, "json"),
            "",
        ]
    return out


def _render_case(case: Case, catalogue: RuleCatalogue, config: ConverterSpecConfig) -> list[str]:
    """Render a single converter or base pipeline specification case section."""
    out = [f'<a id="{case.anchor}"></a>', "", f"### {case.title} (`{case.suite}/{case.name}`)", ""]
    out += [f"**Rules:** {_format_rule_links(case.rules, catalogue)}", ""]
    if config.target_label and case.xml_tag:
        out += [f"**Target:** `{config.target_label}={case.xml_tag!r}`", ""]
    if case.notes:
        out += [case.notes, ""]

    embed = case.suite == "spec"
    if embed:
        out += [f"#### Schema (`{case.schema.name}`)", "", _code_block(case.schema, config.schema_lang), ""]
    else:
        out += [f"**Schema:** [`{rel_posix(case.schema, REPO_ROOT)}`]({repo_blob_url(case.schema)})", ""]

    seen_data: set[Path] = set()
    for run in case.runs:
        params = ", ".join(f"`{k}={v}`" for k, v in run.params.items())
        suffix = f" ({params})" if params else ""
        if config.target_label and run.xml_tag != case.xml_tag:
            suffix += f" (`{config.target_label}={run.xml_tag!r}`)"
        if embed:
            out += _render_embedded_run(run, case, config, suffix, seen_data)
        else:
            files = [run.data, run.expected_json, run.expected_table]
            listed = ", ".join(f"[`{f.name}`]({repo_blob_url(f)})" for f in files)
            out += [f"- Run `{run.id}`{suffix}: {listed}"]
    if not embed:
        out.append("")

    schema_link = f"[`{case.expected_schema.name}`]({repo_blob_url(case.expected_schema)})"
    if embed:
        out += [f"#### PyArrow schema ({schema_link})", "", _code_block(case.expected_schema, "python"), ""]
    else:
        out += [f"**Expected PyArrow schema:** {schema_link}", ""]
    out += ["---", ""]
    return out


def _render_errors(errors: Sequence[ErrorCase], config: ConverterSpecConfig, *, heading_num: int = 3) -> list[str]:
    """Render the numbered error catalogue table for ``config``."""
    by_code: dict[str, list[ErrorCase]] = {}
    for err in errors:
        by_code.setdefault(err.code, []).append(err)

    parent_type = "RuntimeError" if issubclass(config.error_base, RuntimeError) else "ValueError"
    out = [
        f"## {heading_num}. Errors (Unsupported / Invalid Input)",
        "",
        f"Schema generation raises a subclass of `{config.error_base.__name__}` (itself a `{parent_type}`).",
        "",
        "| Code | Exception | Condition | Reason | Fixtures |",
        "| :--- | :--- | :--- | :--- | :--- |",
    ]
    for cls in config.error_classes:
        code: str = getattr(cls, "code", "")
        title: str = getattr(cls, "title", "")
        reason = " ".join((cls.__doc__ or "").split())
        fixtures = "<br>".join(
            f"[`{e.file.name}`]({repo_blob_url(e.file)}): {e.description}" for e in by_code.get(code, [])
        )
        out.append(f"| **{code}** | `{cls.__name__}` | {title} | {reason} | {fixtures} |")
    out.append("")
    return out


def _render_index(cases: Sequence[Case], config: ConverterSpecConfig, *, heading_num: int = 4) -> list[str]:
    """Render the fixture index table for a converter specification."""
    out = [
        f"## {heading_num}. Fixture Index",
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
            f"| [`{case.suite}/{case.name}`]({repo_blob_url(manifest)}) |{target_cell} "
            f"[`{case.schema.name}`]({repo_blob_url(case.schema)}) | {runs} |"
        )
    out.append("")
    return out


def _introspect_preset_overrides(base_cls: type[Any], preset_cls: type[Any]) -> list[tuple[str, str, str, str]]:
    """Return ``(attribute, kind, base_val, preset_val)`` rows comparing ``preset_cls`` to ``base_cls``."""
    rows: list[tuple[str, str, str, str]] = []
    base_fields = base_cls.model_fields
    preset_fields = preset_cls.model_fields

    for field_name, base_field in base_fields.items():
        if field_name not in preset_fields and hasattr(preset_cls, field_name):
            preset_val = getattr(preset_cls, field_name)
            rows.append(
                (
                    f"`{field_name}`",
                    "Locked `ClassVar` (CLI flag removed)",
                    format_value_code(base_field.default),
                    format_value_code(preset_val),
                )
            )
        elif field_name in preset_fields:
            preset_field = preset_fields[field_name]
            if preset_field.default != base_field.default:
                rows.append(
                    (
                        f"`{field_name}`",
                        "Overridden field default",
                        format_value_code(base_field.default),
                        format_value_code(preset_field.default),
                    )
                )

    for extra_attr in ("_PRECOMPUTED_SCHEMAS", "_PRECOMPUTED_SCHEMA", "_DEFAULT_FILE_PREPROCESSORS"):
        val = getattr(preset_cls, extra_attr, None)
        if val:
            base_val = getattr(base_cls, extra_attr, None)
            if val != base_val:
                rows.append(
                    (
                        f"`{extra_attr}`",
                        "Preset class binding",
                        format_value_code(base_val) if base_val else "`None`",
                        format_value_code(val),
                    )
                )
    return rows


def _render_hierarchy_section(spec: PipelineSpecConfig) -> list[str]:
    """Render the Settings class hierarchy diagram and auto-introspected preset overrides tables."""
    if not spec.presets:
        return []

    base_resolved = resolve_pipeline(spec.name)
    base_cls = base_resolved.settings_cls
    if base_cls is None:
        msg = f"Pipeline {spec.name!r} has no settings_cls"
        raise ValueError(msg)

    lines = [
        "## Settings Class Hierarchy & Preset Subclasses",
        "",
        (
            f"The generic `{base_resolved.cli_command}` pipeline is configured by `{base_cls.__name__}` and executed by "
            f"`{base_resolved.pipeline_fn.__name__ if base_resolved.pipeline_fn else ''}`. "
            "Domain preset pipelines subclass the base settings class to lock schema, table, and transform parameters as "
            "`ClassVar` constants or narrowed defaults while delegating execution to the same pipeline runner."
        ),
        "",
        "```mermaid",
        "classDiagram",
    ]

    edges_seen: set[tuple[str, str]] = set()
    for preset_name in spec.presets:
        preset_resolved = resolve_pipeline(preset_name)
        preset_cls = preset_resolved.settings_cls
        if preset_cls is None:
            msg = f"Preset pipeline {preset_name!r} has no settings_cls"
            raise ValueError(msg)
        mro_chain = [cls for cls in inspect.getmro(preset_cls) if issubclass(cls, base_cls)]
        for child_cls, parent_cls in itertools.pairwise(mro_chain):
            edge = (parent_cls.__name__, child_cls.__name__)
            if edge not in edges_seen:
                edges_seen.add(edge)
                lines.append(f"    {parent_cls.__name__} <|-- {child_cls.__name__}")
    lines.extend(["```", ""])

    for preset_name in spec.presets:
        preset_resolved = resolve_pipeline(preset_name)
        preset_cls = preset_resolved.settings_cls
        if preset_cls is None or preset_resolved.pipeline_fn is None:
            msg = f"Preset pipeline {preset_name!r} has no settings_cls or pipeline_fn"
            raise ValueError(msg)
        mro_names = " -> ".join(f"`{cls_name}`" for cls_name in mro_class_names(preset_cls, base_cls))
        lines.extend(
            [
                f"### Preset Subclass: `{preset_resolved.cli_command}` (`{preset_cls.__name__}`)",
                "",
                f"- **CLI Command**: `{preset_resolved.cli_command}`",
                f"- **MRO Chain**: {mro_names}",
                f"- **Shared Runner**: `{preset_resolved.pipeline_fn.__module__}.{preset_resolved.pipeline_fn.__name__}`",
                "",
                "| Setting / Class Attribute | Override Kind | Base (`"
                + base_cls.__name__
                + "`) | Preset (`"
                + preset_cls.__name__
                + "`) |",
                "| --- | --- | --- | --- |",
            ]
        )
        for attr_name, kind, base_val, preset_val in _introspect_preset_overrides(base_cls, preset_cls):
            lines.append(f"| {attr_name} | {kind} | {base_val} | {preset_val} |")
        lines.append("")

    return lines


def _render_preset_run(case: PipelineCase, run: PipelineRun) -> list[str]:
    """Render settings, input previews, expected report summary, and expected tables for ``run``."""
    lines: list[str] = []
    if len(case.runs) > 1:
        lines.extend([f"#### Run: `{run.id}`", ""])

    if run.settings_file is not None and run.settings_file.is_file():
        lines.extend(
            [
                f"**Settings (`{rel_posix(run.settings_file)}`)**",
                "",
                _code_block(run.settings_file),
                "",
            ]
        )

    input_files = (
        [run.input_dir] if run.input_dir.is_file() else sorted(p for p in run.input_dir.rglob("*") if p.is_file())
    )
    for input_file in input_files:
        rel_in = input_file.name if run.input_dir.is_file() else input_file.relative_to(run.input_dir).as_posix()
        label_suffix = " (decompressed preview)" if input_file.name.endswith(".gz") else ""
        lines.extend(
            [
                f"**Input `{rel_in}` (`{rel_posix(input_file)}`){label_suffix}**",
                "",
                _code_block(input_file),
                "",
            ]
        )

    report_preview = {
        "status": run.expected_report.status,
        "ingested_files": list(run.expected_report.ingested_files),
        "load_errors": list(run.expected_report.load_errors),
        "pipeline_load_info_errs": run.expected_report.pipeline_load_info_errs,
    }
    lines.extend(
        [
            f"**Expected `report.txt` Summary & Row Counts (`{run.expected_counts}`)**",
            "",
            "```yaml",
            yaml.safe_dump(report_preview, sort_keys=False).rstrip(),
            "```",
            "",
        ]
    )

    if run.expected_tables is not None and run.expected_tables.is_file():
        lines.extend(
            [
                f"**Expected PyArrow Tables (`{rel_posix(run.expected_tables)}` — `{run.tables_attr}`)**",
                "",
                _code_block(run.expected_tables, "python"),
                "",
            ]
        )

    if run.expected_json is not None and run.expected_json.is_file():
        lines.extend(
            [
                f"**Expected Canonical Table Rows (`{rel_posix(run.expected_json)}`)**",
                "",
                _code_block(run.expected_json, "json"),
                "",
            ]
        )

    return lines


def _render_preset_case(case: PipelineCase, catalogue: RuleCatalogue) -> list[str]:
    """Render a pipeline case section with metadata header and run previews."""
    resolved = resolve_pipeline(case.pipeline)
    settings_cls_name = resolved.settings_cls.__name__ if resolved.settings_cls else ""
    lines = [
        f'<a id="{case.anchor}"></a>',
        "",
        f"### {case.title} (`{case.suite}/{case.name}`)",
        "",
        f"**Suite:** `{case.suite}` · **Target CLI:** `{case.pipeline}` · **Settings Class:** `{settings_cls_name}`  ",
        f"**Directory:** `{rel_posix(case.directory)}/`  ",
        f"**Rules:** {_format_rule_links(case.rules, catalogue, code_wrap=True)}",
        "",
    ]
    if case.notes:
        lines.extend([case.notes, ""])
    for run in case.runs:
        lines.extend(_render_preset_run(case, run))
    lines.extend(["---", ""])
    return lines


def _render_pipeline_fixture_index(
    cases: Sequence[PipelineCase],
    *,
    heading_num: int,
) -> list[str]:
    """Render the fixture index table for a pipeline specification."""
    out = [
        f"## {heading_num}. Fixture Index",
        "",
        "Every case directory contains a `case.yaml` manifest; the test suites and this document are both driven by it.",
        "",
        "| Case | Target CLI | Manifest | Runs |",
        "| :--- | :--- | :--- | :--- |",
    ]
    for case in cases:
        manifest = case.directory / "case.yaml"
        runs = ", ".join(r.id for r in case.runs)
        out.append(
            f"| [`{case.suite}/{case.name}`](#{case.anchor}) | `{case.pipeline}` | "
            f"[`case.yaml`]({repo_blob_url(manifest)}) | {runs} |"
        )
    out.append("")
    return out


def _render_pipeline_cases_and_errors(
    spec: PipelineSpecConfig,
    cases: Sequence[PipelineCase],
    catalogue: RuleCatalogue,
    conv_cfg: ConverterSpecConfig | None,
) -> list[str]:
    """Render sections 3..N (base cases, optional preset cases, error table, and fixture index)."""
    lines: list[str] = [
        "",
        "---",
        "",
        f"## 3. Base Pipeline Specification Cases (`{spec.name}` / `spec/`)",
        "",
    ]
    for shared_file in spec.shared_files:
        if shared_file.is_file():
            lines.extend(
                [
                    f"**Shared Fixture Module (`{rel_posix(shared_file)}`)**",
                    "",
                    _code_block(shared_file, "python"),
                    "",
                ]
            )

    if conv_cfg is not None and (spec.data_dir / "spec").is_dir():
        for conv_case in load_cases(spec.data_dir / "spec"):
            lines.extend(_render_case(conv_case, catalogue, conv_cfg))
    else:
        for case in [c for c in cases if not c.is_preset]:
            lines.extend(_render_preset_case(case, catalogue))

    next_heading = 4
    preset_cases = [c for c in cases if c.is_preset]
    if preset_cases:
        lines.extend(
            [
                f"## {next_heading}. Preset Subclass Specification Cases (`presets/`)",
                "",
            ]
        )
        for case in preset_cases:
            lines.extend(_render_preset_case(case, catalogue))
        next_heading += 1

    if conv_cfg is not None and conv_cfg.error_manifest.is_file():
        errors = load_error_cases(conv_cfg.error_manifest)
        lines.extend(_render_errors(errors, conv_cfg, heading_num=next_heading))
        next_heading += 1

    lines.extend(_render_pipeline_fixture_index(cases, heading_num=next_heading))
    return lines


def render_pipeline_spec(spec: PipelineSpecConfig) -> str:
    """Render the complete Markdown specification page for ``spec``.

    :param spec: Pipeline specification configuration to render.
    :type spec: PipelineSpecConfig
    :return: Rendered Markdown document string.
    :rtype: str
    """
    raw_rules = yaml.safe_load((spec.data_dir / "rules.yaml").read_text(encoding="utf-8"))
    catalogue = load_rules(spec.data_dir / "rules.yaml")
    cases = load_pipeline_cases(spec.data_dir)
    conv_cfg = spec.converter_config or PIPELINE_SPECS.get(spec.name, spec).converter_config

    by_rule: dict[str, list[PipelineCase]] = defaultdict(list)
    for case in cases:
        for rid in case.rules:
            by_rule[rid].append(case)

    cli_command = raw_rules.get("cli_command", spec.name)
    settings_class = raw_rules.get("settings_class", "")
    pipeline_runner = raw_rules.get("pipeline_runner", "")

    lines: list[str] = [
        (
            f"<!-- GENERATED FILE - DO NOT EDIT. Source: `specs/pipelines/{spec.name}/`. "
            "Regenerate with `uv run python -m scripts.gen_pipeline_spec` or `uv run python -m scripts.gen_converter_spec`. -->"
        ),
        "",
        f"# {catalogue.title}",
        "",
        catalogue.intro,
        "",
        "| Property | Value |",
        "| --- | --- |",
        f"| **Base Pipeline Family** | `{spec.name}` |",
        f"| **Base CLI Entry Point** | `{cli_command}` |",
        f"| **Base Settings Class** | `{settings_class}` |",
        f"| **Pipeline Runner** | `{pipeline_runner}` |",
        f"| **Preset Subclasses Covered** | {', '.join(f'`{p}`' for p in spec.presets) or '—'} |",
        f"| **Specification Directory** | `specs/pipelines/{spec.name}/` |",
        "",
        *_render_hierarchy_section(spec),
        "## Running Smoke Tests Across Environments",
        "",
        "```bash",
        f"# Run all base and preset smoke tests for {spec.name} across built-in environments",
        f"uv run pytest tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py --smoke-pipeline {spec.name}",
        "",
    ]
    for preset_name in spec.presets:
        lines.extend(
            [
                f"# Run only the {preset_name} preset subclass smoke tests",
                f"uv run pytest tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py --smoke-pipeline {preset_name}",
                "",
            ]
        )
    lines.extend(
        [
            "# Run against a custom environment directory (config.toml, secrets.toml, settings.toml)",
            "uv run pytest tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py \\",
            f"    --smoke-pipeline {spec.name} \\",
            "    --dlt-config-dir /path/to/env_dlt_config \\",
            "    --settings-file /path/to/env_dlt_config/settings.toml",
            "```",
            "",
            "---",
            "",
            "## 1. Conversion & Pipeline Rules",
            "",
        ]
    )

    default_generated_tables: dict[str, Callable[[], list[str]]] = {
        "rejected_schema_table": render_rejected_schema_table,
        "pydantic_scalar_table": render_pydantic_scalar_table,
    }
    generated_tables = (
        {**default_generated_tables, **conv_cfg.generated_tables} if conv_cfg is not None else default_generated_tables
    )
    covering_links = {
        rule_id: (
            ", ".join(f"[`{c.suite}/{c.name}`](#{c.anchor})" for c in by_rule[rule_id]) if by_rule.get(rule_id) else "—"
        )
        for rule_id in catalogue.rules
    }
    lines.extend(_render_rule_sections(catalogue, covering_links, generated_tables))

    lines.extend(
        [
            "---",
            "",
            "## 2. Case Coverage Matrix",
            "",
            "| Suite | Target CLI | Settings Class | Case | Title | Rules | Expected Table Counts |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for case in cases:
        resolved = resolve_pipeline(case.pipeline)
        cls_name = resolved.settings_cls.__name__ if resolved.settings_cls else ""
        counts_summary = "; ".join(f"`{r.id}`: `{r.expected_counts}`" for r in case.runs)
        lines.append(
            f"| `{case.suite}` | `{case.pipeline}` | `{cls_name}` | [`{case.name}`](#{case.anchor}) | {case.title} | "
            f"{_format_rule_links(case.rules, catalogue, code_wrap=True)} | {counts_summary} |"
        )

    lines.extend(_render_pipeline_cases_and_errors(spec, cases, catalogue, conv_cfg))
    return "\n".join(lines).rstrip() + "\n"


def render(config: ConverterSpecConfig) -> str:
    """Render the full specification document as Markdown.

    :param config: Converter specification configuration to render.
    :type config: ConverterSpecConfig
    :return: Rendered Markdown specification string.
    :rtype: str
    """
    if config.data_dir.parent == PIPELINE_SPECS_ROOT:
        return render_pipeline_spec(PIPELINE_SPECS[config.data_dir.name])

    catalogue = load_rules(config.data_dir / "rules.yaml")
    cases = [case for suite in config.suites for case in load_cases(config.data_dir / suite)]
    errors = load_error_cases(config.error_manifest)

    sources = ", ".join(
        f"`{rel_posix(p, REPO_ROOT)}`"
        for p in (config.data_dir / "rules.yaml", *(config.data_dir / s for s in config.suites))
    )
    out = [GENERATED_BANNER.format(sources=sources), "", f"# {catalogue.title}", ""]
    if catalogue.intro:
        out += [catalogue.intro, ""]
    contents = (
        "**Contents:** [1. Conversion Rules](#1-conversion-rules) · [2. Cases](#2-cases) · "
        "[3. Errors](#3-errors-unsupported-invalid-input) · [4. Fixture Index](#4-fixture-index)"
    )
    out += [contents, ""]
    out += _render_rules(catalogue, cases, config)
    out += ["## 2. Cases", ""]
    for case in cases:
        out += _render_case(case, catalogue, config)
    out += _render_errors(errors, config)
    out += _render_index(cases, config)
    return "\n".join(out).rstrip() + "\n"


XSD_TO_PYARROW: Final[ConverterSpecConfig] = ConverterSpecConfig(
    data_dir=REPO_ROOT / "specs" / "schema_converters" / "xsd_to_pyarrow",
    output=DOCS_DIR / "xsd_to_pyarrow_specification.md",
    suites=("spec", "examples"),
    error_manifest=REPO_ROOT / "specs" / "schema_converters" / "xsd_to_pyarrow" / "spec" / "errors" / "manifest.yaml",
    error_base=XsdToPyArrowError,
    error_classes=XSD_TO_PYARROW_ERRORS,
    schema_lang="xml",
    data_lang="xml",
    generated_tables={"scalar_table": render_xsd_scalar_table},
    target_label="xml_tag",
)

JSONSCHEMA_TO_PYARROW: Final[ConverterSpecConfig] = ConverterSpecConfig(
    data_dir=REPO_ROOT / "specs" / "schema_converters" / "jsonschema_to_pyarrow",
    output=DOCS_DIR / "jsonschema_to_pyarrow_specification.md",
    suites=("spec", "examples"),
    error_manifest=(
        REPO_ROOT / "specs" / "schema_converters" / "jsonschema_to_pyarrow" / "spec" / "errors" / "manifest.yaml"
    ),
    error_base=JSONSchemaToPyArrowError,
    error_classes=JSONSCHEMA_TO_PYARROW_ERRORS,
    schema_lang="json",
    data_lang="json",
    generated_tables={"scalar_table": render_jsonschema_scalar_table},
)

PYDANTIC_TO_PYARROW: Final[ConverterSpecConfig] = ConverterSpecConfig(
    data_dir=REPO_ROOT / "specs" / "schema_converters" / "pydantic_to_pyarrow",
    output=DOCS_DIR / "pydantic_to_pyarrow_specification.md",
    suites=("spec", "examples"),
    error_manifest=(
        REPO_ROOT / "specs" / "schema_converters" / "pydantic_to_pyarrow" / "spec" / "errors" / "manifest.yaml"
    ),
    error_base=PydanticToPyArrowError,
    error_classes=PYDANTIC_TO_PYARROW_ERRORS,
    schema_lang="python",
    data_lang="json",
    generated_tables={"scalar_table": render_pydantic_scalar_table},
)

XSV_TO_PYARROW: Final[ConverterSpecConfig] = ConverterSpecConfig(
    data_dir=PIPELINE_SPECS_ROOT / "xsv",
    output=DOCS_DIR / "xsv_specification.md",
    suites=("spec",),
    error_manifest=PIPELINE_SPECS_ROOT / "xsv" / "spec" / "errors" / "manifest.yaml",
    error_base=XsvSchemaError,
    error_classes=XSV_SCHEMA_ERRORS,
    schema_lang="json",
    data_lang="text",
    generated_tables={
        "xsv_config_table": render_xsv_config_table,
        "column_type_table": render_xsv_column_type_table,
    },
)

XMLTOPYARROW_PIPELINE: Final[ConverterSpecConfig] = ConverterSpecConfig(
    data_dir=PIPELINE_SPECS_ROOT / "xmltopyarrow",
    output=DOCS_DIR / "xmltopyarrow_specification.md",
    suites=("spec",),
    error_manifest=PIPELINE_SPECS_ROOT / "xmltopyarrow" / "spec" / "errors" / "manifest.yaml",
    error_base=XmlToPyArrowConfigError,
    error_classes=XMLTOPYARROW_CONFIG_ERRORS,
    schema_lang="xml",
    data_lang="xml",
    generated_tables={
        "presets_table": render_xmltopyarrow_presets_table,
        "rejected_schema_table": render_rejected_schema_table,
    },
    target_label="xml_tag",
)

JSONLINES_JSONSCHEMA_PIPELINE: Final[ConverterSpecConfig] = ConverterSpecConfig(
    data_dir=PIPELINE_SPECS_ROOT / "jsonlines_jsonschema",
    output=DOCS_DIR / "jsonlines_jsonschema_specification.md",
    suites=("spec",),
    error_manifest=PIPELINE_SPECS_ROOT / "jsonlines_jsonschema" / "spec" / "errors" / "manifest.yaml",
    error_base=JsonlJsonschemaError,
    error_classes=JSONLINES_JSONSCHEMA_ERRORS,
    schema_lang="json",
    data_lang="json",
    generated_tables={
        "supported_drafts_table": render_jsonlines_jsonschema_drafts_table,
        "rejected_schema_table": render_rejected_schema_table,
    },
)

PIPELINE_SPECS: dict[str, PipelineSpecConfig] = {
    "xmltopyarrow": PipelineSpecConfig(
        name="xmltopyarrow",
        data_dir=PIPELINE_SPECS_ROOT / "xmltopyarrow",
        output_path=DOCS_DIR / "xmltopyarrow_specification.md",
        presets=("pubmed",),
        shared_files=(PIPELINE_SPECS_ROOT / "xmltopyarrow" / "transforms.py",),
        converter_config=XMLTOPYARROW_PIPELINE,
    ),
    "jsonlines_jsonschema": PipelineSpecConfig(
        name="jsonlines_jsonschema",
        data_dir=PIPELINE_SPECS_ROOT / "jsonlines_jsonschema",
        output_path=DOCS_DIR / "jsonlines_jsonschema_specification.md",
        presets=("ncbi_datasets",),
        shared_files=(
            PIPELINE_SPECS_ROOT / "jsonlines_jsonschema" / "sample_registry.py",
            PIPELINE_SPECS_ROOT / "jsonlines_jsonschema" / "transforms.py",
        ),
        converter_config=JSONLINES_JSONSCHEMA_PIPELINE,
    ),
    "xsv": PipelineSpecConfig(
        name="xsv",
        data_dir=PIPELINE_SPECS_ROOT / "xsv",
        output_path=DOCS_DIR / "xsv_specification.md",
        presets=(),
        shared_files=(PIPELINE_SPECS_ROOT / "xsv" / "transforms.py",),
        converter_config=XSV_TO_PYARROW,
    ),
    "jsonlines_extract": PipelineSpecConfig(
        name="jsonlines_extract",
        data_dir=PIPELINE_SPECS_ROOT / "jsonlines_extract",
        output_path=DOCS_DIR / "jsonlines_extract_specification.md",
        presets=(),
        shared_files=(PIPELINE_SPECS_ROOT / "jsonlines_extract" / "transforms.py",),
    ),
    "jsonlines_pydantic": PipelineSpecConfig(
        name="jsonlines_pydantic",
        data_dir=PIPELINE_SPECS_ROOT / "jsonlines_pydantic",
        output_path=DOCS_DIR / "jsonlines_pydantic_specification.md",
        presets=(),
        shared_files=(
            PIPELINE_SPECS_ROOT / "jsonlines_pydantic" / "sample_registry.py",
            PIPELINE_SPECS_ROOT / "jsonlines_pydantic" / "transforms.py",
        ),
    ),
    "xmltodict": PipelineSpecConfig(
        name="xmltodict",
        data_dir=PIPELINE_SPECS_ROOT / "xmltodict",
        output_path=DOCS_DIR / "xmltodict_specification.md",
        presets=(),
        shared_files=(PIPELINE_SPECS_ROOT / "xmltodict" / "transforms.py",),
    ),
}

CONFIGS: Final[tuple[ConverterSpecConfig, ...]] = (
    XSD_TO_PYARROW,
    JSONSCHEMA_TO_PYARROW,
    PYDANTIC_TO_PYARROW,
    XSV_TO_PYARROW,
    XMLTOPYARROW_PIPELINE,
    JSONLINES_JSONSCHEMA_PIPELINE,
)


def main(argv: Sequence[str] | None = None) -> int:
    """Write or check every configured specification document.

    :param argv: Optional sequence of CLI arguments (defaults to ``sys.argv[1:]``).
    :type argv: Sequence[str] | None
    :return: Process exit code (``0`` on success, ``1`` if ``--check`` detects stale files).
    :rtype: int
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if a committed document is out of date")
    args = parser.parse_args(argv)

    file_entries = (
        *((config.output, render(config)) for config in CONFIGS),
        *(
            (spec.output_path, render_pipeline_spec(spec))
            for spec in PIPELINE_SPECS.values()
            if spec.converter_config is None
        ),
    )
    return check_or_write_files(
        file_entries,
        check=args.check,
        repo_root=REPO_ROOT,
    )


if __name__ == "__main__":
    sys.exit(main())
