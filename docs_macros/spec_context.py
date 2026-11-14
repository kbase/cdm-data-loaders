"""Converter and pipeline specification context builders for Jinja2 documentation templates."""

import importlib
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Any, Final

import yaml
from pydantic import BeforeValidator, computed_field, model_validator

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.pipelines.jsonlines.settings import (
    JSONLINES_JSONSCHEMA_ERRORS,
    JsonlJsonschemaError,
)
from cdm_data_loaders.pipelines.xmltopyarrow.settings import (
    XMLTOPYARROW_CONFIG_ERRORS,
    XmlToPyArrowConfigError,
)
from cdm_data_loaders.pipelines.xsv.settings import (
    XSV_SCHEMA_ERRORS,
    XsvSchemaError,
)
from docs_macros.constants import (
    CONVERTER_SPECS_ROOT,
    DOCS_DIR,
    PIPELINE_SPECS_ROOT,
    REPO_ROOT,
)
from docs_macros.formatting import (
    CodeBlockContext,
    FrozenDocModel,
    ModuleApiUrl,
    ModuleBlobUrl,
    NonNullStr,
    NormalizedProse,
    format_value_code,
    read_code_block,
    rel_posix,
    repo_blob_url,
)
from docs_macros.tables_context import (
    TABLE_TEMPLATE_REGISTRY,
    SimpleTableRow,
    TableRenderContext,
    build_table_context,
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


class ConverterSpecConfig(FrozenDocModel):
    """Configuration describing a schema converter or schema-backed pipeline specification."""

    name: str
    data_dir: Path
    output: Path
    suites: tuple[str, ...]
    error_manifest: Path
    error_base: type[Exception]
    error_classes: Sequence[type[Exception]]
    schema_lang: str
    data_lang: str
    generated_tables: Mapping[str, str]
    target_label: str | None = None
    expected_schema_lang: str = "python"
    expected_schema_heading: str = "PyArrow schema"
    run_output_heading: str = "Converted rows"
    facade_module: str = ""


class PipelineSpecConfig(FrozenDocModel):
    """Configuration describing a pipeline family's specification page."""

    name: str
    data_dir: Path
    output_path: Path
    presets: tuple[str, ...] = ()
    shared_files: tuple[Path, ...] = ()
    converter_config: ConverterSpecConfig | None = None


def _coerce_indented_statement(value: object) -> str:
    """Indent continuation lines of a rule statement by two spaces for Markdown list items.

    :param value: Raw rule statement string.
    :type value: object
    :return: Statement with continuation lines indented by two spaces.
    :rtype: str
    """
    if isinstance(value, str):
        return value.replace("\n", "\n  ")
    msg = f"Expected str, got {type(value).__name__}"
    raise TypeError(msg)


type IndentedStatement = Annotated[str, BeforeValidator(_coerce_indented_statement)]


class _RuleAnchorModel(FrozenDocModel):
    """Shared base providing a lowercase HTML anchor derived from ``id``."""

    id: str

    @computed_field
    @property
    def anchor(self) -> str:
        """Lowercase HTML anchor identifier derived from ``id``.

        :return: Lowercase rule anchor ID.
        :rtype: str
        """
        return self.id.lower()


class RuleRefContext(_RuleAnchorModel):
    """Rule identifier and target anchor for inline rule link lists."""


class CaseLinkContext(FrozenDocModel):
    """Case display label and target anchor for rule coverage summaries."""

    label: str
    anchor: str
    code_wrap: bool


class RuleItemContext(_RuleAnchorModel):
    """Single rule statement, HTML anchor, and covering case links inside a rule section."""

    statement: IndentedStatement
    covering_cases: tuple[CaseLinkContext, ...]
    empty_cases_fallback: str


class RuleSectionContext(FrozenDocModel):
    """Numbered ``### 1.N`` rule section with optional intro and attached reference table."""

    number: int
    title: str
    intro: str
    rules: tuple[RuleItemContext, ...]
    table: TableRenderContext | None


class ConverterRunInputContext(FrozenDocModel):
    """Single embedded input file preview within a converter specification run."""

    label: str
    code_block: CodeBlockContext


class ConverterExtraOutputContext(FrozenDocModel):
    """Extra named table JSON output block within a converter or pipeline specification run."""

    table_name: str
    code_block: CodeBlockContext


class ConverterLinkedFileContext(FrozenDocModel):
    """GitHub blob file link for non-embedded ``examples/*`` runs."""

    name: str
    url: str


class ConverterRunContext(FrozenDocModel):
    """View model for a single run inside ``partials/converter_case.md.j2``."""

    id: str
    param_pairs: tuple[tuple[str, object], ...]
    xml_tag_override_repr: str | None
    inputs: tuple[ConverterRunInputContext, ...]
    expected_json_block: CodeBlockContext | None
    extra_outputs: tuple[ConverterExtraOutputContext, ...]
    expected_rejected_block: CodeBlockContext | None
    linked_files: tuple[ConverterLinkedFileContext, ...]


class ConverterCaseContext(FrozenDocModel):
    """View model for a converter or schema-backed pipeline case in ``partials/converter_case.md.j2``."""

    suite: str
    name: str
    title: str
    anchor: str
    embed: bool
    rules: tuple[RuleRefContext, ...]
    target_label: str | None
    xml_tag: str | None
    xml_tag_repr: str | None
    notes: NonNullStr
    schema_name: str
    schema_rel_path: str
    schema_url: str
    schema_block: CodeBlockContext
    runs: tuple[ConverterRunContext, ...]
    run_output_heading: str
    expected_schema_heading: str
    expected_schema_name: str
    expected_schema_url: str
    expected_schema_block: CodeBlockContext
    manifest_url: str

    @computed_field
    @property
    def run_ids_csv(self) -> str:
        """Comma-separated run IDs for the case.

        :return: Comma-separated run identifier string.
        :rtype: str
        """
        return ", ".join(r.id for r in self.runs)


class ErrorFixtureRefContext(FrozenDocModel):
    """Single error fixture file link, parameters, description, and code block in ``partials/errors_section.md.j2``."""

    name: str
    url: str
    param_pairs: tuple[tuple[str, str], ...]
    description: NormalizedProse
    code_block: CodeBlockContext


class ErrorRowContext(FrozenDocModel):
    """Error item model for ``partials/errors_section.md.j2``."""

    code: str
    anchor: str = ""
    exception_name: str
    title: str
    reason: NormalizedProse
    fixtures: tuple[ErrorFixtureRefContext, ...]

    @model_validator(mode="before")
    @classmethod
    def _populate_default_anchor(cls, data: object) -> object:
        """Populate ``anchor`` from ``code.lower()`` when not explicitly provided.

        :param data: Raw input mapping or model data.
        :type data: object
        :return: Input data with ``anchor`` populated when empty or omitted.
        :rtype: object
        """
        if isinstance(data, dict) and not data.get("anchor"):
            return {**data, "anchor": str(data.get("code", "")).lower()}
        return data


class ErrorSectionContext(FrozenDocModel):
    """View model for ``partials/errors_section.md.j2``."""

    base_class_name: str
    parent_type_name: str
    rows: tuple[ErrorRowContext, ...]


class PresetOverrideRowContext(FrozenDocModel):
    """Row model comparing a preset settings attribute to its base settings class."""

    attr_name: str
    kind: str
    base_val: str
    preset_val: str


class PresetDetailContext(FrozenDocModel):
    """Preset settings subclass metadata and override rows in ``partials/settings_hierarchy.md.j2``."""

    cli_command: str
    class_name: str
    settings_api_url: ModuleApiUrl = None
    base_class_name: str
    shared_runner: str
    shared_runner_url: ModuleBlobUrl = None
    overrides: tuple[PresetOverrideRowContext, ...]


class SettingsHierarchyContext(FrozenDocModel):
    """View model for ``partials/settings_hierarchy.md.j2``."""

    base_cli_command: str
    base_class_name: str
    base_settings_url: ModuleBlobUrl = None
    runner_name: str
    runner_url: ModuleBlobUrl = None
    presets: tuple[PresetDetailContext, ...]


class PipelineInputPreviewContext(FrozenDocModel):
    """Input file preview block within ``partials/pipeline_case.md.j2``."""

    rel_in: str
    repo_rel: str
    decompressed: bool
    code_block: CodeBlockContext


class PipelineRunContext(FrozenDocModel):
    """View model for a single smoke/preset pipeline run in ``partials/pipeline_case.md.j2``."""

    id: str
    show_heading: bool
    settings_rel: str | None
    settings_block: CodeBlockContext | None
    inputs: tuple[PipelineInputPreviewContext, ...]
    expected_counts_repr: str
    report_yaml_block: CodeBlockContext
    expected_tables_rel: str | None
    tables_attr: str
    expected_tables_block: CodeBlockContext | None
    expected_json_rel: str | None
    expected_json_block: CodeBlockContext | None


class PipelineCaseContext(FrozenDocModel):
    """View model for a smoke or preset pipeline case in ``partials/pipeline_case.md.j2``."""

    suite: str
    name: str
    title: str
    anchor: str
    pipeline: str
    settings_class_name: str
    directory_rel: str
    rules: tuple[RuleRefContext, ...]
    notes: NonNullStr
    runs: tuple[PipelineRunContext, ...]
    run_count_entries: tuple[tuple[str, str], ...]
    manifest_url: str

    @computed_field
    @property
    def run_ids_csv(self) -> str:
        """Comma-separated run IDs for the case.

        :return: Comma-separated run identifier string.
        :rtype: str
        """
        return ", ".join(r.id for r in self.runs)


class SharedFileContext(FrozenDocModel):
    """Shared Python fixture module block rendered at the start of base pipeline cases."""

    repo_rel: str
    url: str
    code_block: CodeBlockContext


class ConverterSpecPageContext(FrozenDocModel):
    """Top-level view model passed to ``pages/converter_spec.md.j2``."""

    title: str
    intro: NonNullStr
    spec_dir_rel: str
    spec_dir_url: str
    facade_module: str
    facade_url: ModuleBlobUrl = None
    target_label: str | None
    sections: tuple[RuleSectionContext, ...]
    cases: tuple[ConverterCaseContext, ...]
    error_section: ErrorSectionContext

    @computed_field
    @property
    def spec_cases(self) -> tuple[ConverterCaseContext, ...]:
        """Cases belonging to the ``spec`` suite.

        :return: Tuple of ``spec`` suite cases.
        :rtype: tuple[ConverterCaseContext, ...]
        """
        return tuple(c for c in self.cases if c.suite == "spec")

    @computed_field
    @property
    def example_cases(self) -> tuple[ConverterCaseContext, ...]:
        """Cases belonging to the ``examples`` suite.

        :return: Tuple of ``examples`` suite cases.
        :rtype: tuple[ConverterCaseContext, ...]
        """
        return tuple(c for c in self.cases if c.suite == "examples")

    @property
    def fixture_index_table(self) -> TableRenderContext:
        """Build the unified fixture index table context for ``partials/fixture_index.md.j2``.

        :return: ``TableRenderContext`` for the converter fixture index.
        :rtype: TableRenderContext
        """
        if self.target_label:
            headers = ("Case", f"`{self.target_label}`", "Schema", "Runs")
            rows = tuple(
                SimpleTableRow(
                    cells=(
                        f"[`{c.suite}/{c.name}`]({c.manifest_url})",
                        f"`{c.xml_tag}`",
                        f"[`{c.schema_name}`]({c.schema_url})",
                        c.run_ids_csv,
                    )
                )
                for c in self.cases
            )
        else:
            headers = ("Case", "Schema", "Runs")
            rows = tuple(
                SimpleTableRow(
                    cells=(
                        f"[`{c.suite}/{c.name}`]({c.manifest_url})",
                        f"[`{c.schema_name}`]({c.schema_url})",
                        c.run_ids_csv,
                    )
                )
                for c in self.cases
            )
        return TableRenderContext(table_id="fixture_index", headers=headers, rows=rows)


class PipelineSpecPageContext(FrozenDocModel):
    """Top-level view model passed to ``pages/pipeline_spec.md.j2``."""

    name: str
    title: str
    intro: NonNullStr
    cli_command: str
    settings_class: str
    settings_class_url: ModuleBlobUrl = None
    pipeline_runner: str
    pipeline_runner_url: ModuleBlobUrl = None
    spec_dir_rel: str
    spec_dir_url: str
    presets: tuple[str, ...]
    hierarchy: SettingsHierarchyContext | None
    sections: tuple[RuleSectionContext, ...]
    all_cases: tuple[PipelineCaseContext, ...]
    shared_files: tuple[SharedFileContext, ...]
    use_converter_base_cases: bool
    converter_base_cases: tuple[ConverterCaseContext, ...]
    smoke_base_cases: tuple[PipelineCaseContext, ...]
    preset_cases: tuple[PipelineCaseContext, ...]
    error_section: ErrorSectionContext | None

    @property
    def fixture_index_table(self) -> TableRenderContext:
        """Build the unified fixture index table context for ``partials/fixture_index.md.j2``.

        :return: ``TableRenderContext`` for the pipeline fixture index.
        :rtype: TableRenderContext
        """
        rows = tuple(
            SimpleTableRow(
                cells=(
                    f"[`{c.suite}/{c.name}`](#{c.anchor})",
                    f"`{c.pipeline}`",
                    f"[`case.yaml`]({c.manifest_url})",
                    c.run_ids_csv,
                )
            )
            for c in self.all_cases
        )
        return TableRenderContext(
            table_id="fixture_index",
            headers=("Case", "Target CLI", "Manifest", "Runs"),
            rows=rows,
        )


class SourceFormatDefaults(FrozenDocModel):
    """Default source schema/data code-fence languages and target selector label for ``<source>_to_<target>``."""

    schema_lang: str
    data_lang: str
    target_label: str | None = None


class TargetFormatDefaults(FrozenDocModel):
    """Default expected schema language and section headings for ``<source>_to_<target>``."""

    expected_schema_lang: str
    expected_schema_heading: str
    run_output_heading: str


PRESET_EXTRA_ATTRS: Final[tuple[str, ...]] = (
    "_PRECOMPUTED_SCHEMAS",
    "_PRECOMPUTED_SCHEMA",
    "_DEFAULT_FILE_PREPROCESSORS",
)

CONVERTER_SUITE_NAMES: Final[tuple[str, ...]] = ("spec", "examples")
CONVERTER_FACADES_PACKAGE: Final[str] = "cdm_data_loaders.converters.facades"

SOURCE_FORMAT_DEFAULTS: Final[MappingProxyType[str, SourceFormatDefaults]] = MappingProxyType(
    {
        "xsd": SourceFormatDefaults(schema_lang="xml", data_lang="xml", target_label="xml_tag"),
        "jsonschema": SourceFormatDefaults(schema_lang="json", data_lang="json"),
        "pydantic": SourceFormatDefaults(schema_lang="python", data_lang="json"),
        "pyarrow": SourceFormatDefaults(schema_lang="python", data_lang="python"),
        "pyiceberg": SourceFormatDefaults(schema_lang="python", data_lang="python"),
        "dlt": SourceFormatDefaults(schema_lang="json", data_lang="json"),
    }
)

TARGET_FORMAT_DEFAULTS: Final[MappingProxyType[str, TargetFormatDefaults]] = MappingProxyType(
    {
        "pyarrow": TargetFormatDefaults(
            expected_schema_lang="python",
            expected_schema_heading="PyArrow schema",
            run_output_heading="Converted rows",
        ),
        "jsonschema": TargetFormatDefaults(
            expected_schema_lang="json",
            expected_schema_heading="JSON Schema",
            run_output_heading="Serialized records",
        ),
        "linkml": TargetFormatDefaults(
            expected_schema_lang="yaml",
            expected_schema_heading="LinkML schema",
            run_output_heading="Serialized records",
        ),
        "dlt": TargetFormatDefaults(
            expected_schema_lang="json",
            expected_schema_heading="dlt schema",
            run_output_heading="Converted rows",
        ),
        "pyspark": TargetFormatDefaults(
            expected_schema_lang="python",
            expected_schema_heading="PySpark schema",
            run_output_heading="Converted rows",
        ),
    }
)

DEFAULT_PIPELINE_TABLES: Final[MappingProxyType[str, str]] = MappingProxyType(
    {
        "rejected_schema_table": "rejected_schema_table",
        "pydantic_scalar_table": "pydantic_scalar_table",
    }
)


def _resolve_converter_facade_errors(
    name: str,
    facade_module_name: str,
) -> tuple[type[Exception], tuple[type[Exception], ...]]:
    """Introspect ``facade_module_name`` to resolve ``(error_base, error_classes)`` for converter ``name``.

    :param name: Converter identifier (e.g. ``"pydantic_to_pyarrow"``).
    :type name: str
    :param facade_module_name: Dotted Python module path of the converter facade.
    :type facade_module_name: str
    :return: Pair of ``(error_base_cls, error_classes_tuple)``.
    :rtype: tuple[type[Exception], tuple[type[Exception], ...]]
    """
    facade_mod = importlib.import_module(facade_module_name)
    errors_attr = f"{name.upper()}_ERRORS"
    if not hasattr(facade_mod, errors_attr):
        msg = f"Facade module {facade_module_name!r} does not define {errors_attr!r}"
        raise ValueError(msg)
    error_classes = tuple(getattr(facade_mod, errors_attr))
    candidates = [
        obj
        for obj in vars(facade_mod).values()
        if isinstance(obj, type)
        and issubclass(obj, ConversionError)
        and obj.__module__ == facade_mod.__name__
        and ConversionError in obj.__bases__
        and all(issubclass(err_cls, obj) for err_cls in error_classes)
    ]
    if not candidates:
        msg = f"Cannot resolve direct ConversionError base subclass in {facade_module_name!r}"
        raise ValueError(msg)
    return candidates[0], error_classes


def _resolve_converter_generated_tables(
    name: str,
    source_fmt: str,
    raw_rules: Mapping[str, Any],
) -> MappingProxyType[str, str]:
    """Resolve ``generated:`` section table keys in ``rules.yaml`` to ``TABLE_TEMPLATE_REGISTRY`` IDs.

    :param name: Converter identifier (e.g. ``"pyarrow_to_linkml"``).
    :type name: str
    :param source_fmt: Source format prefix before ``_to_`` (e.g. ``"pyarrow"`` or ``"xsd"``).
    :type source_fmt: str
    :param raw_rules: Parsed top-level ``rules.yaml`` mapping.
    :type raw_rules: Mapping[str, Any]
    :return: Read-only mapping from ``rules.yaml`` ``generated`` keys to ``TABLE_TEMPLATE_REGISTRY`` keys.
    :rtype: MappingProxyType[str, str]
    """
    resolved: dict[str, str] = {}
    for section in raw_rules.get("sections", ()) or ():
        if not isinstance(section, Mapping):
            continue
        gen_key = section.get("generated")
        if not isinstance(gen_key, str):
            continue
        if gen_key == "scalar_table":
            full_key = f"{name}_scalar_table"
            src_key = f"{source_fmt}_scalar_table"
            if full_key in TABLE_TEMPLATE_REGISTRY:
                resolved[gen_key] = full_key
            elif src_key in TABLE_TEMPLATE_REGISTRY:
                resolved[gen_key] = src_key
        elif gen_key in TABLE_TEMPLATE_REGISTRY:
            resolved[gen_key] = gen_key

    explicit = raw_rules.get("generated_tables")
    if isinstance(explicit, Mapping):
        resolved.update({str(k): str(v) for k, v in explicit.items()})
    return MappingProxyType(resolved)


def _build_converter_spec_config(
    data_dir: Path,
    docs_dir: Path,
    *,
    facade_package: str = CONVERTER_FACADES_PACKAGE,
) -> ConverterSpecConfig:
    """Construct a ``ConverterSpecConfig`` from a ``specs/schema_converters/<xxx_to_yyy>/`` directory.

    :param data_dir: Converter specification directory containing ``rules.yaml``.
    :type data_dir: Path
    :param docs_dir: Output ``docs/`` directory for ``<name>_specification.md``.
    :type docs_dir: Path
    :param facade_package: Base package containing converter facade modules.
    :type facade_package: str
    :return: Populated ``ConverterSpecConfig`` instance.
    :rtype: ConverterSpecConfig
    """
    name = data_dir.name
    raw_rules: Mapping[str, Any] = yaml.safe_load((data_dir / "rules.yaml").read_text(encoding="utf-8")) or {}
    source_fmt, _, target_fmt = name.partition("_to_")
    src_defaults = SOURCE_FORMAT_DEFAULTS.get(source_fmt)
    tgt_defaults = TARGET_FORMAT_DEFAULTS.get(target_fmt)

    schema_lang = str(raw_rules.get("schema_lang", src_defaults.schema_lang if src_defaults else ""))
    data_lang = str(raw_rules.get("data_lang", src_defaults.data_lang if src_defaults else ""))
    if not schema_lang or not data_lang:
        msg = f"Cannot infer schema_lang/data_lang for converter {name!r}; set them in rules.yaml or SOURCE_FORMAT_DEFAULTS."
        raise ValueError(msg)

    target_label: str | None = raw_rules.get("target_label", src_defaults.target_label if src_defaults else None)
    expected_schema_lang = str(
        raw_rules.get("expected_schema_lang", tgt_defaults.expected_schema_lang if tgt_defaults else "")
    )
    expected_schema_heading = str(
        raw_rules.get("expected_schema_heading", tgt_defaults.expected_schema_heading if tgt_defaults else "")
    )
    run_output_heading = str(
        raw_rules.get("run_output_heading", tgt_defaults.run_output_heading if tgt_defaults else "")
    )
    if not expected_schema_lang or not expected_schema_heading or not run_output_heading:
        msg = f"Cannot infer target rendering defaults for converter {name!r}; set them in rules.yaml or TARGET_FORMAT_DEFAULTS."
        raise ValueError(msg)

    facade_module_name = str(raw_rules.get("facade_module", f"{facade_package}.{name}"))
    error_base, error_classes = _resolve_converter_facade_errors(name, facade_module_name)
    discovered_suites = tuple(suite for suite in CONVERTER_SUITE_NAMES if (data_dir / suite).is_dir())

    return ConverterSpecConfig(
        name=name,
        data_dir=data_dir,
        output=docs_dir / f"{name}_specification.md",
        suites=discovered_suites or ("spec",),
        error_manifest=data_dir / "spec" / "errors" / "manifest.yaml",
        error_base=error_base,
        error_classes=error_classes,
        schema_lang=schema_lang,
        data_lang=data_lang,
        generated_tables=_resolve_converter_generated_tables(name, source_fmt, raw_rules),
        target_label=target_label,
        expected_schema_lang=expected_schema_lang,
        expected_schema_heading=expected_schema_heading,
        run_output_heading=run_output_heading,
        facade_module=facade_module_name,
    )


def discover_converter_specs(
    specs_root: Path = CONVERTER_SPECS_ROOT,
    docs_dir: Path = DOCS_DIR,
    *,
    facade_package: str = CONVERTER_FACADES_PACKAGE,
) -> MappingProxyType[str, ConverterSpecConfig]:
    """Auto-discover all converter specification suites under ``specs_root``.

    :param specs_root: Root directory containing ``<source>_to_<target>/rules.yaml`` subdirectories.
    :type specs_root: Path
    :param docs_dir: Output ``docs/`` directory for ``<name>_specification.md`` files.
    :type docs_dir: Path
    :param facade_package: Base package containing ``<source>_to_<target>`` facade modules.
    :type facade_package: str
    :return: Read-only mapping from converter name to ``ConverterSpecConfig``.
    :rtype: MappingProxyType[str, ConverterSpecConfig]
    """
    if not specs_root.is_dir():
        return MappingProxyType({})
    configs: dict[str, ConverterSpecConfig] = {}
    for candidate in sorted(specs_root.iterdir()):
        if candidate.is_dir() and (candidate / "rules.yaml").is_file():
            configs[candidate.name] = _build_converter_spec_config(
                candidate,
                docs_dir,
                facade_package=facade_package,
            )
    return MappingProxyType(configs)


XSV_TO_PYARROW: Final[ConverterSpecConfig] = ConverterSpecConfig(
    name="xsv",
    data_dir=PIPELINE_SPECS_ROOT / "xsv",
    output=DOCS_DIR / "xsv_specification.md",
    suites=("spec",),
    error_manifest=PIPELINE_SPECS_ROOT / "xsv" / "spec" / "errors" / "manifest.yaml",
    error_base=XsvSchemaError,
    error_classes=XSV_SCHEMA_ERRORS,
    schema_lang="json",
    data_lang="text",
    generated_tables=MappingProxyType(
        {
            "xsv_config_table": "xsv_config_table",
            "column_type_table": "column_type_table",
        }
    ),
)

XMLTOPYARROW_PIPELINE: Final[ConverterSpecConfig] = ConverterSpecConfig(
    name="xmltopyarrow",
    data_dir=PIPELINE_SPECS_ROOT / "xmltopyarrow",
    output=DOCS_DIR / "xmltopyarrow_specification.md",
    suites=("spec",),
    error_manifest=PIPELINE_SPECS_ROOT / "xmltopyarrow" / "spec" / "errors" / "manifest.yaml",
    error_base=XmlToPyArrowConfigError,
    error_classes=XMLTOPYARROW_CONFIG_ERRORS,
    schema_lang="xml",
    data_lang="xml",
    generated_tables=MappingProxyType(
        {
            "presets_table": "presets_table",
            "rejected_schema_table": "rejected_schema_table",
        }
    ),
    target_label="xml_tag",
)

JSONLINES_JSONSCHEMA_PIPELINE: Final[ConverterSpecConfig] = ConverterSpecConfig(
    name="jsonlines_jsonschema",
    data_dir=PIPELINE_SPECS_ROOT / "jsonlines_jsonschema",
    output=DOCS_DIR / "jsonlines_jsonschema_specification.md",
    suites=("spec",),
    error_manifest=PIPELINE_SPECS_ROOT / "jsonlines_jsonschema" / "spec" / "errors" / "manifest.yaml",
    error_base=JsonlJsonschemaError,
    error_classes=JSONLINES_JSONSCHEMA_ERRORS,
    schema_lang="json",
    data_lang="json",
    generated_tables=MappingProxyType(
        {
            "supported_drafts_table": "supported_drafts_table",
            "rejected_schema_table": "rejected_schema_table",
        }
    ),
)

CONVERTER_SPECS: Final[MappingProxyType[str, ConverterSpecConfig]] = discover_converter_specs()

PIPELINE_SPECS: Final[MappingProxyType[str, PipelineSpecConfig]] = MappingProxyType(
    {
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
)


def _resolve_rule_refs(rule_ids: Sequence[str], catalogue: RuleCatalogue) -> tuple[RuleRefContext, ...]:
    """Filter ``rule_ids`` against ``catalogue`` into ``RuleRefContext`` items.

    :param rule_ids: Candidate rule identifier strings.
    :type rule_ids: Sequence[str]
    :param catalogue: Parsed rule catalogue.
    :type catalogue: RuleCatalogue
    :return: Tuple of resolved ``RuleRefContext`` objects.
    :rtype: tuple[RuleRefContext, ...]
    """
    return tuple(RuleRefContext(id=rid) for rid in rule_ids if rid in catalogue.rules)


def _build_rule_sections(
    catalogue: RuleCatalogue,
    covering_map: Mapping[str, Sequence[CaseLinkContext]],
    table_key_map: Mapping[str, str],
    *,
    empty_cases_fallback: str,
) -> tuple[RuleSectionContext, ...]:
    """Build numbered ``RuleSectionContext`` objects with resolved covering cases and reference tables.

    :param catalogue: Parsed rule catalogue from ``rules.yaml``.
    :type catalogue: RuleCatalogue
    :param covering_map: Mapping of rule ID to covering case link models.
    :type covering_map: Mapping[str, Sequence[CaseLinkContext]]
    :param table_key_map: Mapping from ``rules.yaml`` ``generated`` keys to ``TABLE_TEMPLATE_REGISTRY`` IDs.
    :type table_key_map: Mapping[str, str]
    :param empty_cases_fallback: Fallback string when a rule has zero covering cases.
    :type empty_cases_fallback: str
    :return: Tuple of ``RuleSectionContext`` items.
    :rtype: tuple[RuleSectionContext, ...]
    """
    sections: list[RuleSectionContext] = []
    for number, section in enumerate(catalogue.sections, start=1):
        rules = tuple(
            RuleItemContext(
                id=rule.id,
                statement=rule.statement,
                covering_cases=tuple(covering_map.get(rule.id, ())),
                empty_cases_fallback=empty_cases_fallback,
            )
            for rule in section.rules
        )
        table_ctx = (
            build_table_context(table_key_map[section.generated])
            if section.generated and section.generated in table_key_map
            else None
        )
        sections.append(
            RuleSectionContext(
                number=number,
                title=section.title,
                intro=section.intro or "",
                rules=rules,
                table=table_ctx,
            )
        )
    return tuple(sections)


def _build_converter_run_context(
    run: Run,
    case: Case,
    config: ConverterSpecConfig,
    *,
    embed: bool,
    seen_data: set[Path],
) -> ConverterRunContext:
    """Build ``ConverterRunContext`` for a single ``Run`` in embedded or linked mode.

    :param run: Converter run manifest entry.
    :type run: Run
    :param case: Enclosing converter case manifest.
    :type case: Case
    :param config: Converter specification configuration.
    :type config: ConverterSpecConfig
    :param embed: Whether source and output files are embedded inline (``spec`` suite).
    :type embed: bool
    :param seen_data: Mutable set tracking already-rendered input data paths across runs in a case.
    :type seen_data: set[Path]
    :return: Populated ``ConverterRunContext``.
    :rtype: ConverterRunContext
    """
    xml_override = repr(run.xml_tag) if config.target_label and run.xml_tag != case.xml_tag else None
    param_pairs = tuple(run.params.items())

    if not embed:
        unique_files = list(dict.fromkeys((run.data, run.expected_json, run.expected_table)))
        linked = tuple(ConverterLinkedFileContext(name=f.name, url=repo_blob_url(f)) for f in unique_files)
        return ConverterRunContext(
            id=run.id,
            param_pairs=param_pairs,
            xml_tag_override_repr=xml_override,
            inputs=(),
            expected_json_block=None,
            extra_outputs=(),
            expected_rejected_block=None,
            linked_files=linked,
        )

    inputs: list[ConverterRunInputContext] = []
    if run.data not in seen_data:
        if run.data.is_dir():
            inputs.extend(
                ConverterRunInputContext(
                    label=rel_posix(data_file, case.directory),
                    code_block=read_code_block(data_file, config.data_lang),
                )
                for data_file in sorted(p for p in run.data.rglob("*") if p.is_file())
            )
        else:
            inputs.append(
                ConverterRunInputContext(
                    label=run.data.name,
                    code_block=read_code_block(run.data, config.data_lang),
                )
            )
        seen_data.add(run.data)

    extra_outputs = tuple(
        ConverterExtraOutputContext(
            table_name=extra_table,
            code_block=read_code_block(extra_path, "json"),
        )
        for extra_table, extra_path in run.extra_expected_json.items()
    )
    rejected_block = read_code_block(run.expected_rejected, "json") if run.expected_rejected is not None else None
    return ConverterRunContext(
        id=run.id,
        param_pairs=param_pairs,
        xml_tag_override_repr=xml_override,
        inputs=tuple(inputs),
        expected_json_block=read_code_block(run.expected_json, "json"),
        extra_outputs=extra_outputs,
        expected_rejected_block=rejected_block,
        linked_files=(),
    )


def _build_converter_case_context(
    case: Case,
    catalogue: RuleCatalogue,
    config: ConverterSpecConfig,
) -> ConverterCaseContext:
    """Build ``ConverterCaseContext`` for a ``Case`` manifest entry.

    :param case: Converter case manifest.
    :type case: Case
    :param catalogue: Parsed rule catalogue.
    :type catalogue: RuleCatalogue
    :param config: Converter specification configuration.
    :type config: ConverterSpecConfig
    :return: Populated ``ConverterCaseContext``.
    :rtype: ConverterCaseContext
    """
    embed = case.suite == "spec"
    seen_data: set[Path] = set()
    runs = tuple(_build_converter_run_context(run, case, config, embed=embed, seen_data=seen_data) for run in case.runs)
    return ConverterCaseContext.model_validate(
        {
            "suite": case.suite,
            "name": case.name,
            "title": case.title,
            "anchor": case.anchor,
            "embed": embed,
            "rules": _resolve_rule_refs(case.rules, catalogue),
            "target_label": config.target_label,
            "xml_tag": case.xml_tag,
            "xml_tag_repr": repr(case.xml_tag) if config.target_label and case.xml_tag else None,
            "notes": case.notes,
            "schema_name": case.schema.name,
            "schema_rel_path": rel_posix(case.schema, REPO_ROOT),
            "schema_url": repo_blob_url(case.schema),
            "schema_block": read_code_block(case.schema, config.schema_lang),
            "runs": runs,
            "run_output_heading": config.run_output_heading,
            "expected_schema_heading": config.expected_schema_heading,
            "expected_schema_name": case.expected_schema.name,
            "expected_schema_url": repo_blob_url(case.expected_schema),
            "expected_schema_block": read_code_block(case.expected_schema, config.expected_schema_lang),
            "manifest_url": repo_blob_url(case.directory / "case.yaml"),
        }
    )


def _build_error_fixture_params(
    err: ErrorCase,
    config: ConverterSpecConfig,
) -> tuple[tuple[str, str], ...]:
    """Extract formatted ``(param_name, repr_value)`` pairs for an ``ErrorCase``.

    :param err: Loaded error manifest entry.
    :type err: ErrorCase
    :param config: Converter or pipeline specification configuration.
    :type config: ConverterSpecConfig
    :return: Tuple of ``(name, formatted_repr)`` pairs.
    :rtype: tuple[tuple[str, str], ...]
    """
    pairs: list[tuple[str, str]] = []
    if config.target_label and err.xml_tag is not None and config.target_label not in err.params:
        pairs.append((config.target_label, repr(err.xml_tag)))
    pairs.extend((key, repr(val)) for key, val in err.params.items())
    return tuple(pairs)


def _build_error_section(
    errors: Sequence[ErrorCase],
    config: ConverterSpecConfig,
) -> ErrorSectionContext:
    """Build ``ErrorSectionContext`` from ``errors`` and ``config.error_classes``.

    :param errors: Loaded error manifest entries.
    :type errors: Sequence[ErrorCase]
    :param config: Converter specification configuration.
    :type config: ConverterSpecConfig
    :return: Populated ``ErrorSectionContext``.
    :rtype: ErrorSectionContext
    """
    by_code: dict[str, list[ErrorCase]] = defaultdict(list)
    for err in errors:
        by_code[err.code].append(err)

    parent_type = "RuntimeError" if issubclass(config.error_base, RuntimeError) else "ValueError"
    rows: list[ErrorRowContext] = []
    for cls in config.error_classes:
        code = str(getattr(cls, "code", ""))
        title = str(getattr(cls, "title", ""))
        fixtures = tuple(
            ErrorFixtureRefContext(
                name=e.file.name,
                url=repo_blob_url(e.file),
                param_pairs=_build_error_fixture_params(e, config),
                description=e.description,
                code_block=read_code_block(e.file, config.schema_lang),
            )
            for e in by_code.get(code, ())
        )
        rows.append(
            ErrorRowContext.model_validate(
                {
                    "code": code,
                    "exception_name": cls.__name__,
                    "title": title,
                    "reason": cls.__doc__,
                    "fixtures": fixtures,
                }
            )
        )
    return ErrorSectionContext(
        base_class_name=config.error_base.__name__,
        parent_type_name=parent_type,
        rows=tuple(rows),
    )


def _introspect_preset_overrides(
    base_cls: type[Any],
    preset_cls: type[Any],
) -> tuple[PresetOverrideRowContext, ...]:
    """Compare ``preset_cls`` against ``base_cls`` to extract locked and overridden settings attributes.

    :param base_cls: Base pipeline settings class.
    :type base_cls: type[Any]
    :param preset_cls: Preset pipeline settings subclass.
    :type preset_cls: type[Any]
    :return: Tuple of ``PresetOverrideRowContext`` rows.
    :rtype: tuple[PresetOverrideRowContext, ...]
    """
    rows: list[PresetOverrideRowContext] = []
    base_fields = base_cls.model_fields
    preset_fields = preset_cls.model_fields

    for field_name, base_field in base_fields.items():
        if field_name not in preset_fields and hasattr(preset_cls, field_name):
            rows.append(
                PresetOverrideRowContext(
                    attr_name=field_name,
                    kind="Locked `ClassVar` (CLI flag removed)",
                    base_val=format_value_code(base_field.default),
                    preset_val=format_value_code(getattr(preset_cls, field_name)),
                )
            )
        elif field_name in preset_fields and preset_fields[field_name].default != base_field.default:
            rows.append(
                PresetOverrideRowContext(
                    attr_name=field_name,
                    kind="Overridden field default",
                    base_val=format_value_code(base_field.default),
                    preset_val=format_value_code(preset_fields[field_name].default),
                )
            )

    for extra_attr in PRESET_EXTRA_ATTRS:
        val = getattr(preset_cls, extra_attr, None)
        if val:
            base_val = getattr(base_cls, extra_attr, None)
            if val != base_val:
                rows.append(
                    PresetOverrideRowContext(
                        attr_name=extra_attr,
                        kind="Preset class binding",
                        base_val=format_value_code(base_val) if base_val else "`None`",
                        preset_val=format_value_code(val),
                    )
                )
    return tuple(rows)


def _build_hierarchy_context(spec: PipelineSpecConfig) -> SettingsHierarchyContext | None:
    """Build ``SettingsHierarchyContext`` when ``spec`` defines preset subclasses.

    :param spec: Pipeline specification configuration.
    :type spec: PipelineSpecConfig
    :return: ``SettingsHierarchyContext`` or ``None`` when ``spec.presets`` is empty.
    :rtype: SettingsHierarchyContext | None
    """
    if not spec.presets:
        return None

    base_resolved = resolve_pipeline(spec.name)
    base_cls = base_resolved.settings_cls
    if base_cls is None:
        msg = f"Pipeline {spec.name!r} has no settings_cls"
        raise ValueError(msg)

    presets: list[PresetDetailContext] = []

    for preset_name in spec.presets:
        preset_resolved = resolve_pipeline(preset_name)
        preset_cls = preset_resolved.settings_cls
        if preset_cls is None or preset_resolved.pipeline_fn is None:
            msg = f"Preset pipeline {preset_name!r} has no settings_cls or pipeline_fn"
            raise ValueError(msg)

        shared_runner = f"{preset_resolved.pipeline_fn.__module__}.{preset_resolved.pipeline_fn.__name__}"
        presets.append(
            PresetDetailContext.model_validate(
                {
                    "cli_command": preset_resolved.cli_command,
                    "class_name": preset_cls.__name__,
                    "settings_api_url": preset_cls,
                    "base_class_name": base_cls.__name__,
                    "shared_runner": shared_runner,
                    "shared_runner_url": preset_resolved.pipeline_fn.__module__,
                    "overrides": _introspect_preset_overrides(base_cls, preset_cls),
                }
            )
        )

    return SettingsHierarchyContext.model_validate(
        {
            "base_cli_command": base_resolved.cli_command,
            "base_class_name": base_cls.__name__,
            "base_settings_url": base_cls.__module__,
            "runner_name": base_resolved.pipeline_fn.__name__ if base_resolved.pipeline_fn else "",
            "runner_url": base_resolved.pipeline_fn.__module__ if base_resolved.pipeline_fn else None,
            "presets": tuple(presets),
        }
    )


def _optional_file_preview(path: Path | None, lang: str | None = None) -> tuple[str | None, CodeBlockContext | None]:
    """Return ``(repo_rel, CodeBlockContext)`` for ``path`` when it exists as a file, else ``(None, None)``.

    :param path: Optional file path to preview.
    :type path: Path | None
    :param lang: Optional explicit Pygments language override.
    :type lang: str | None
    :return: Pair of repository-relative path and code block context, or ``(None, None)``.
    :rtype: tuple[str | None, CodeBlockContext | None]
    """
    if path is None or not path.is_file():
        return (None, None)
    return (rel_posix(path), read_code_block(path, lang))


def _build_pipeline_run_context(case: PipelineCase, run: PipelineRun) -> PipelineRunContext:
    """Build ``PipelineRunContext`` for a single ``PipelineRun``.

    :param case: Enclosing pipeline case manifest.
    :type case: PipelineCase
    :param run: Pipeline run manifest entry.
    :type run: PipelineRun
    :return: Populated ``PipelineRunContext``.
    :rtype: PipelineRunContext
    """
    settings_rel, settings_block = _optional_file_preview(run.settings_file)

    input_files = (
        [run.input_dir] if run.input_dir.is_file() else sorted(p for p in run.input_dir.rglob("*") if p.is_file())
    )
    inputs = tuple(
        PipelineInputPreviewContext(
            rel_in=input_file.name if run.input_dir.is_file() else input_file.relative_to(run.input_dir).as_posix(),
            repo_rel=rel_posix(input_file),
            decompressed=input_file.name.endswith(".gz"),
            code_block=read_code_block(input_file),
        )
        for input_file in input_files
    )

    report_preview = {
        "status": run.expected_report.status,
        "ingested_files": list(run.expected_report.ingested_files),
        "load_errors": list(run.expected_report.load_errors),
        "pipeline_load_info_errs": run.expected_report.pipeline_load_info_errs,
    }
    report_yaml_text = yaml.safe_dump(report_preview, sort_keys=False).rstrip()
    report_yaml_block = CodeBlockContext(
        content=report_yaml_text,
        lang="yaml",
    )

    expected_tables_rel, expected_tables_block = _optional_file_preview(run.expected_tables, "python")
    expected_json_rel, expected_json_block = _optional_file_preview(run.expected_json, "json")
    return PipelineRunContext(
        id=run.id,
        show_heading=len(case.runs) > 1,
        settings_rel=settings_rel,
        settings_block=settings_block,
        inputs=inputs,
        expected_counts_repr=repr(run.expected_counts),
        report_yaml_block=report_yaml_block,
        expected_tables_rel=expected_tables_rel,
        tables_attr=run.tables_attr,
        expected_tables_block=expected_tables_block,
        expected_json_rel=expected_json_rel,
        expected_json_block=expected_json_block,
    )


def _build_pipeline_case_context(case: PipelineCase, catalogue: RuleCatalogue) -> PipelineCaseContext:
    """Build ``PipelineCaseContext`` for a ``PipelineCase`` manifest entry.

    :param case: Pipeline case manifest.
    :type case: PipelineCase
    :param catalogue: Parsed rule catalogue.
    :type catalogue: RuleCatalogue
    :return: Populated ``PipelineCaseContext``.
    :rtype: PipelineCaseContext
    """
    resolved = resolve_pipeline(case.pipeline)
    cls_name = resolved.settings_cls.__name__ if resolved.settings_cls else ""
    return PipelineCaseContext.model_validate(
        {
            "suite": case.suite,
            "name": case.name,
            "title": case.title,
            "anchor": case.anchor,
            "pipeline": case.pipeline,
            "settings_class_name": cls_name,
            "directory_rel": rel_posix(case.directory),
            "rules": _resolve_rule_refs(case.rules, catalogue),
            "notes": case.notes,
            "runs": tuple(_build_pipeline_run_context(case, run) for run in case.runs),
            "run_count_entries": tuple((r.id, repr(r.expected_counts)) for r in case.runs),
            "manifest_url": repo_blob_url(case.directory / "case.yaml"),
        }
    )


def resolve_converter_config(
    name_or_config: str | ConverterSpecConfig,
    *,
    specs_root: Path = CONVERTER_SPECS_ROOT,
    docs_dir: Path = DOCS_DIR,
) -> ConverterSpecConfig:
    """Resolve a converter name or ``ConverterSpecConfig`` instance, discovering newly added suites dynamically.

    :param name_or_config: Converter spec key or a ``ConverterSpecConfig``.
    :type name_or_config: str | ConverterSpecConfig
    :param specs_root: Root directory containing converter specification directories.
    :type specs_root: Path
    :param docs_dir: Output ``docs/`` directory for ``<name>_specification.md``.
    :type docs_dir: Path
    :return: Resolved ``ConverterSpecConfig``.
    :rtype: ConverterSpecConfig
    """
    if isinstance(name_or_config, ConverterSpecConfig):
        return name_or_config
    if specs_root == CONVERTER_SPECS_ROOT and docs_dir == DOCS_DIR and name_or_config in CONVERTER_SPECS:
        return CONVERTER_SPECS[name_or_config]
    discovered = discover_converter_specs(specs_root=specs_root, docs_dir=docs_dir)
    if name_or_config not in discovered:
        msg = f"Unknown converter specification: {name_or_config!r}"
        raise KeyError(msg)
    return discovered[name_or_config]


def resolve_pipeline_config(name_or_config: str | PipelineSpecConfig) -> PipelineSpecConfig:
    """Resolve a pipeline family name or ``PipelineSpecConfig`` instance.

    :param name_or_config: Pipeline spec key in ``PIPELINE_SPECS`` or a ``PipelineSpecConfig``.
    :type name_or_config: str | PipelineSpecConfig
    :return: Resolved ``PipelineSpecConfig``.
    :rtype: PipelineSpecConfig
    """
    if isinstance(name_or_config, PipelineSpecConfig):
        return name_or_config
    if name_or_config not in PIPELINE_SPECS:
        msg = f"Unknown pipeline specification: {name_or_config!r}"
        raise KeyError(msg)
    return PIPELINE_SPECS[name_or_config]


def build_converter_spec_context(name_or_config: str | ConverterSpecConfig) -> ConverterSpecPageContext:
    """Build the ``ConverterSpecPageContext`` view model for ``pages/converter_spec.md.j2``.

    :param name_or_config: Converter spec key or ``ConverterSpecConfig`` instance.
    :type name_or_config: str | ConverterSpecConfig
    :return: Populated ``ConverterSpecPageContext``.
    :rtype: ConverterSpecPageContext
    """
    config = resolve_converter_config(name_or_config)
    catalogue = load_rules(config.data_dir / "rules.yaml")
    cases = [case for suite in config.suites for case in load_cases(config.data_dir / suite)]
    errors = load_error_cases(config.error_manifest)

    covering: dict[str, list[CaseLinkContext]] = {rule_id: [] for rule_id in catalogue.rules}
    for case in cases:
        for rule_id in case.rules:
            covering.setdefault(rule_id, []).append(
                CaseLinkContext(label=case.name, anchor=case.anchor, code_wrap=False)
            )

    sections = _build_rule_sections(
        catalogue,
        covering,
        config.generated_tables,
        empty_cases_fallback="",
    )
    case_contexts = tuple(_build_converter_case_context(case, catalogue, config) for case in cases)
    error_section = _build_error_section(errors, config)
    facade_module = config.facade_module or f"{CONVERTER_FACADES_PACKAGE}.{config.name}"
    return ConverterSpecPageContext.model_validate(
        {
            "title": catalogue.title,
            "intro": catalogue.intro,
            "spec_dir_rel": rel_posix(config.data_dir),
            "spec_dir_url": repo_blob_url(config.data_dir),
            "facade_module": facade_module,
            "facade_url": facade_module,
            "target_label": config.target_label,
            "sections": sections,
            "cases": case_contexts,
            "error_section": error_section,
        }
    )


def build_pipeline_spec_context(name_or_config: str | PipelineSpecConfig) -> PipelineSpecPageContext:
    """Build the ``PipelineSpecPageContext`` view model for ``pages/pipeline_spec.md.j2``.

    :param name_or_config: Pipeline spec key or ``PipelineSpecConfig`` instance.
    :type name_or_config: str | PipelineSpecConfig
    :return: Populated ``PipelineSpecPageContext``.
    :rtype: PipelineSpecPageContext
    """
    spec = resolve_pipeline_config(name_or_config)
    raw_rules = yaml.safe_load((spec.data_dir / "rules.yaml").read_text(encoding="utf-8"))
    catalogue = load_rules(spec.data_dir / "rules.yaml")
    cases = load_pipeline_cases(spec.data_dir)
    conv_cfg = spec.converter_config or PIPELINE_SPECS.get(spec.name, spec).converter_config

    covering: dict[str, list[CaseLinkContext]] = defaultdict(list)
    for case in cases:
        for rid in case.rules:
            covering[rid].append(CaseLinkContext(label=f"{case.suite}/{case.name}", anchor=case.anchor, code_wrap=True))

    table_key_map = (
        {**DEFAULT_PIPELINE_TABLES, **conv_cfg.generated_tables}
        if conv_cfg is not None
        else dict(DEFAULT_PIPELINE_TABLES)
    )
    sections = _build_rule_sections(
        catalogue,
        covering,
        table_key_map,
        empty_cases_fallback="—",
    )

    shared_files = tuple(
        SharedFileContext(
            repo_rel=rel_posix(shared_file),
            url=repo_blob_url(shared_file),
            code_block=read_code_block(shared_file, "python"),
        )
        for shared_file in spec.shared_files
        if shared_file.is_file()
    )

    all_case_contexts = tuple(_build_pipeline_case_context(c, catalogue) for c in cases)
    use_conv_base = conv_cfg is not None and (spec.data_dir / "spec").is_dir()
    conv_base_cases = (
        tuple(_build_converter_case_context(c, catalogue, conv_cfg) for c in load_cases(spec.data_dir / "spec"))
        if use_conv_base and conv_cfg is not None
        else ()
    )
    smoke_base_cases = (
        () if use_conv_base else tuple(_build_pipeline_case_context(c, catalogue) for c in cases if not c.is_preset)
    )
    preset_cases = tuple(_build_pipeline_case_context(c, catalogue) for c in cases if c.is_preset)

    error_section: ErrorSectionContext | None = None
    if conv_cfg is not None and conv_cfg.error_manifest.is_file():
        errors = load_error_cases(conv_cfg.error_manifest)
        error_section = _build_error_section(errors, conv_cfg)

    settings_class = str(raw_rules.get("settings_class", ""))
    pipeline_runner = str(raw_rules.get("pipeline_runner", ""))

    return PipelineSpecPageContext.model_validate(
        {
            "name": spec.name,
            "title": catalogue.title,
            "intro": catalogue.intro,
            "cli_command": str(raw_rules.get("cli_command", spec.name)),
            "settings_class": settings_class,
            "settings_class_url": settings_class,
            "pipeline_runner": pipeline_runner,
            "pipeline_runner_url": pipeline_runner,
            "spec_dir_rel": rel_posix(spec.data_dir),
            "spec_dir_url": repo_blob_url(spec.data_dir),
            "presets": spec.presets,
            "hierarchy": _build_hierarchy_context(spec),
            "sections": sections,
            "all_cases": all_case_contexts,
            "shared_files": shared_files,
            "use_converter_base_cases": use_conv_base,
            "converter_base_cases": conv_base_cases,
            "smoke_base_cases": smoke_base_cases,
            "preset_cases": preset_cases,
            "error_section": error_section,
        }
    )
