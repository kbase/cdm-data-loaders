"""Configuration and Jinja2 view models for converter and pipeline specification pages."""

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Annotated

from pydantic import BeforeValidator, computed_field, model_validator

from docs_macros.formatting import (
    CodeBlockContext,
    FrozenDocModel,
    ModuleApiUrl,
    ModuleBlobUrl,
    NonNullStr,
    NormalizedProse,
)
from docs_macros.tables.models import SimpleTableRow, TableRenderContext


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
    """Configuration describing a pipeline family specification page."""

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
        """Populate ``anchor`` from ``code.lower()`` when omitted or empty.

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
