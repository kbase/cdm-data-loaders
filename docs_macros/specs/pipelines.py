"""Pipeline family case, run, and page context builders."""

from collections import defaultdict
from pathlib import Path

import yaml

from cdm_data_loaders.core.pipeline_catalog import resolve_pipeline
from docs_macros.formatting import CodeBlockContext, read_code_block, rel_posix, repo_blob_url
from docs_macros.specs.converters import (
    build_converter_case_context,
    build_error_section,
    build_rule_sections,
    resolve_rule_refs,
)
from docs_macros.specs.discovery import (
    DEFAULT_PIPELINE_TABLES,
    discover_pipeline_specs,
    resolve_pipeline_config,
)
from docs_macros.specs.hierarchy import build_hierarchy_context
from docs_macros.specs.models import (
    CaseLinkContext,
    ErrorSectionContext,
    PipelineCaseContext,
    PipelineInputPreviewContext,
    PipelineRunContext,
    PipelineSpecConfig,
    PipelineSpecPageContext,
    SharedFileContext,
)
from specs._loader.converter_spec import RuleCatalogue, load_cases, load_error_cases, load_rules
from specs._loader.pipeline_spec import PipelineCase, PipelineRun, load_pipeline_cases


def _optional_file_preview(path: Path | None, lang: str | None = None) -> tuple[str | None, CodeBlockContext | None]:
    """Return ``(repo_rel, CodeBlockContext)`` for ``path`` when it exists as a file, else ``(None, None)``.

    :param path: Optional file path to preview.
    :type path: Path | None
    :param lang: Optional Pygments language override.
    :type lang: str | None
    :return: Pair of repository-relative path and code block context, or ``(None, None)``.
    :rtype: tuple[str | None, CodeBlockContext | None]
    """
    if path is None or not path.is_file():
        return (None, None)
    return (rel_posix(path), read_code_block(path, lang))


def build_pipeline_run_context(case: PipelineCase, run: PipelineRun) -> PipelineRunContext:
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


def build_pipeline_case_context(case: PipelineCase, catalogue: RuleCatalogue) -> PipelineCaseContext:
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
            "rules": resolve_rule_refs(case.rules, catalogue),
            "notes": case.notes,
            "runs": tuple(build_pipeline_run_context(case, run) for run in case.runs),
            "run_count_entries": tuple((r.id, repr(r.expected_counts)) for r in case.runs),
            "manifest_url": repo_blob_url(case.directory / "case.yaml"),
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
    catalogue = load_rules(spec.data_dir / "rules.yaml")
    raw_meta = catalogue.metadata
    cases = load_pipeline_cases(spec.data_dir)
    default_specs = discover_pipeline_specs()
    conv_cfg = spec.converter_config or (
        default_specs[spec.name].converter_config if spec.name in default_specs else None
    )

    covering: dict[str, list[CaseLinkContext]] = defaultdict(list)
    for case in cases:
        for rid in case.rules:
            covering[rid].append(CaseLinkContext(label=f"{case.suite}/{case.name}", anchor=case.anchor, code_wrap=True))

    table_key_map = (
        {**DEFAULT_PIPELINE_TABLES, **conv_cfg.generated_tables}
        if conv_cfg is not None
        else dict(DEFAULT_PIPELINE_TABLES)
    )
    sections = build_rule_sections(
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

    all_case_contexts = tuple(build_pipeline_case_context(c, catalogue) for c in cases)
    use_conv_base = conv_cfg is not None and (spec.data_dir / "spec").is_dir()
    conv_base_cases = (
        tuple(build_converter_case_context(c, catalogue, conv_cfg) for c in load_cases(spec.data_dir / "spec"))
        if use_conv_base and conv_cfg is not None
        else ()
    )
    smoke_base_cases = (
        () if use_conv_base else tuple(build_pipeline_case_context(c, catalogue) for c in cases if not c.is_preset)
    )
    preset_cases = tuple(build_pipeline_case_context(c, catalogue) for c in cases if c.is_preset)

    error_section: ErrorSectionContext | None = None
    if conv_cfg is not None and conv_cfg.error_manifest.is_file():
        errors = load_error_cases(conv_cfg.error_manifest)
        error_section = build_error_section(errors, conv_cfg)

    settings_class = str(raw_meta.get("settings_class", ""))
    pipeline_runner = str(raw_meta.get("pipeline_runner", ""))

    return PipelineSpecPageContext.model_validate(
        {
            "name": spec.name,
            "title": catalogue.title,
            "intro": catalogue.intro,
            "cli_command": str(raw_meta.get("cli_command", spec.name)),
            "settings_class": settings_class,
            "settings_class_url": settings_class,
            "pipeline_runner": pipeline_runner,
            "pipeline_runner_url": pipeline_runner,
            "spec_dir_rel": rel_posix(spec.data_dir),
            "spec_dir_url": repo_blob_url(spec.data_dir),
            "presets": spec.presets,
            "hierarchy": build_hierarchy_context(spec),
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
