"""Converter rule, case, error-catalog, and page context builders."""

from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path

from docs_macros.constants import REPO_ROOT
from docs_macros.formatting import read_code_block, rel_posix, repo_blob_url
from docs_macros.specs.discovery import CONVERTER_FACADES_PACKAGE, resolve_converter_config
from docs_macros.specs.models import (
    CaseLinkContext,
    ConverterCaseContext,
    ConverterExtraOutputContext,
    ConverterLinkedFileContext,
    ConverterRunContext,
    ConverterRunInputContext,
    ConverterSpecConfig,
    ConverterSpecPageContext,
    ErrorFixtureRefContext,
    ErrorRowContext,
    ErrorSectionContext,
    RuleItemContext,
    RuleRefContext,
    RuleSectionContext,
)
from docs_macros.tables.registry import build_table_context
from specs._loader.converter_spec import (
    Case,
    ErrorCase,
    RuleCatalogue,
    Run,
    load_cases,
    load_error_cases,
    load_rules,
)


def resolve_rule_refs(rule_ids: Sequence[str], catalogue: RuleCatalogue) -> tuple[RuleRefContext, ...]:
    """Filter ``rule_ids`` against ``catalogue`` into ``RuleRefContext`` items.

    :param rule_ids: Candidate rule identifier strings.
    :type rule_ids: Sequence[str]
    :param catalogue: Parsed rule catalogue.
    :type catalogue: RuleCatalogue
    :return: Tuple of resolved ``RuleRefContext`` objects.
    :rtype: tuple[RuleRefContext, ...]
    """
    return tuple(RuleRefContext(id=rid) for rid in rule_ids if rid in catalogue.rules)


def build_rule_sections(
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


def build_converter_run_context(
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
    :param seen_data: Mutable set tracking rendered input data paths across runs in a case.
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


def build_converter_case_context(
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
    runs = tuple(build_converter_run_context(run, case, config, embed=embed, seen_data=seen_data) for run in case.runs)
    return ConverterCaseContext.model_validate(
        {
            "suite": case.suite,
            "name": case.name,
            "title": case.title,
            "anchor": case.anchor,
            "embed": embed,
            "rules": resolve_rule_refs(case.rules, catalogue),
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


def build_error_fixture_params(
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


def build_error_section(
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
                param_pairs=build_error_fixture_params(e, config),
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

    sections = build_rule_sections(
        catalogue,
        covering,
        config.generated_tables,
        empty_cases_fallback="",
    )
    case_contexts = tuple(build_converter_case_context(case, catalogue, config) for case in cases)
    error_section = build_error_section(errors, config)
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
