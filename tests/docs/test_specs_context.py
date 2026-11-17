"""Tests for ``docs_macros.specs`` discovery, converter/pipeline page sections, and error templates."""

from pathlib import Path

import pytest

from docs_macros.formatting import CodeBlockContext
from docs_macros.render import create_jinja_env, render_converter_spec, render_pipeline_spec
from docs_macros.specs.converters import build_converter_spec_context
from docs_macros.specs.discovery import discover_converter_specs, discover_pipeline_specs
from docs_macros.specs.models import (
    ErrorFixtureRefContext,
    ErrorRowContext,
    ErrorSectionContext,
)
from docs_macros.specs.pipelines import build_pipeline_spec_context

pytestmark = pytest.mark.docs


@pytest.mark.parametrize(
    ("case_kind", "expected_keys"),
    [
        pytest.param(
            "repo",
            {
                "xsd_to_pyarrow",
                "jsonschema_to_pyarrow",
                "pydantic_to_pyarrow",
                "pyarrow_to_jsonschema",
                "pyarrow_to_linkml",
            },
            id="repo-specs",
        ),
        pytest.param("empty", set(), id="empty-dir"),
        pytest.param("synthetic", {"pydantic_to_pyarrow"}, id="synthetic-valid-spec"),
    ],
)
def test_discover_converter_specs_pass(
    tmp_path: Path,
    case_kind: str,
    expected_keys: set[str],
) -> None:
    """Verify discover_converter_specs discovers repo suites, empty roots, and synthetic rules.yaml defaults."""
    specs_root = tmp_path / "specs"
    docs_dir = tmp_path / "docs"
    if case_kind == "repo":
        discovered = discover_converter_specs()
    else:
        if case_kind == "synthetic":
            conv_dir = specs_root / "pydantic_to_pyarrow"
            (conv_dir / "spec").mkdir(parents=True)
            (conv_dir / "rules.yaml").write_text(
                "title: Synthetic\nsections:\n  - id: s1\n    title: S1\n    generated: scalar_table\n    rules: []\n",
                encoding="utf-8",
            )
        discovered = discover_converter_specs(specs_root=specs_root, docs_dir=docs_dir)

    assert set(discovered) == expected_keys
    if case_kind == "synthetic":
        cfg = discovered["pydantic_to_pyarrow"]
        assert cfg.output == docs_dir / "pydantic_to_pyarrow_specification.md"
        assert cfg.suites == ("spec",)
        assert cfg.schema_lang == "python"
        assert cfg.data_lang == "json"
        assert cfg.generated_tables == {"scalar_table": "pydantic_scalar_table"}


@pytest.mark.parametrize(
    ("spec_dir_name", "rules_yaml", "expected_match"),
    [
        pytest.param(
            "unknown_to_pyarrow",
            "title: Bad Source\nsections: []\n",
            "Cannot infer schema_lang/data_lang",
            id="missing-from-to",
        ),
        pytest.param(
            "pydantic_to_unknown",
            "title: Bad Target\nsections: []\n",
            "Cannot infer target rendering defaults",
            id="unknown-format-token",
        ),
        pytest.param(
            "jsonschema_to_dlt",
            "title: Missing Errors Tuple\nsections: []\n",
            "JSONSCHEMA_TO_DLT_ERRORS",
            id="missing-error-tuple",
        ),
    ],
)
def test_discover_converter_specs_fail_invalid_rules(
    tmp_path: Path,
    spec_dir_name: str,
    rules_yaml: str,
    expected_match: str,
) -> None:
    """Verify discover_converter_specs rejects uninferrable source/target formats and missing error tuples."""
    specs_root = tmp_path / "specs"
    docs_dir = tmp_path / "docs"
    bad_dir = specs_root / spec_dir_name
    bad_dir.mkdir(parents=True)
    (bad_dir / "rules.yaml").write_text(rules_yaml, encoding="utf-8")

    with pytest.raises(ValueError, match=expected_match):
        discover_converter_specs(specs_root=specs_root, docs_dir=docs_dir)


@pytest.mark.parametrize(
    ("use_repo_defaults", "expected_keys"),
    [
        pytest.param(
            True,
            {
                "xmltopyarrow",
                "jsonlines_jsonschema",
                "xsv",
                "jsonlines_extract",
                "jsonlines_pydantic",
                "xmltodict",
            },
            id="repo-specs",
        ),
        pytest.param(False, set(), id="empty-dir"),
    ],
)
def test_discover_pipeline_specs_pass(
    tmp_path: Path,
    use_repo_defaults: bool,
    expected_keys: set[str],
) -> None:
    """Verify discover_pipeline_specs discovers repo pipeline suites and returns an empty mapping when absent."""
    if use_repo_defaults:
        discovered = discover_pipeline_specs()
    else:
        discovered = discover_pipeline_specs(specs_root=tmp_path / "specs", docs_dir=tmp_path / "docs")
    assert set(discovered) == expected_keys


@pytest.mark.parametrize("spec_name", sorted(discover_converter_specs()), ids=str)
def test_render_converter_spec_splits_spec_and_examples_with_schema_blocks(spec_name: str) -> None:
    """Converter specs split cases into Spec and Examples sections and embed schemas as code blocks in both."""
    cfg = discover_converter_specs()[spec_name]
    rendered = render_converter_spec(cfg)

    assert "## Cases" not in rendered
    assert "[Spec](#spec)" in rendered
    assert "[Examples](#examples)" in rendered
    assert "## Spec\n" in rendered
    assert "## Examples\n" in rendered

    before_spec, after_spec = rendered.split("## Spec\n", maxsplit=1)
    assert "## Fixture Index\n" in before_spec
    spec_cases_section, examples_section = after_spec.split("## Examples\n", maxsplit=1)
    assert spec_cases_section

    assert "#### Schema (`" in examples_section
    assert f"#### {cfg.expected_schema_heading} (" in examples_section
    assert "**Schema:**" not in examples_section
    assert f"**Expected {cfg.expected_schema_heading}:**" not in examples_section
    assert "```" in examples_section


@pytest.mark.parametrize(
    ("kind", "spec_name"),
    [
        *(pytest.param("converter", name, id=f"converter-{name}") for name in sorted(discover_converter_specs())),
        pytest.param("pipeline", "xsv", id="pipeline-xsv"),
        pytest.param("pipeline", "xmltopyarrow", id="pipeline-xmltopyarrow"),
        pytest.param("pipeline", "jsonlines_jsonschema", id="pipeline-jsonlines_jsonschema"),
    ],
)
def test_render_error_section_rule_style_and_expandable_fixtures_pass(kind: str, spec_name: str) -> None:
    """Verify error sections render rule-style items with condition, reason, and expandable fixture codeblocks."""
    if kind == "converter":
        ctx_error = build_converter_spec_context(spec_name).error_section
        rendered = render_converter_spec(spec_name)
    else:
        ctx_error = build_pipeline_spec_context(spec_name).error_section
        assert ctx_error is not None
        rendered = render_pipeline_spec(spec_name)

    assert "| Code | Exception | Condition | Reason | Fixtures |" not in rendered
    assert "## Errors (Unsupported / Invalid Input)" in rendered

    for row in ctx_error.rows:
        assert f'<a id="{row.anchor}"></a>' in rendered
        assert f"- **{row.code}** (`{row.exception_name}`)" in rendered
        assert f"*Condition:* {row.title}" in rendered
        assert f"*Reason:* {row.reason}" in rendered
        assert "    *Fixtures:*" in rendered
        for fix in row.fixtures:
            assert f"    - [`{fix.name}`]({fix.url})" in rendered
            assert fix.description in rendered
            assert f'\n\n        ??? abstract "{fix.name}"\n\n            ' in rendered
            first_code_line = fix.code_block.content.splitlines()[0]
            assert f"            {first_code_line}" in rendered
            for param_key, param_repr in fix.param_pairs:
                assert f"`{param_key}={param_repr}`" in rendered


def test_render_error_section_boundary_cases_pass() -> None:
    """Verify partials/errors_section.md.j2 handles empty fixture lists and empty fixture descriptions."""
    template = create_jinja_env().get_template("partials/errors_section.md.j2")
    section = ErrorSectionContext(
        base_class_name="SampleConversionError",
        parent_type_name="ValueError",
        rows=(
            ErrorRowContext(
                code="E1",
                anchor="e1",
                exception_name="NoFixturesError",
                title="Missing fixture case",
                reason="Used to test empty fixture fallback.",
                fixtures=(),
            ),
            ErrorRowContext(
                code="E2",
                anchor="e2",
                exception_name="BareFixtureError",
                title="Fixture without description",
                reason="Used to test empty description and multiple param pairs.",
                fixtures=(
                    ErrorFixtureRefContext(
                        name="bad.xml",
                        url="https://example.org/bad.xml",
                        param_pairs=(("xml_tag", "'entry'"), ("table_name", "'entries'")),
                        description="",
                        code_block=CodeBlockContext(content="<root/>", lang="xml", fence="```"),
                    ),
                ),
            ),
        ),
    )
    rendered = template.render(error_section=section)
    assert "    *Fixtures:* —" in rendered
    assert (
        "    - [`bad.xml`](https://example.org/bad.xml) (`xml_tag='entry'`, `table_name='entries'`)\n\n"
        '        ??? abstract "bad.xml"\n\n'
        "            ```xml\n"
        "            <root/>\n"
        "            ```"
    ) in rendered
