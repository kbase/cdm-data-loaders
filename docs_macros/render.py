"""Jinja2 environment factory and Markdown page/table renderers for ``docs_macros``."""

from collections.abc import Sequence
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape

from docs_macros.cli_context import CliEntryDoc, build_cli_reference_context
from docs_macros.constants import REPO_ROOT, SRC_ROOT, TEMPLATES_DIR
from docs_macros.formatting import code_fence_for_text, escape_table_cell
from docs_macros.schemas_context import SchemaAssetDoc, build_bundled_schemas_context
from docs_macros.specs.converters import build_converter_spec_context
from docs_macros.specs.models import ConverterSpecConfig, PipelineSpecConfig
from docs_macros.specs.pipelines import build_pipeline_spec_context
from docs_macros.tables.registry import build_table_context


def create_jinja_env(templates_dir: Path = TEMPLATES_DIR) -> Environment:
    """Create the strict Jinja2 environment configured for Markdown documentation templates.

    :param templates_dir: Root directory containing ``.md.j2`` templates.
    :type templates_dir: Path
    :return: Configured ``jinja2.Environment`` instance.
    :rtype: Environment
    """
    env = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
        autoescape=select_autoescape(enabled_extensions=(), default_for_string=False, default=False),
    )
    env.filters["escape_cell"] = escape_table_cell
    env.filters["code_fence"] = code_fence_for_text
    return env


def _render_page_template(
    template_name: str,
    *,
    templates_dir: Path = TEMPLATES_DIR,
    **context: object,
) -> str:
    """Render a Jinja2 page template with ``context`` and normalize its trailing newline.

    :param template_name: Template path relative to ``templates_dir``.
    :type template_name: str
    :param templates_dir: Root directory containing ``.md.j2`` templates.
    :type templates_dir: Path
    :param context: Template context keyword arguments.
    :type context: object
    :return: Rendered Markdown document string ending with a single newline.
    :rtype: str
    """
    env = create_jinja_env(templates_dir)
    rendered = env.get_template(template_name).render(**context)
    return rendered.rstrip() + "\n"


def render_cli_reference(
    entries: Sequence[CliEntryDoc] | None = None,
    *,
    templates_dir: Path = TEMPLATES_DIR,
) -> str:
    """Render the CLI reference Markdown document from ``pages/cli_reference.md.j2``.

    :param entries: Optional sequence of pre-introspected CLI entry descriptors.
    :type entries: Sequence[CliEntryDoc] | None
    :param templates_dir: Root directory containing ``.md.j2`` templates.
    :type templates_dir: Path
    :return: Rendered Markdown document string ending with a single newline.
    :rtype: str
    """
    return _render_page_template(
        "pages/cli_reference.md.j2",
        templates_dir=templates_dir,
        page=build_cli_reference_context(entries),
    )


def render_bundled_schemas(
    assets: Sequence[SchemaAssetDoc] | None = None,
    *,
    src_root: Path = SRC_ROOT,
    repo_root: Path = REPO_ROOT,
    templates_dir: Path = TEMPLATES_DIR,
) -> str:
    """Render the bundled schemas catalog and inline collapsible sources from ``pages/bundled_schemas.md.j2``.

    :param assets: Optional sequence of pre-discovered schema assets.
    :type assets: Sequence[SchemaAssetDoc] | None
    :param src_root: Package source root directory.
    :type src_root: Path
    :param repo_root: Repository root directory.
    :type repo_root: Path
    :param templates_dir: Root directory containing ``.md.j2`` templates.
    :type templates_dir: Path
    :return: Rendered Markdown document string ending with a single newline.
    :rtype: str
    """
    return _render_page_template(
        "pages/bundled_schemas.md.j2",
        templates_dir=templates_dir,
        page=build_bundled_schemas_context(assets, src_root=src_root, repo_root=repo_root),
    )


def render_converter_spec(
    name_or_config: str | ConverterSpecConfig,
    *,
    templates_dir: Path = TEMPLATES_DIR,
) -> str:
    """Render a schema converter specification document from ``pages/converter_spec.md.j2``.

    :param name_or_config: Converter spec key or ``ConverterSpecConfig`` instance.
    :type name_or_config: str | ConverterSpecConfig
    :param templates_dir: Root directory containing ``.md.j2`` templates.
    :type templates_dir: Path
    :return: Rendered Markdown document string ending with a single newline.
    :rtype: str
    """
    return _render_page_template(
        "pages/converter_spec.md.j2",
        templates_dir=templates_dir,
        spec=build_converter_spec_context(name_or_config),
    )


def render_pipeline_spec(
    name_or_config: str | PipelineSpecConfig,
    *,
    templates_dir: Path = TEMPLATES_DIR,
) -> str:
    """Render a pipeline family specification document from ``pages/pipeline_spec.md.j2``.

    :param name_or_config: Pipeline spec key or ``PipelineSpecConfig`` instance.
    :type name_or_config: str | PipelineSpecConfig
    :param templates_dir: Root directory containing ``.md.j2`` templates.
    :type templates_dir: Path
    :return: Rendered Markdown document string ending with a single newline.
    :rtype: str
    """
    return _render_page_template(
        "pages/pipeline_spec.md.j2",
        templates_dir=templates_dir,
        spec=build_pipeline_spec_context(name_or_config),
    )


def render_reference_table(
    table_id: str,
    *,
    templates_dir: Path = TEMPLATES_DIR,
) -> str:
    """Render an individual reference table template from ``TABLE_TEMPLATE_REGISTRY``.

    :param table_id: Canonical reference table identifier (e.g. ``"xsv_config_table"``).
    :type table_id: str
    :param templates_dir: Root directory containing ``.md.j2`` templates.
    :type templates_dir: Path
    :return: Rendered Markdown table string without trailing blank lines.
    :rtype: str
    """
    env = create_jinja_env(templates_dir)
    table_ctx = build_table_context(table_id)
    rendered = env.get_template(table_ctx.template).render(
        headers=table_ctx.headers,
        rows=table_ctx.rows,
    )
    return rendered.rstrip()


def render_macro_stub(
    macro_call: str,
    *,
    templates_dir: Path = TEMPLATES_DIR,
) -> str:
    """Render a ``render_macros: true`` Markdown stub file from ``pages/macro_stub.md.j2``.

    :param macro_call: Jinja macro invocation expression (e.g. ``'render_converter_spec("xsd_to_pyarrow")'``).
    :type macro_call: str
    :param templates_dir: Root directory containing ``.md.j2`` templates.
    :type templates_dir: Path
    :return: Rendered 4-line Markdown stub ending with a single newline.
    :rtype: str
    """
    return _render_page_template(
        "pages/macro_stub.md.j2",
        templates_dir=templates_dir,
        macro_call=macro_call,
    )
