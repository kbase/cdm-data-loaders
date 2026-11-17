"""Shared Zensical macro rendering and corpus materialization helpers for documentation tests."""

import functools
import re
import tomllib
from collections.abc import Sequence
from functools import partial
from pathlib import Path
from typing import Final

import jinja2
import yaml
from _pytest.mark.expression import Expression
from zensical.extensions.macros import MacroEnv, _load_module

from docs_macros.constants import DOCS_DIR, REPO_ROOT, TEMPLATES_DIR
from docs_macros.sync import build_generated_docs_plan

ZENSICAL_PATH: Final[Path] = REPO_ROOT / "zensical.toml"
INDEX_PATH: Final[Path] = DOCS_DIR / "index.md"
README_PATH: Final[Path] = REPO_ROOT / "README.md"
FRONT_MATTER_RE: Final[re.Pattern[str]] = re.compile(r"\A---\n(.*?)\n---\n?(.*)\Z", re.DOTALL)


def _matches_docs_marker(name: str, **_kwargs: str | int | bool | None) -> bool:
    """Evaluate a marker identifier as ``True`` iff it equals ``docs``.

    :param name: Marker identifier passed by ``Expression.evaluate``.
    :type name: str
    :param _kwargs: Optional marker keyword arguments passed by ``Expression.evaluate``.
    :type _kwargs: str | int | bool | None
    :return: ``True`` when ``name == "docs"``.
    :rtype: bool
    """
    return name == "docs"


def should_ignore_docs_collect(markexpr: str) -> bool | None:
    """Return ``True`` when ``markexpr`` excludes ``docs``-marked tests, else ``None``.

    :param markexpr: Raw ``-m`` marker expression string from pytest config.
    :type markexpr: str
    :return: ``True`` to skip ``tests/docs`` collection, or ``None`` to collect.
    :rtype: bool | None
    """
    stripped = markexpr.strip()
    if not stripped:
        return None
    try:
        compiled = Expression.compile(stripped)
    except SyntaxError:
        return None
    if not compiled.evaluate(partial(_matches_docs_marker)):
        return True
    return None


def excluded_doc_names(zensical_path: Path = ZENSICAL_PATH) -> set[str]:
    """Return the set of excluded entries listed under ``[project].exclude_docs`` in ``zensical.toml``.

    :param zensical_path: Path to ``zensical.toml``.
    :type zensical_path: Path
    :return: Normalized set of excluded file or directory names.
    :rtype: set[str]
    """
    if not zensical_path.exists():
        return set()
    zensical_data = tomllib.loads(zensical_path.read_text(encoding="utf-8"))
    project_data = zensical_data.get("project", {})
    raw_exclude = project_data.get("exclude_docs", "") or ""
    return {line.strip().lstrip("/").rstrip("/") for line in str(raw_exclude).splitlines() if line.strip()}


@functools.cache
def macro_jinja_env() -> jinja2.Environment:
    """Load ``docs_macros`` via Zensical's ``_load_module`` into a strict Jinja2 macro environment.

    :return: Configured Jinja2 environment with ``docs_macros`` variables, macros, and filters registered.
    :rtype: jinja2.Environment
    """
    macro_env = MacroEnv(conf={"root_dir": str(REPO_ROOT), "docs_dir": "docs"})
    _load_module(macro_env, "docs_macros", REPO_ROOT)
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(TEMPLATES_DIR)),
        undefined=jinja2.StrictUndefined,
        autoescape=jinja2.select_autoescape(enabled_extensions=(), default_for_string=False, default=False),
    )
    env.globals.update(macro_env.variables)
    env.globals.update(macro_env.macros)
    env.filters.update(macro_env.filters)
    return env


def materialize_markdown_text(raw_text: str) -> str:
    """Expand Zensical ``render_macros: true`` front-matter Markdown text via ``docs_macros``.

    :param raw_text: Raw Markdown document text (with or without YAML front matter).
    :type raw_text: str
    :return: Rendered Markdown body when ``render_macros: true``, otherwise ``raw_text`` unchanged.
    :rtype: str
    """
    fm_match = FRONT_MATTER_RE.match(raw_text)
    if fm_match is None:
        return raw_text
    meta = yaml.safe_load(fm_match.group(1)) or {}
    if not isinstance(meta, dict) or meta.get("render_macros") is not True:
        return raw_text
    body = fm_match.group(2)
    return macro_jinja_env().from_string(body).render()


def materialize_markdown_file(md_path: Path) -> str:
    """Read ``md_path`` and expand Zensical ``render_macros: true`` front-matter pages via ``docs_macros``.

    :param md_path: Path to the Markdown file on disk.
    :type md_path: Path
    :return: Rendered Markdown string.
    :rtype: str
    """
    return materialize_markdown_text(md_path.read_text(encoding="utf-8"))


@functools.cache
def materialized_default_corpus() -> tuple[tuple[str, str], ...]:
    """Build and cache ``(label, rendered_markdown)`` pairs for ``README.md``, ``docs/*.md``, and generated stubs.

    :return: Immutable sequence of ``(relative_label, materialized_markdown)`` pairs.
    :rtype: tuple[tuple[str, str], ...]
    """
    excluded = excluded_doc_names()
    by_label: dict[str, str] = {"README.md": materialize_markdown_file(README_PATH)}
    for md_path in sorted(p for p in DOCS_DIR.glob("*.md") if p.name not in excluded):
        label = md_path.relative_to(REPO_ROOT).as_posix()
        by_label[label] = materialize_markdown_file(md_path)
    for rel_path, stub_text in build_generated_docs_plan().items():
        if rel_path.suffix == ".md" and rel_path.name not in excluded:
            by_label[f"docs/{rel_path.as_posix()}"] = materialize_markdown_text(stub_text)
    return tuple(by_label.items())


def resolve_markdown_sources(
    md_paths: Sequence[Path] | None,
    repo_root: Path = REPO_ROOT,
) -> Sequence[tuple[str, str]]:
    """Return ``(label, markdown_text)`` pairs from explicit ``md_paths`` or the cached materialized corpus.

    :param md_paths: Optional explicit sequence of Markdown file paths to read and materialize.
    :type md_paths: Sequence[Path] | None
    :param repo_root: Repository root used to compute relative labels for explicit paths.
    :type repo_root: Path
    :return: Sequence of ``(label, rendered_markdown)`` pairs.
    :rtype: Sequence[tuple[str, str]]
    """
    if md_paths is None:
        return materialized_default_corpus()
    pairs: list[tuple[str, str]] = []
    for md_path in md_paths:
        label = md_path.relative_to(repo_root).as_posix() if md_path.is_relative_to(repo_root) else md_path.name
        pairs.append((label, materialize_markdown_file(md_path)))
    return pairs
