"""Zensical macros plugin entrypoint for ``docs_macros``."""

import importlib
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Protocol


class MacroPluginEnv(Protocol):
    """Structural interface for Zensical's ``MacroEnv`` plugin registration object."""

    def macro(self, fn: Callable[..., str], name: str | None = None) -> object:
        """Register a Python callable as a Jinja2 macro in Zensical's macros preprocessor.

        :param fn: Callable returning rendered Markdown text.
        :type fn: Callable[..., str]
        :param name: Optional macro name override.
        :type name: str | None
        :return: Registered callable.
        :rtype: object
        """

    def filter(self, fn: Callable[..., str], name: str | None = None) -> object:
        """Register a Python callable as a Jinja2 filter in Zensical's macros preprocessor.

        :param fn: Filter callable transforming strings.
        :type fn: Callable[..., str]
        :param name: Optional filter name override.
        :type name: str | None
        :return: Registered callable.
        :rtype: object
        """


def define_env(env: MacroPluginEnv) -> None:
    """Register documentation rendering macros and filters for Zensical's ``[project.plugins.macros]`` extension.

    :param env: Zensical ``MacroEnv`` instance passed during Markdown preprocessing.
    :type env: MacroPluginEnv
    :return: None.
    :rtype: None
    """
    repo_root_str = str(Path(__file__).resolve().parent.parent)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)

    env_mod = importlib.import_module("docs_macros.env")
    fmt_mod = importlib.import_module("docs_macros.formatting")
    env.macro(env_mod.render_cli_reference, "render_cli_reference")
    env.macro(env_mod.render_bundled_schemas, "render_bundled_schemas")
    env.macro(env_mod.render_converter_spec, "render_converter_spec")
    env.macro(env_mod.render_pipeline_spec, "render_pipeline_spec")
    env.macro(env_mod.render_reference_table, "render_reference_table")
    env.filter(fmt_mod.escape_table_cell, "escape_cell")
    env.filter(fmt_mod.code_fence_for_text, "code_fence")
