"""Catalog-wide CLI shortcut and entry-point checks across all registered console scripts."""

import pytest

from cdm_data_loaders.core.pipeline_catalog import load_project_scripts, resolve_pipeline
from tests.helpers.cli_assertions import assert_no_cli_clashes

SCRIPT_NAMES = tuple(load_project_scripts())


@pytest.mark.parametrize("cli_command", SCRIPT_NAMES, ids=SCRIPT_NAMES)
def test_registered_console_script_resolves_and_has_no_cli_clashes(cli_command: str) -> None:
    """Every ``[project.scripts]`` entry resolves cleanly and defines conflict-free CLI flags/shortcuts."""
    spec = resolve_pipeline(cli_command)
    assert spec.cli_command == cli_command
    assert callable(spec.cli_callable)
    if spec.settings_cls is not None:
        assert_no_cli_clashes(spec.settings_cls)
