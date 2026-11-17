"""Pytest fixtures for standalone file parser tests."""

from typing import Any

import pytest

from tests.cdm_data_loaders.parsers._helpers import RESULTS


@pytest.fixture(scope="session")
def checkm2_stats_results() -> dict[str, Any]:
    """Return expected results from parsing CheckM2 and BBMap stats files.

    :return: Expected parser results dictionary.
    :rtype: dict[str, Any]
    """
    return RESULTS
