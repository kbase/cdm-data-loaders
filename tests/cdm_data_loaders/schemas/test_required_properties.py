"""Check that the bundled JSON Schemas only require properties they define."""

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Final

import pytest

import cdm_data_loaders.schemas

SCHEMA_DIR: Final[Path] = Path(cdm_data_loaders.schemas.__file__).parent
SCHEMA_FILES: Final[list[Path]] = sorted(SCHEMA_DIR.rglob("*.json"))


def _objects_with_required(node: Any) -> Iterator[dict[str, Any]]:  # noqa: ANN401
    """Yield every sub-schema of ``node`` that has both ``properties`` and ``required``."""
    if isinstance(node, dict):
        if isinstance(node.get("properties"), dict) and isinstance(node.get("required"), list):
            yield node
        for value in node.values():
            yield from _objects_with_required(value)
    elif isinstance(node, list):
        for value in node:
            yield from _objects_with_required(value)


def test_schema_files_found() -> None:
    """Ensure the parametrized test below has schemas to check."""
    assert SCHEMA_FILES


@pytest.mark.parametrize("path", SCHEMA_FILES, ids=lambda p: str(p.relative_to(SCHEMA_DIR)))
def test_required_names_are_properties(path: Path) -> None:
    """Every name in a ``required`` list must be defined in the same sub-schema's ``properties``."""
    schema = json.loads(path.read_text())
    for obj in _objects_with_required(schema):
        missing = sorted(set(obj["required"]) - set(obj["properties"]))
        assert not missing, f"{path.name} requires names with no matching property: {missing}"
