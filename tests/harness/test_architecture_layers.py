"""AST-based architecture lint tests enforcing the 6-layer DAG, schemas isolation, and test hygiene."""

import ast
from pathlib import Path
from types import MappingProxyType
from typing import Final

import pytest

from tests._support.paths_and_constants import REPO_ROOT

SRC_PACKAGE_DIR: Final[Path] = REPO_ROOT / "src" / "cdm_data_loaders"
SPECS_LOADER_DIR: Final[Path] = REPO_ROOT / "specs" / "_loader"
DOCS_MACROS_DIR: Final[Path] = REPO_ROOT / "docs_macros"
TESTS_DIR: Final[Path] = REPO_ROOT / "tests"

ALLOWED_PACKAGE_IMPORTS: Final[MappingProxyType[str, frozenset[str]]] = MappingProxyType(
    {
        # Layer 0: zero internal cdm_data_loaders cross-package imports
        "utils": frozenset({"utils"}),
        # Layer 1: io and validation
        "io": frozenset({"utils", "io"}),
        "validation": frozenset({"utils", "validation"}),
        # Bundled schema catalog: static PyArrow schemas only
        "schemas": frozenset({"utils", "schemas"}),
        # Layer 2: converters and coercion
        "converters": frozenset({"utils", "io", "validation", "converters", "coercion"}),
        "coercion": frozenset({"utils", "io", "validation", "converters", "coercion"}),
        # Layer 3: core framework
        "core": frozenset({"utils", "io", "validation", "converters", "coercion", "core"}),
        # Layer 4: readers, parsers, datasets
        "readers": frozenset({"utils", "io", "validation", "converters", "coercion", "core", "readers"}),
        "parsers": frozenset(
            {"utils", "io", "validation", "converters", "coercion", "core", "readers", "parsers", "schemas"}
        ),
        "datasets": frozenset(
            {
                "utils",
                "io",
                "validation",
                "converters",
                "coercion",
                "core",
                "readers",
                "parsers",
                "datasets",
                "schemas",
            }
        ),
        # Layer 5: pipelines and standalone CLIs
        "pipelines": frozenset(
            {
                "utils",
                "io",
                "validation",
                "converters",
                "coercion",
                "core",
                "readers",
                "parsers",
                "datasets",
                "schemas",
                "pipelines",
            }
        ),
        "cli": frozenset(
            {
                "utils",
                "io",
                "validation",
                "converters",
                "coercion",
                "core",
                "readers",
                "parsers",
                "datasets",
                "schemas",
                "cli",
            }
        ),
    }
)

ALLOWED_SCHEMAS_FILENAMES: Final[frozenset[str]] = frozenset({"__init__.py", "schema.py"})
ALLOWED_SCHEMAS_ASSET_SUFFIXES: Final[frozenset[str]] = frozenset({".xsd", ".json"})


def _iter_imported_modules(py_file: Path) -> list[tuple[int, str]]:
    """Parse ``py_file`` with ``ast`` and return ``(lineno, dotted_module)`` for every import statement.

    :param py_file: Python file path to inspect.
    :type py_file: Path
    :return: List of ``(line_number, imported_module_path)`` pairs.
    :rtype: list[tuple[int, str]]
    """
    tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
    imports: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend((node.lineno, alias.name) for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imports.append((node.lineno, node.module))
    return imports


def test_all_top_level_src_packages_are_registered_in_layer_dag() -> None:
    """Verify every top-level subpackage under ``src/cdm_data_loaders/`` is declared in ``ALLOWED_PACKAGE_IMPORTS``."""
    discovered = {
        entry.name
        for entry in SRC_PACKAGE_DIR.iterdir()
        if entry.is_dir() and not entry.name.startswith((".", "__pycache__"))
    }
    assert discovered == set(ALLOWED_PACKAGE_IMPORTS)


@pytest.mark.parametrize("package_name", sorted(ALLOWED_PACKAGE_IMPORTS), ids=str)
def test_src_package_obeys_layer_dependency_dag(package_name: str) -> None:
    """Verify modules in ``src/cdm_data_loaders/<package_name>/`` only import allowed lower/peer layers."""
    allowed = ALLOWED_PACKAGE_IMPORTS[package_name]
    pkg_dir = SRC_PACKAGE_DIR / package_name
    violations: list[str] = []

    for py_file in sorted(pkg_dir.rglob("*.py")):
        rel_path = py_file.relative_to(REPO_ROOT)
        for lineno, mod_name in _iter_imported_modules(py_file):
            root_pkg, sep, rest = mod_name.partition(".")
            if root_pkg != "cdm_data_loaders" or not sep:
                continue
            target_pkg = rest.split(".", 1)[0]
            if target_pkg not in allowed:
                violations.append(f"{rel_path}:{lineno} imports forbidden package {mod_name!r}")

    assert violations == []


def test_schemas_directory_contains_only_assets_and_static_schema_modules() -> None:
    """Verify ``src/cdm_data_loaders/schemas/`` contains only bundled assets, ``__init__.py``, and static ``schema.py``."""
    schemas_dir = SRC_PACKAGE_DIR / "schemas"
    unexpected_files: list[str] = []
    forbidden_imports: list[str] = []

    for path in sorted(schemas_dir.rglob("*")):
        if path.is_dir() or "__pycache__" in path.parts or path.name.startswith("."):
            continue
        rel_path = path.relative_to(REPO_ROOT)
        if path.name not in ALLOWED_SCHEMAS_FILENAMES and path.suffix not in ALLOWED_SCHEMAS_ASSET_SUFFIXES:
            unexpected_files.append(str(rel_path))
        if path.suffix == ".py":
            for lineno, mod_name in _iter_imported_modules(path):
                if mod_name.startswith("cdm_data_loaders") and mod_name != "cdm_data_loaders.utils.arrow_schema":
                    forbidden_imports.append(f"{rel_path}:{lineno} imports {mod_name!r}")

    assert unexpected_files == []
    assert forbidden_imports == []


def test_specs_loader_and_docs_macros_never_import_pytest_or_tests() -> None:
    """Verify ``specs/_loader/`` and ``docs_macros/`` have zero ``pytest`` or ``tests.*`` imports."""
    roots = [SPECS_LOADER_DIR]
    if DOCS_MACROS_DIR.is_dir():
        roots.append(DOCS_MACROS_DIR)

    violations: list[str] = []
    for root in roots:
        for py_file in sorted(root.rglob("*.py")):
            rel_path = py_file.relative_to(REPO_ROOT)
            for lineno, mod_name in _iter_imported_modules(py_file):
                top = mod_name.split(".", 1)[0]
                if top in {"pytest", "_pytest", "tests"}:
                    violations.append(f"{rel_path}:{lineno} imports {mod_name!r}")

    assert violations == []


def test_tests_never_import_from_conftest_modules() -> None:
    """Verify no file under ``tests/`` imports symbols from any ``conftest`` module."""
    violations: list[str] = []
    for py_file in sorted(TESTS_DIR.rglob("*.py")):
        rel_path = py_file.relative_to(REPO_ROOT)
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                violations.extend(
                    f"{rel_path}:{node.lineno} imports {alias.name!r}"
                    for alias in node.names
                    if "conftest" in alias.name.split(".")
                )
            elif isinstance(node, ast.ImportFrom):
                mod_parts = node.module.split(".") if node.module else []
                imported_conftest = "conftest" in mod_parts or any(alias.name == "conftest" for alias in node.names)
                if imported_conftest:
                    violations.append(f"{rel_path}:{node.lineno} imports from conftest ({node.module!r})")

    assert violations == []
