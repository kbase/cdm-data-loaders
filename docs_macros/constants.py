"""Shared paths, URLs, and file-extension metadata constants for documentation rendering."""

import re
from pathlib import Path
from types import MappingProxyType
from typing import Final

REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
DOCS_DIR: Final[Path] = REPO_ROOT / "docs"
TEMPLATES_DIR: Final[Path] = DOCS_DIR / "_templates"
PYPROJECT_PATH: Final[Path] = REPO_ROOT / "pyproject.toml"
SRC_ROOT: Final[Path] = REPO_ROOT / "src" / "cdm_data_loaders"
SCHEMAS_SUBDIR: Final[str] = "schemas"
PUBLISHED_JSONSCHEMA_SUBDIRS: Final[tuple[str, ...]] = ("xsv_config",)
CONVERTER_SPECS_ROOT: Final[Path] = REPO_ROOT / "specs" / "schema_converters"
PIPELINE_SPECS_ROOT: Final[Path] = REPO_ROOT / "specs" / "pipelines"
SITE_URL_BASE: Final[str] = "https://kbase.github.io/cdm-data-loaders"
GITHUB_BLOB_BASE: Final[str] = "https://github.com/kbase/cdm-data-loaders/blob/develop"

PYTHON_ARTIFACT_SUFFIXES: Final[frozenset[str]] = frozenset({".py", ".pyc", ".pyo", ".pyd", ".pyi"})
EXCLUDED_SCHEMA_DIR_NAMES: Final[frozenset[str]] = frozenset({"__pycache__"})
BACKTICK_RUN_RE: Final[re.Pattern[str]] = re.compile(r"`+")
NON_ALNUM_SLUG_RE: Final[re.Pattern[str]] = re.compile(r"[^a-z0-9]+")

SCHEMA_EXTENSION_METADATA: Final[MappingProxyType[str, tuple[str, str]]] = MappingProxyType(
    {
        ".avsc": ("Avro Schema", "json"),
        ".csv": ("CSV", "text"),
        ".graphql": ("GraphQL Schema", "graphql"),
        ".json": ("JSON Schema", "json"),
        ".jsonl": ("JSON Lines", "json"),
        ".md": ("Markdown", "markdown"),
        ".proto": ("Protocol Buffers", "protobuf"),
        ".sql": ("SQL", "sql"),
        ".toml": ("TOML", "toml"),
        ".tsv": ("TSV", "text"),
        ".txt": ("Text", "text"),
        ".xml": ("XML", "xml"),
        ".xsd": ("XML Schema (XSD)", "xml"),
        ".yaml": ("YAML", "yaml"),
        ".yml": ("YAML", "yaml"),
    }
)

CODE_LANG_BY_SUFFIX: Final[MappingProxyType[str, str]] = MappingProxyType(
    {
        ".json": "json",
        ".jsonl": "json",
        ".md5": "text",
        ".py": "python",
        ".toml": "toml",
        ".txt": "text",
        ".xml": "xml",
        ".xsd": "xml",
        ".yaml": "yaml",
        ".yml": "yaml",
    }
)
