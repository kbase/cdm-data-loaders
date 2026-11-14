"""Bundled schema asset discovery and context builders for ``docs/bundled_schemas.md``."""

from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path

from pydantic import computed_field

from docs_macros.constants import (
    EXCLUDED_SCHEMA_DIR_NAMES,
    PUBLISHED_JSONSCHEMA_SUBDIRS,
    PYTHON_ARTIFACT_SUFFIXES,
    REPO_ROOT,
    SCHEMA_EXTENSION_METADATA,
    SCHEMAS_SUBDIR,
    SITE_URL_BASE,
    SRC_ROOT,
)
from docs_macros.formatting import (
    CodeBlockContext,
    FrozenDocModel,
    rel_posix,
    repo_blob_url,
    slugify_anchor,
)


class SchemaAssetDoc(FrozenDocModel):
    """Documentation metadata and source block for a bundled non-Python file under ``schemas/``."""

    rel_path: Path
    short_title: str
    repo_rel_path: str
    github_url: str
    kind: str
    highlight_lang: str
    package_area: str
    code_block: CodeBlockContext
    published_rel_path: Path | None = None

    @computed_field
    @property
    def anchor_id(self) -> str:
        """Deterministic HTML anchor identifier for the schema asset.

        :return: Hyphen-delimited lowercase HTML anchor ID.
        :rtype: str
        """
        return slugify_anchor("schema", self.short_title)

    @computed_field
    @property
    def published_site_url(self) -> str | None:
        """Published documentation site URL when the asset is served statically under ``/schemas/``.

        :return: Full documentation site URL or ``None``.
        :rtype: str | None
        """
        if self.published_rel_path is None:
            return None
        return f"{SITE_URL_BASE}/{self.published_rel_path.as_posix()}"


class SchemaGroupContext(FrozenDocModel):
    """Schema assets grouped under a single ``package_area`` for ``pages/bundled_schemas.md.j2``."""

    group_name: str
    assets: tuple[SchemaAssetDoc, ...]

    @computed_field
    @property
    def anchor_id(self) -> str:
        """Deterministic HTML anchor identifier for the schema group.

        :return: Hyphen-delimited lowercase HTML anchor ID.
        :rtype: str
        """
        return slugify_anchor("schema-group", self.group_name)


class BundledSchemasPageContext(FrozenDocModel):
    """View model passed to ``pages/bundled_schemas.md.j2``."""

    assets: tuple[SchemaAssetDoc, ...]
    groups: tuple[SchemaGroupContext, ...]


def resolve_schema_file_metadata(path: Path) -> tuple[str, str]:
    """Return ``(kind, highlight_lang)`` metadata for a non-Python schema file.

    :param path: Schema asset file path.
    :type path: Path
    :return: Human-readable format label and Pygments highlight language identifier.
    :rtype: tuple[str, str]
    """
    suffix = path.suffix.lower()
    if suffix in SCHEMA_EXTENSION_METADATA:
        return SCHEMA_EXTENSION_METADATA[suffix]
    if suffix:
        return (f"{suffix.lstrip('.').upper()} File", "text")
    return ("File", "text")


def is_published_jsonschema(
    rel_to_schemas: Path,
    published_subdirs: Sequence[str] = PUBLISHED_JSONSCHEMA_SUBDIRS,
) -> bool:
    """Return whether ``rel_to_schemas`` is a JSON Schema served directly under ``/schemas/`` on the docs site.

    :param rel_to_schemas: Path relative to ``src/cdm_data_loaders/schemas/``.
    :type rel_to_schemas: Path
    :param published_subdirs: Top-level schema subdirectories published as static JSON Schemas.
    :type published_subdirs: Sequence[str]
    :return: ``True`` if the file is served statically under ``docs/schemas/``.
    :rtype: bool
    """
    return (
        rel_to_schemas.suffix.lower() == ".json"
        and len(rel_to_schemas.parts) > 1
        and rel_to_schemas.parts[0] in published_subdirs
    )


def discover_schema_assets(
    src_root: Path = SRC_ROOT,
    repo_root: Path = REPO_ROOT,
    published_subdirs: Sequence[str] = PUBLISHED_JSONSCHEMA_SUBDIRS,
) -> list[SchemaAssetDoc]:
    """Discover all bundled non-Python files under ``src_root / "schemas"`` with source blocks.

    :param src_root: Package source root directory.
    :type src_root: Path
    :param repo_root: Repository root directory.
    :type repo_root: Path
    :param published_subdirs: Top-level schema subdirectories published as static JSON Schemas.
    :type published_subdirs: Sequence[str]
    :return: Sorted list of discovered non-Python schema asset descriptors.
    :rtype: list[SchemaAssetDoc]
    """
    schemas_dir = src_root / SCHEMAS_SUBDIR
    if not schemas_dir.is_dir():
        return []

    assets: list[SchemaAssetDoc] = []
    for path in sorted(schemas_dir.rglob("*")):
        if not path.is_file():
            continue
        rel_to_schemas = path.relative_to(schemas_dir)
        if any(part in EXCLUDED_SCHEMA_DIR_NAMES or part.startswith(".") for part in rel_to_schemas.parts):
            continue
        if path.suffix.lower() in PYTHON_ARTIFACT_SUFFIXES:
            continue

        rel_path = path.relative_to(src_root)
        short_title = rel_to_schemas.as_posix()
        repo_rel = rel_posix(path, repo_root)
        kind, highlight_lang = resolve_schema_file_metadata(path)
        package_area = rel_to_schemas.parts[0] if len(rel_to_schemas.parts) > 1 else SCHEMAS_SUBDIR
        published_rel_path = (
            Path(SCHEMAS_SUBDIR) / rel_to_schemas
            if is_published_jsonschema(rel_to_schemas, published_subdirs)
            else None
        )
        source_text = path.read_text(encoding="utf-8").rstrip()
        code_block = CodeBlockContext(
            content=source_text,
            lang=highlight_lang,
            title=repo_rel,
            linenums=True,
        )
        assets.append(
            SchemaAssetDoc(
                rel_path=rel_path,
                short_title=short_title,
                repo_rel_path=repo_rel,
                github_url=repo_blob_url(path, repo_root),
                kind=kind,
                highlight_lang=highlight_lang,
                package_area=package_area,
                code_block=code_block,
                published_rel_path=published_rel_path,
            )
        )
    return assets


def build_bundled_schemas_context(
    assets: Sequence[SchemaAssetDoc] | None = None,
    *,
    src_root: Path = SRC_ROOT,
    repo_root: Path = REPO_ROOT,
) -> BundledSchemasPageContext:
    """Build the ``BundledSchemasPageContext`` view model for ``pages/bundled_schemas.md.j2``.

    :param assets: Optional pre-discovered schema assets (discovered from ``src_root`` when ``None``).
    :type assets: Sequence[SchemaAssetDoc] | None
    :param src_root: Package source root directory.
    :type src_root: Path
    :param repo_root: Repository root directory.
    :type repo_root: Path
    :return: Populated ``BundledSchemasPageContext``.
    :rtype: BundledSchemasPageContext
    """
    resolved = tuple(assets) if assets is not None else tuple(discover_schema_assets(src_root, repo_root))
    by_group: dict[str, list[SchemaAssetDoc]] = defaultdict(list)
    for asset in resolved:
        by_group[asset.package_area].append(asset)

    groups = tuple(
        SchemaGroupContext(
            group_name=group_name,
            assets=tuple(group_assets),
        )
        for group_name, group_assets in sorted(by_group.items())
    )
    return BundledSchemasPageContext(assets=resolved, groups=groups)
