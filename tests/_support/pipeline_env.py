"""Environment configuration and input staging for manifest-driven pipeline smoke tests."""

import shutil
from collections.abc import Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final
from urllib.parse import unquote, urlparse

import boto3
from dlt.common.libs.pyiceberg import drop_iceberg_table, get_catalog, is_ephemeral_catalog
from pydantic_settings import BaseSettings
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError, RESTError
from types_boto3_s3.client import S3Client

from cdm_data_loaders.utils.iceberg.cleanup import install_iceberg_catalog_cleanup
from specs._loader.pipeline_spec import load_settings_file
from tests._support.dlt_isolation import load_dlt_toml_files
from tests._support.paths_and_constants import LOCAL_FS

SETTINGS_FILENAMES: Final[tuple[str, ...]] = (
    "settings.toml",
    "settings.yaml",
    "settings.yml",
    "settings.json",
)
ENV_TEST_OPTION_KEYS: Final[frozenset[str]] = frozenset(
    {
        "drop_existing_tables",
        "drop_tables_after",
    }
)


def discover_env_settings_file(
    config_dir: Path | None,
    explicit_settings_file: Path | str | None = None,
) -> Path | None:
    """Resolve the environment settings configuration file path.

    :param config_dir: Optional directory passed via ``--dlt-config-dir``.
    :type config_dir: Path | None
    :param explicit_settings_file: Optional path passed via ``--settings-file``.
    :type explicit_settings_file: Path | str | None
    :return: Resolved ``Path`` to the settings file, or ``None`` when absent.
    :rtype: Path | None
    :raises FileNotFoundError: If ``explicit_settings_file`` does not exist.
    """
    if explicit_settings_file is not None:
        resolved = Path(explicit_settings_file).resolve()
        if not resolved.is_file():
            msg = f"Settings config file does not exist: {resolved}"
            raise FileNotFoundError(msg)
        return resolved

    if config_dir is not None:
        base = Path(config_dir).resolve()
        for filename in SETTINGS_FILENAMES:
            candidate = base / filename
            if candidate.is_file():
                return candidate
    return None


def parse_env_settings_doc(raw: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Split a parsed settings config document into global overrides and per-pipeline overrides.

    :param raw: Parsed settings mapping.
    :type raw: Mapping[str, Any]
    :return: Pair of ``(global_overrides, pipeline_overrides)``.
    :rtype: tuple[dict[str, Any], dict[str, dict[str, Any]]]
    :raises TypeError: If a ``pipelines.<name>`` entry is not a mapping.
    """
    global_overrides: dict[str, Any] = {}
    pipeline_overrides: dict[str, dict[str, Any]] = {}

    for key, value in raw.items():
        if key == "pipelines" and isinstance(value, Mapping):
            for pipe_name, pipe_cfg in value.items():
                if not isinstance(pipe_cfg, Mapping):
                    msg = f"Expected mapping for pipelines.{pipe_name}, got {type(pipe_cfg).__name__}"
                    raise TypeError(msg)
                pipeline_overrides.setdefault(str(pipe_name), {}).update(dict(pipe_cfg))
        elif isinstance(value, Mapping):
            pipeline_overrides.setdefault(str(key), {}).update(dict(value))
        else:
            global_overrides[str(key)] = value

    return global_overrides, pipeline_overrides


def _infer_use_destination(
    global_overrides: Mapping[str, Any],
    dlt_config: Mapping[str, Any],
    dlt_secrets: Mapping[str, Any],
) -> str:
    """Infer target ``use_destination`` name from overrides or dlt config sections.

    :param global_overrides: Global settings overrides.
    :type global_overrides: Mapping[str, Any]
    :param dlt_config: Parsed ``config.toml``.
    :type dlt_config: Mapping[str, Any]
    :param dlt_secrets: Parsed ``secrets.toml``.
    :type dlt_secrets: Mapping[str, Any]
    :return: Destination name.
    :rtype: str
    """
    if global_overrides.get("use_destination"):
        return str(global_overrides["use_destination"])

    dest_keys: list[str] = []
    for doc in (dlt_config, dlt_secrets):
        dest_section = doc.get("destination")
        if isinstance(dest_section, Mapping):
            for key in dest_section:
                if str(key) not in dest_keys:
                    dest_keys.append(str(key))
    if len(dest_keys) == 1:
        return dest_keys[0]
    return LOCAL_FS


def _has_configured_destination_target(
    use_destination: str,
    dlt_config: Mapping[str, Any],
    dlt_secrets: Mapping[str, Any],
) -> bool:
    """Check whether ``use_destination`` defines an explicit bucket or non-filesystem target.

    :param use_destination: Destination name.
    :type use_destination: str
    :param dlt_config: Parsed ``config.toml``.
    :type dlt_config: Mapping[str, Any]
    :param dlt_secrets: Parsed ``secrets.toml``.
    :type dlt_secrets: Mapping[str, Any]
    :return: ``True`` when an explicit destination target is configured.
    :rtype: bool
    """
    for doc in (dlt_config, dlt_secrets):
        dest_section = doc.get("destination")
        if not isinstance(dest_section, Mapping):
            continue
        block = dest_section.get(use_destination)
        if not isinstance(block, Mapping):
            continue
        if block.get("bucket_url"):
            return True
        dest_type = block.get("destination_type")
        if dest_type and str(dest_type) != "filesystem":
            return True
    return False


@dataclass(frozen=True, slots=True)
class PipelineEnvSpec:
    """Execution environment configuration for a pipeline smoke test."""

    name: str
    use_destination: str
    dlt_config: dict[str, Any] = field(default_factory=dict)
    dlt_secrets: dict[str, Any] = field(default_factory=dict)
    dlt_config_dir: Path | None = None
    settings_file: Path | None = None
    settings_overrides: dict[str, Any] = field(default_factory=dict)
    pipeline_settings_overrides: dict[str, dict[str, Any]] = field(default_factory=dict)
    preserve_dlt_destination: bool = False
    drop_existing_tables: bool = False
    drop_tables_after: bool = False
    s3_client: S3Client | None = None
    s3_input_bucket: str | None = None

    def should_drop_existing_tables(self, pipeline_name: str, *, cli_flag: bool = False) -> bool:
        """Return whether existing tables for ``pipeline_name`` should be dropped before running.

        :param pipeline_name: Pipeline identifier.
        :type pipeline_name: str
        :param cli_flag: Value of ``--drop-existing-tables``.
        :type cli_flag: bool
        :return: ``True`` when pre-run table drop is enabled.
        :rtype: bool
        """
        pipe_cfg = self.pipeline_settings_overrides.get(pipeline_name, {})
        if "drop_existing_tables" in pipe_cfg:
            return bool(pipe_cfg["drop_existing_tables"])
        return bool(cli_flag or self.drop_existing_tables)

    def should_drop_tables_after(self, pipeline_name: str, *, cli_flag: bool = False) -> bool:
        """Return whether tables added by ``pipeline_name`` should be dropped after the test finishes.

        :param pipeline_name: Pipeline identifier.
        :type pipeline_name: str
        :param cli_flag: Value of ``--drop-tables-after``.
        :type cli_flag: bool
        :return: ``True`` when post-run table drop is enabled.
        :rtype: bool
        """
        pipe_cfg = self.pipeline_settings_overrides.get(pipeline_name, {})
        if "drop_tables_after" in pipe_cfg:
            return bool(pipe_cfg["drop_tables_after"])
        return bool(cli_flag or self.drop_tables_after)

    def settings_for_pipeline(
        self,
        pipeline_name: str,
        settings_cls: type[BaseSettings] | None = None,
    ) -> dict[str, Any]:
        """Return merged environment settings overrides for ``pipeline_name``.

        :param pipeline_name: Pipeline identifier.
        :type pipeline_name: str
        :param settings_cls: Optional settings class to validate field names against.
        :type settings_cls: type[BaseSettings] | None
        :return: Filtered settings dictionary for ``pipeline_name``.
        :rtype: dict[str, Any]
        :raises ValueError: If an unknown field is supplied for ``settings_cls``.
        """
        merged: dict[str, Any] = {
            k: v
            for k, v in {
                **self.settings_overrides,
                **self.pipeline_settings_overrides.get(pipeline_name, {}),
            }.items()
            if k not in ENV_TEST_OPTION_KEYS
        }
        if settings_cls is None:
            return merged

        valid_fields = set(settings_cls.model_fields)
        filtered: dict[str, Any] = {}
        for key, value in merged.items():
            if key in valid_fields:
                filtered[key] = value
            elif hasattr(settings_cls, key):
                continue
            else:
                msg = f"Unknown field {key!r} for {settings_cls.__name__} in environment {self.name!r}"
                raise ValueError(msg)
        return filtered

    def isolated_dlt_configs(
        self,
        run_slug: str,
        *,
        isolate_bucket_url: bool = True,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Return deep-copied ``(dlt_config, dlt_secrets)`` with an isolated per-run ``bucket_url``.

        :param run_slug: Unique per-run path suffix.
        :type run_slug: str
        :param isolate_bucket_url: Whether to append ``run_slug`` to ``bucket_url``.
        :type isolate_bucket_url: bool
        :return: Pair of ``(dlt_config, dlt_secrets)``.
        :rtype: tuple[dict[str, Any], dict[str, Any]]
        """
        cfg = deepcopy(self.dlt_config)
        sec = deepcopy(self.dlt_secrets)
        if not self.preserve_dlt_destination or not isolate_bucket_url:
            return cfg, sec

        for doc in (cfg, sec):
            dest_section = doc.get("destination")
            if isinstance(dest_section, dict):
                block = dest_section.get(self.use_destination)
                if isinstance(block, dict) and block.get("bucket_url"):
                    base_url = str(block["bucket_url"]).rstrip("/")
                    block["bucket_url"] = f"{base_url}/{run_slug}"
        return cfg, sec


def load_pipeline_env_from_dir(
    config_dir: Path | None,
    settings_file: Path | str | None = None,
) -> PipelineEnvSpec:
    """Build a ``PipelineEnvSpec`` from ``config.toml``, ``secrets.toml``, and ``settings.toml``/``.yaml``.

    :param config_dir: Optional directory containing ``config.toml`` and ``secrets.toml``.
    :type config_dir: Path | None
    :param settings_file: Optional settings override file path.
    :type settings_file: Path | str | None
    :return: Populated ``PipelineEnvSpec``.
    :rtype: PipelineEnvSpec
    """
    resolved_dir = Path(config_dir).resolve() if config_dir is not None else None
    dlt_config, dlt_secrets = load_dlt_toml_files(resolved_dir) if resolved_dir is not None else ({}, {})
    resolved_settings_file = discover_env_settings_file(resolved_dir, settings_file)
    raw_settings = load_settings_file(resolved_settings_file) if resolved_settings_file is not None else {}
    global_overrides, pipeline_overrides = parse_env_settings_doc(raw_settings)

    use_destination = _infer_use_destination(global_overrides, dlt_config, dlt_secrets)
    preserve_dest = _has_configured_destination_target(use_destination, dlt_config, dlt_secrets)
    env_name = resolved_dir.name if resolved_dir is not None else "custom_settings"

    return PipelineEnvSpec(
        name=env_name,
        use_destination=use_destination,
        dlt_config=dlt_config,
        dlt_secrets=dlt_secrets,
        dlt_config_dir=resolved_dir,
        settings_file=resolved_settings_file,
        settings_overrides=global_overrides,
        pipeline_settings_overrides=pipeline_overrides,
        preserve_dlt_destination=preserve_dest,
        drop_existing_tables=bool(global_overrides.get("drop_existing_tables", False)),
        drop_tables_after=bool(global_overrides.get("drop_tables_after", False)),
    )


def list_iceberg_namespace_tables(dataset_name: str) -> set[str]:
    """Return table names registered in ``dataset_name`` in the active Iceberg catalog.

    :param dataset_name: Target Iceberg namespace.
    :type dataset_name: str
    :return: Unqualified table names in ``dataset_name``.
    :rtype: set[str]
    """
    install_iceberg_catalog_cleanup()
    catalog = get_catalog()
    try:
        if is_ephemeral_catalog(catalog):
            return set()
        try:
            identifiers = catalog.list_tables(dataset_name)
        except NoSuchNamespaceError:
            return set()
        return {
            ident[-1] if isinstance(ident, tuple) else str(ident).rsplit(".", maxsplit=1)[-1] for ident in identifiers
        }
    finally:
        if hasattr(catalog, "close"):
            catalog.close()


def resolve_destination_bucket_url(
    dlt_config: Mapping[str, Any],
    dlt_secrets: Mapping[str, Any],
    use_destination: str | None,
) -> str | None:
    """Return the configured ``bucket_url`` for ``use_destination`` from dlt config or secrets.

    :param dlt_config: dlt configuration mapping.
    :type dlt_config: Mapping[str, Any]
    :param dlt_secrets: dlt secrets mapping.
    :type dlt_secrets: Mapping[str, Any]
    :param use_destination: Destination name.
    :type use_destination: str | None
    :return: Stripped ``bucket_url`` string or ``None``.
    :rtype: str | None
    """
    if not use_destination:
        return None
    for doc in (dlt_config, dlt_secrets):
        dest_section = doc.get("destination")
        if isinstance(dest_section, Mapping):
            block = dest_section.get(use_destination)
            if isinstance(block, Mapping) and block.get("bucket_url"):
                return str(block["bucket_url"]).rstrip("/")
    return None


def _purge_location_files(
    location: str,
    *,
    s3_client: S3Client | None = None,
) -> None:
    """Delete any remaining files under ``location`` on local filesystem or S3.

    :param location: Local path, ``file://`` URI, or ``s3://`` URI.
    :type location: str
    :param s3_client: Optional boto3 S3 client for S3 prefixes.
    :type s3_client: S3Client | None
    """
    parsed = urlparse(location)
    if parsed.scheme in {"s3", "s3a"}:
        if s3_client is None:
            return
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/").rstrip("/")
        if not bucket or not prefix:
            return
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
            contents = page.get("Contents") or []
            if not contents:
                continue
            for start in range(0, len(contents), 1000):
                batch = [{"Key": obj["Key"]} for obj in contents[start : start + 1000]]
                s3_client.delete_objects(Bucket=bucket, Delete={"Objects": batch})  # pyright: ignore[reportArgumentType]
        return

    local_path = Path(unquote(parsed.path)) if parsed.scheme == "file" else Path(location)
    if local_path.exists():
        shutil.rmtree(local_path, ignore_errors=True)


def _collect_table_locations(
    catalog: Catalog,
    table_id: str,
    dataset_name: str,
    table_name: str,
    bucket_url: str | None,
) -> list[str]:
    """Return unique filesystem or S3 locations associated with ``table_id``.

    :param catalog: Open PyIceberg catalog.
    :type catalog: Catalog
    :param table_id: Qualified table identifier string.
    :type table_id: str
    :param dataset_name: Dataset namespace name.
    :type dataset_name: str
    :param table_name: Table name.
    :type table_name: str
    :param bucket_url: Optional fallback destination bucket URL.
    :type bucket_url: str | None
    :return: Ordered unique location strings.
    :rtype: list[str]
    """
    locations: list[str] = []
    try:
        table_loc = catalog.load_table(table_id).location()
        if table_loc:
            locations.append(table_loc)
    except (NoSuchTableError, RESTError):
        pass
    if bucket_url:
        fallback_loc = f"{bucket_url.rstrip('/')}/{dataset_name}/{table_name}"
        if fallback_loc not in locations:
            locations.append(fallback_loc)
    return locations


def _build_s3_client_from_secrets(
    dlt_secrets: Mapping[str, Any],
    use_destination: str,
) -> S3Client:
    """Construct a boto3 S3 client from dlt secrets if credentials are present.

    :param dlt_secrets: dlt secrets mapping.
    :type dlt_secrets: Mapping[str, Any]
    :param use_destination: Destination block name for fallback credential lookup.
    :type use_destination: str
    :return: Configured boto3 ``S3Client``.
    :rtype: S3Client
    """
    creds: Mapping[str, Any] = {}
    sources_block = dlt_secrets.get("sources")
    if isinstance(sources_block, Mapping):
        fs_block = sources_block.get("filesystem")
        if isinstance(fs_block, Mapping) and isinstance(fs_block.get("credentials"), Mapping):
            creds = fs_block["credentials"]
        elif isinstance(sources_block.get("credentials"), Mapping):
            creds = sources_block["credentials"]
    if not creds:
        dest_block = dlt_secrets.get("destination")
        if isinstance(dest_block, Mapping):
            named_block = dest_block.get(use_destination)
            if isinstance(named_block, Mapping) and isinstance(named_block.get("credentials"), Mapping):
                creds = named_block["credentials"]

    kwargs: dict[str, Any] = {}
    for cred_key in ("aws_access_key_id", "aws_secret_access_key", "endpoint_url", "region_name"):
        if cred_key in creds:
            kwargs[cred_key] = creds[cred_key]
    return boto3.client("s3", **kwargs)


def drop_iceberg_tables_for_dataset(
    dataset_name: str,
    table_names: Iterable[str] | None = None,
    *,
    include_namespace_tables: bool = True,
    bucket_url: str | None = None,
    s3_client: S3Client | None = None,
    dlt_secrets: Mapping[str, Any] | None = None,
    use_destination: str | None = None,
) -> list[str]:
    """Drop tables in ``dataset_name`` from the active Iceberg catalog, purge their files, and return dropped names.

    :param dataset_name: Target Iceberg namespace.
    :type dataset_name: str
    :param table_names: Optional table names to target.
    :type table_names: Iterable[str] | None
    :param include_namespace_tables: Whether to also list all tables in ``dataset_name``.
    :type include_namespace_tables: bool
    :param bucket_url: Optional fallback destination bucket URL.
    :type bucket_url: str | None
    :param s3_client: Optional pre-built boto3 S3 client.
    :type s3_client: S3Client | None
    :param dlt_secrets: Optional dlt secrets dictionary for S3 cleanup.
    :type dlt_secrets: Mapping[str, Any] | None
    :param use_destination: Optional destination name for S3 credential lookup.
    :type use_destination: str | None
    :return: Sorted list of dropped table names.
    :rtype: list[str]
    """
    install_iceberg_catalog_cleanup()
    catalog = get_catalog()
    try:
        if is_ephemeral_catalog(catalog):
            return []
        targets: set[str] = set(table_names) if table_names is not None else set()
        if include_namespace_tables or table_names is None:
            try:
                identifiers = catalog.list_tables(dataset_name)
            except NoSuchNamespaceError:
                identifiers = []
            targets.update(
                ident[-1] if isinstance(ident, tuple) else str(ident).rsplit(".", maxsplit=1)[-1]
                for ident in identifiers
            )

        active_s3_client = s3_client
        dropped_tables: list[str] = []
        for table_name in sorted(targets):
            table_id = f"{dataset_name}.{table_name}"
            locations = _collect_table_locations(catalog, table_id, dataset_name, table_name, bucket_url)

            if drop_iceberg_table(catalog, table_id, purge=True):
                dropped_tables.append(table_name)

            for loc in locations:
                if (
                    urlparse(loc).scheme in {"s3", "s3a"}
                    and active_s3_client is None
                    and dlt_secrets
                    and use_destination
                ):
                    active_s3_client = _build_s3_client_from_secrets(dlt_secrets, use_destination)
                _purge_location_files(loc, s3_client=active_s3_client)

        return dropped_tables
    finally:
        if hasattr(catalog, "close"):
            catalog.close()


def stage_input_directory(
    source_dir: Path,
    target_dir_or_url: str | Path,
    *,
    input_subdir: str | None = None,
    s3_client: S3Client | None = None,
    dlt_secrets: Mapping[str, Any] | None = None,
    use_destination: str = LOCAL_FS,
) -> str:
    """Copy all files in ``source_dir`` (or a single file) to a local directory or ``s3://`` URL prefix.

    :param source_dir: Source fixture directory or single file.
    :type source_dir: Path
    :param target_dir_or_url: Local directory path or ``s3://bucket/prefix`` URL.
    :type target_dir_or_url: str | Path
    :param input_subdir: Optional subdirectory name when staging a single file.
    :type input_subdir: str | None
    :param s3_client: Optional pre-configured boto3 S3 client.
    :type s3_client: S3Client | None
    :param dlt_secrets: Optional dlt secrets dict used to construct an S3 client when ``s3_client`` is ``None``.
    :type dlt_secrets: Mapping[str, Any] | None
    :param use_destination: Destination block name for fallback S3 credential lookup.
    :type use_destination: str
    :return: Staged ``input_dir`` string suitable for ``Settings(input_dir=...)``.
    :rtype: str
    """
    if source_dir.is_file():
        rel_single = Path(input_subdir) / source_dir.name if input_subdir else Path(source_dir.name)
        file_pairs: list[tuple[Path, Path]] = [(source_dir, rel_single)]
    else:
        file_pairs = [
            (src_file, src_file.relative_to(source_dir))
            for src_file in sorted(source_dir.rglob("*"))
            if src_file.is_file()
        ]

    target_str = str(target_dir_or_url)
    if target_str.startswith("s3://"):
        parsed = urlparse(target_str)
        bucket = parsed.netloc
        prefix = parsed.path.strip("/")
        client = s3_client or _build_s3_client_from_secrets(dlt_secrets or {}, use_destination)
        for src_file, rel_path in file_pairs:
            rel_posix = rel_path.as_posix()
            key = f"{prefix}/{rel_posix}" if prefix else rel_posix
            client.put_object(Bucket=bucket, Key=key, Body=src_file.read_bytes())
        return f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}"

    dest_dir = Path(target_dir_or_url)
    dest_dir.mkdir(parents=True, exist_ok=True)
    for src_file, rel_path in file_pairs:
        dst_file = dest_dir / rel_path
        dst_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_file, dst_file)
    return str(dest_dir)
