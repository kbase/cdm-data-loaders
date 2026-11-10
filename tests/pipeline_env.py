"""Environment configuration and input staging for manifest-driven pipeline smoke tests.

Supports both built-in pytest test environments (``local_fs``, ``s3_moto``, ``s3_ceph``) and
external environment directories supplied via ``--dlt-config-dir`` / ``--settings-file``
containing ``config.toml``, ``secrets.toml``, and ``settings.toml`` (or ``.yaml`` / ``.json``).
"""

import shutil
from collections.abc import Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final
from urllib.parse import urlparse

import boto3
from dlt.common.libs.pyiceberg import drop_iceberg_table, get_catalog, is_ephemeral_catalog
from pydantic_settings import BaseSettings
from pyiceberg.exceptions import NoSuchNamespaceError

from tests.conftest import LOCAL_FS
from tests.dlt_config_isolation import load_dlt_toml_files
from tests.pipeline_spec import load_settings_file

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

    :param config_dir: optional directory passed via ``--dlt-config-dir``
    :type config_dir: Path | None
    :param explicit_settings_file: optional explicit path passed via ``--settings-file``
    :type explicit_settings_file: Path | str | None
    :raises FileNotFoundError: if ``explicit_settings_file`` does not exist
    :return: resolved ``Path`` to the settings file, or ``None`` if none is present
    :rtype: Path | None
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

    Supports a ``[pipelines.<pipeline_name>]`` table/section as well as top-level settings fields.
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
    """Complete execution environment configuration for a pipeline smoke test."""

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
    s3_client: Any = None
    s3_input_bucket: str | None = None

    def should_drop_existing_tables(self, pipeline_name: str, *, cli_flag: bool = False) -> bool:
        """Return whether existing tables for ``pipeline_name`` should be dropped before running."""
        pipe_cfg = self.pipeline_settings_overrides.get(pipeline_name, {})
        if "drop_existing_tables" in pipe_cfg:
            return bool(pipe_cfg["drop_existing_tables"])
        return bool(cli_flag or self.drop_existing_tables)

    def should_drop_tables_after(self, pipeline_name: str, *, cli_flag: bool = False) -> bool:
        """Return whether tables added by ``pipeline_name`` should be dropped after the test finishes."""
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

        If ``settings_cls`` is provided, validates that every key is a recognized field or
        class attribute on ``settings_cls`` and filters out ``ClassVar`` attributes and
        runner-level test options (``drop_existing_tables``, ``drop_tables_after``) so the
        returned dict can be passed directly to ``settings_cls(...)``.
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
                # ClassVar on preset settings class (e.g., dataset_name on PubMedXmlToPyArrowSettings)
                continue
            else:
                msg = f"Unknown field {key!r} for {settings_cls.__name__} in environment {self.name!r}"
                raise ValueError(msg)
        return filtered

    def isolated_dlt_configs(self, run_slug: str) -> tuple[dict[str, Any], dict[str, Any]]:
        """Return deep-copied ``(dlt_config, dlt_secrets)`` with an isolated per-run ``bucket_url`` if configured."""
        cfg = deepcopy(self.dlt_config)
        sec = deepcopy(self.dlt_secrets)
        if not self.preserve_dlt_destination:
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

    Any ``.pyiceberg.yaml`` file present inside ``config_dir`` is ignored; PyIceberg configuration
    must reside in ``$HOME/.pyiceberg.yaml``.
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
    """Return table names (without namespace prefix) currently registered in ``dataset_name`` in the active Iceberg catalog."""
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


def drop_iceberg_tables_for_dataset(
    dataset_name: str,
    table_names: Iterable[str] | None = None,
    *,
    include_namespace_tables: bool = True,
) -> list[str]:
    """Drop tables in ``dataset_name`` from the active Iceberg catalog and return dropped table names."""
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
        return [
            table_name
            for table_name in sorted(targets)
            if drop_iceberg_table(catalog, f"{dataset_name}.{table_name}", purge=True)
        ]
    finally:
        if hasattr(catalog, "close"):
            catalog.close()


def _build_s3_client_from_secrets(
    dlt_secrets: Mapping[str, Any],
    use_destination: str,
) -> Any:  # noqa: ANN401
    """Construct a boto3 S3 client from dlt secrets if credentials are present."""
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
    if "aws_access_key_id" in creds:
        kwargs["aws_access_key_id"] = creds["aws_access_key_id"]
    if "aws_secret_access_key" in creds:
        kwargs["aws_secret_access_key"] = creds["aws_secret_access_key"]
    if "endpoint_url" in creds:
        kwargs["endpoint_url"] = creds["endpoint_url"]
    if "region_name" in creds:
        kwargs["region_name"] = creds["region_name"]
    return boto3.client("s3", **kwargs)


def stage_input_directory(
    source_dir: Path,
    target_dir_or_url: str | Path,
    *,
    input_subdir: str | None = None,
    s3_client: Any = None,  # noqa: ANN401
    dlt_secrets: Mapping[str, Any] | None = None,
    use_destination: str = LOCAL_FS,
) -> str:
    """Copy all files in ``source_dir`` (or a single file) to a local directory or ``s3://`` URL prefix.

    :param source_dir: source fixture directory or single file
    :type source_dir: Path
    :param target_dir_or_url: local directory path or ``s3://bucket/prefix`` URL
    :type target_dir_or_url: str | Path
    :param input_subdir: optional subdirectory name under ``target_dir_or_url`` when staging a single file
    :type input_subdir: str | None
    :param s3_client: optional pre-configured boto3 S3 client
    :type s3_client: Any
    :param dlt_secrets: optional dlt secrets dict used to construct an S3 client when ``s3_client`` is None
    :type dlt_secrets: Mapping[str, Any] | None
    :param use_destination: destination block name for fallback S3 credential lookup
    :type use_destination: str
    :return: staged ``input_dir`` string suitable for ``Settings(input_dir=...)``
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
