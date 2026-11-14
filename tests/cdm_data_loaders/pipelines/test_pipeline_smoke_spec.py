"""Specification-driven smoke tests for CDM ingestion pipelines across environments.

Cases and rules live under ``specs/pipelines/<pipeline>/`` (``rules.yaml``, ``*/case.yaml``,
``*/settings.toml``, ``*/expected_tables.py``, and ``*/expected.json``). The same manifests
generate ``docs/<pipeline>_pipeline_specification.md`` via ``scripts/gen_pipeline_spec.py``.
"""

import os
import subprocess
from collections.abc import Generator
from pathlib import Path
from typing import Any
from uuid import uuid4

import boto3
import pytest
from _pytest.mark.structures import ParameterSet

from cdm_data_loaders.utils.file_transfer.s3.client import get_s3_client, reset_s3_client
from scripts.gen_converter_spec import PipelineSpecConfig
from scripts.gen_pipeline_spec import (
    PIPELINE_SPECS,
)
from scripts.gen_pipeline_spec import (
    main as gen_pipeline_spec_main,
)
from scripts.gen_pipeline_spec import (
    render as render_pipeline_spec,
)
from tests.cdm_data_loaders.pipelines import framework
from tests.cdm_data_loaders.pipelines.framework import (
    ExecutionMode,
    PipelineTestRunner,
    resolve_pipeline,
)
from tests.cdm_data_loaders.pipelines.helpers import (
    assert_smoke_run_output,
    canonicalize_table_rows,
    normalize_expected_table_rows,
)
from tests.conftest import (
    LOCAL_FS,
    S3,
)
from tests.converter_spec import RuleCatalogue, load_rules
from tests.dlt_config_isolation import isolated_dlt_config, load_dlt_toml_files
from tests.fs_backends import MOTO_ACCESS_KEY, MOTO_SECRET_KEY
from tests.helpers import make_cts_settings
from tests.pipeline_env import (
    PipelineEnvSpec,
    discover_env_settings_file,
    drop_iceberg_tables_for_dataset,
    list_iceberg_namespace_tables,
    load_pipeline_env_from_dir,
    parse_env_settings_doc,
    stage_input_directory,
)
from tests.pipeline_spec import (
    PipelineCase,
    PipelineRun,
    load_expected_json_tables,
    load_expected_tables,
    load_pipeline_cases,
    load_settings_file,
)

CATALOGUES_BY_FAMILY: dict[str, RuleCatalogue] = {
    name: load_rules(spec.data_dir / "rules.yaml") for name, spec in PIPELINE_SPECS.items()
}
CASES_BY_FAMILY: dict[str, list[PipelineCase]] = {
    name: load_pipeline_cases(spec.data_dir) for name, spec in PIPELINE_SPECS.items()
}
ALL_CASES: list[PipelineCase] = [case for cases in CASES_BY_FAMILY.values() for case in cases]
CASES_BY_PIPELINE: dict[str, list[PipelineCase]] = {
    pipeline_name: [c for c in ALL_CASES if c.pipeline == pipeline_name]
    for pipeline_name in {c.pipeline for c in ALL_CASES}
}

ALL_RUN_PARAMS: list[ParameterSet] = [
    pytest.param(case, run, id=f"{case.pipeline}-{case.name}-{run.id}") for case in ALL_CASES for run in case.runs
]
SMOKE_RUN_PARAMS: list[ParameterSet] = [
    pytest.param(case, run, id=f"{case.pipeline}-{case.name}-{run.id}")
    for case in ALL_CASES
    for run in case.runs
    if run.smoke
]


# Manifest, preset inheritance, and rule coverage invariants


@pytest.mark.parametrize("spec", list(PIPELINE_SPECS.values()), ids=lambda s: s.name)
def test_manifests_pass_preset_subclasses_inherit_base_settings_and_runner(spec: PipelineSpecConfig) -> None:
    """Every preset pipeline in a family subclasses the family's base Settings class and shares its pipeline_fn."""
    base_resolved = resolve_pipeline(spec.name)
    assert base_resolved.settings_cls is not None
    assert base_resolved.pipeline_fn is not None

    for preset_name in spec.presets:
        preset_resolved = resolve_pipeline(preset_name)
        assert preset_resolved.settings_cls is not None
        assert issubclass(preset_resolved.settings_cls, base_resolved.settings_cls)
        assert preset_resolved.settings_cls is not base_resolved.settings_cls
        assert preset_resolved.pipeline_fn is base_resolved.pipeline_fn


@pytest.mark.parametrize("family_name", sorted(PIPELINE_SPECS), ids=str)
def test_manifests_pass_every_case_rule_exists(family_name: str) -> None:
    """Every rule referenced by a pipeline case manifest is defined in that family's rules.yaml."""
    catalogue = CATALOGUES_BY_FAMILY[family_name]
    cases = CASES_BY_FAMILY[family_name]
    unknown = {f"{case.pipeline}/{case.name}": sorted(set(case.rules) - catalogue.rules.keys()) for case in cases}
    assert {name: ids for name, ids in unknown.items() if ids} == {}


@pytest.mark.parametrize("family_name", sorted(PIPELINE_SPECS), ids=str)
def test_manifests_pass_every_rule_is_exercised(family_name: str) -> None:
    """Every rule in rules.yaml is exercised by at least one case manifest in the family."""
    catalogue = CATALOGUES_BY_FAMILY[family_name]
    cases = CASES_BY_FAMILY[family_name]
    exercised = {rid for case in cases for rid in case.rules}
    unexercised = sorted(set(catalogue.rules) - exercised)
    assert unexercised == [], f"Rules in {family_name}/rules.yaml not exercised by any case: {unexercised}"


@pytest.mark.parametrize("case", ALL_CASES, ids=lambda c: f"{c.pipeline}-{c.name}")
def test_manifests_pass_every_case_file_exists(case: PipelineCase) -> None:
    """Every input path, settings file, expected_tables.py, and expected.json exists on disk."""
    assert case.runs, f"{case.pipeline}/{case.name} has no runs"
    for run in case.runs:
        assert run.input_dir.exists(), f"Missing input_dir: {run.input_dir}"
        if run.input_dir.is_dir():
            assert any(p.is_file() for p in run.input_dir.rglob("*")), f"Empty input_dir: {run.input_dir}"
        if run.settings_file is not None:
            assert run.settings_file.is_file(), f"Missing settings_file: {run.settings_file}"
        if run.expected_tables is not None:
            assert run.expected_tables.is_file(), f"Missing expected_tables: {run.expected_tables}"
        if run.expected_json is not None:
            assert run.expected_json.is_file(), f"Missing expected_json: {run.expected_json}"


@pytest.mark.parametrize(("case", "run"), ALL_RUN_PARAMS)
def test_manifests_pass_expected_tables_validate(case: PipelineCase, run: PipelineRun) -> None:
    """Every expected PyArrow table is valid and matches expected_counts and expected.json."""
    assert case.family in PIPELINE_SPECS
    assert run.expected_tables is not None
    assert run.expected_json is not None

    tables = load_expected_tables(run)
    for table in tables.values():
        table.validate(full=True)

    assert {name: table.num_rows for name, table in tables.items()} == run.expected_counts

    expected_json_data = load_expected_json_tables(run)
    normalized_from_tables = {name: normalize_expected_table_rows(table) for name, table in sorted(tables.items())}
    normalized_from_json = {name: canonicalize_table_rows(rows) for name, rows in sorted(expected_json_data.items())}
    assert normalized_from_tables == normalized_from_json


# Documentation sync tests


@pytest.mark.docs
@pytest.mark.parametrize("spec", list(PIPELINE_SPECS.values()), ids=lambda s: s.name)
def test_spec_docs_pass_up_to_date_with_manifests(spec: PipelineSpecConfig) -> None:
    """docs/<family>_pipeline_specification.md matches the output of scripts/gen_pipeline_spec.py."""
    assert spec.output_path.is_file(), (
        f"{spec.output_path} does not exist; run `uv run python -m scripts.gen_pipeline_spec`."
    )
    assert spec.output_path.read_text(encoding="utf-8") == render_pipeline_spec(spec), (
        f"{spec.output_path.name} is out of date; run `uv run python -m scripts.gen_pipeline_spec`."
    )
    assert gen_pipeline_spec_main(["--check", "--pipeline", spec.name]) == 0


@pytest.mark.docs
def test_gen_pipeline_spec_main_check_fail_when_stale(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """gen_pipeline_spec.main --check returns 1 when the output file is missing or stale, and 0 after writing."""
    out_file = tmp_path / "xmltopyarrow_pipeline_specification.md"
    temp_spec = PipelineSpecConfig(
        name="xmltopyarrow",
        data_dir=PIPELINE_SPECS["xmltopyarrow"].data_dir,
        output_path=out_file,
        presets=PIPELINE_SPECS["xmltopyarrow"].presets,
    )
    monkeypatch.setitem(PIPELINE_SPECS, "xmltopyarrow", temp_spec)

    assert gen_pipeline_spec_main(["--check", "--pipeline", "xmltopyarrow"]) == 1
    assert "is out of date" in capsys.readouterr().err

    assert gen_pipeline_spec_main(["--pipeline", "xmltopyarrow"]) == 0
    assert out_file.is_file()
    assert gen_pipeline_spec_main(["--check", "--pipeline", "xmltopyarrow"]) == 0


# Built-in and user-supplied smoke test environments


BUILTIN_ENV_PARAMS: tuple[ParameterSet, ...] = (
    pytest.param("local_fs", id="env-local_fs"),
    pytest.param("s3_moto", id="env-s3_moto"),
    pytest.param("s3_ceph", id="env-s3_ceph", marks=pytest.mark.requires_ceph),
)


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Parametrize smoke_env_kind over built-in environments unless --dlt-config-dir or --settings-file is set."""
    if "smoke_env_kind" in metafunc.fixturenames:
        dlt_config_dir = metafunc.config.getoption("--dlt-config-dir")
        settings_file = metafunc.config.getoption("--settings-file")
        if dlt_config_dir or settings_file:
            metafunc.parametrize("smoke_env_kind", [pytest.param("custom", id="env-custom")])
        else:
            metafunc.parametrize("smoke_env_kind", BUILTIN_ENV_PARAMS)


@pytest.fixture
def smoke_env(
    smoke_env_kind: str,
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[PipelineEnvSpec]:
    """Provide a PipelineEnvSpec for local_fs, s3_moto, s3_ceph, or a custom --dlt-config-dir."""
    if smoke_env_kind == "custom":
        cfg_dir_opt = request.config.getoption("--dlt-config-dir")
        settings_file_opt = request.config.getoption("--settings-file")
        yield load_pipeline_env_from_dir(
            Path(cfg_dir_opt) if cfg_dir_opt else None,
            settings_file_opt,
        )
        return

    if smoke_env_kind == "local_fs":
        local_bucket = tmp_path / "local_fs_bucket"
        local_bucket.mkdir(parents=True, exist_ok=True)
        yield PipelineEnvSpec(
            name="local_fs",
            use_destination=LOCAL_FS,
            dlt_config={
                "destination": {
                    LOCAL_FS: {
                        "destination_type": "filesystem",
                        "bucket_url": str(local_bucket),
                    }
                },
                "normalize": {"data_writer": {"disable_compression": False}},
            },
            dlt_secrets={},
            preserve_dlt_destination=True,
        )
        return

    if smoke_env_kind == "s3_moto":
        endpoint_url = request.getfixturevalue("s3_moto_endpoint")
        creds = {
            "aws_access_key_id": MOTO_ACCESS_KEY,
            "aws_secret_access_key": MOTO_SECRET_KEY,
            "endpoint_url": endpoint_url,
        }
    else:
        creds = {
            "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "endpoint_url": os.environ["AWS_ENDPOINT_URL"],
        }

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", creds["aws_access_key_id"])
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", creds["aws_secret_access_key"])
    monkeypatch.setenv("AWS_ENDPOINT_URL", creds["endpoint_url"])
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setattr(boto3, "DEFAULT_SESSION", None)
    reset_s3_client()
    try:
        client = get_s3_client(creds)
        in_bucket = f"smoke-in-{uuid4().hex[:16]}"
        out_bucket = f"smoke-out-{uuid4().hex[:16]}"
        client.create_bucket(Bucket=in_bucket)
        client.create_bucket(Bucket=out_bucket)
        s3_cfg = {
            "destination": {
                S3: {
                    "destination_type": "filesystem",
                    "bucket_url": f"s3://{out_bucket}",
                }
            },
            "normalize": {"data_writer": {"disable_compression": False}},
        }
        s3_sec = {
            "sources": {"filesystem": {"credentials": dict(creds)}},
            "destination": {S3: {"credentials": dict(creds)}},
        }
        yield PipelineEnvSpec(
            name=smoke_env_kind,
            use_destination=S3,
            dlt_config=s3_cfg,
            dlt_secrets=s3_sec,
            preserve_dlt_destination=True,
            s3_client=client,
            s3_input_bucket=in_bucket,
        )
    finally:
        reset_s3_client()


def _known_pipeline_tables(pipeline_name: str) -> set[str]:
    """Return all table names referenced across expected_counts for ``pipeline_name``."""
    return {
        table_name
        for case in CASES_BY_PIPELINE.get(pipeline_name, [])
        for run in case.runs
        for table_name in run.expected_counts
    }


def execute_smoke_case_run(
    case: PipelineCase,
    run: PipelineRun,
    env: PipelineEnvSpec,
    execution_mode: ExecutionMode,
    tmp_path: Path,
    *,
    docker_image: str | None = None,
    drop_existing_tables: bool = True,
    drop_tables_after: bool = True,
) -> None:
    """Stage input files, merge case and environment settings, run the pipeline, and verify all outputs."""
    if execution_mode == ExecutionMode.DOCKER and env.name == "s3_moto":
        pytest.skip("s3_moto runs in-process and is not reachable from a Docker container")

    spec = resolve_pipeline(case.pipeline)
    assert spec.settings_cls is not None

    run_slug = f"{case.pipeline}_{case.name}_{run.id}_{uuid4().hex[:8]}"

    env_overrides = env.settings_for_pipeline(case.pipeline, spec.settings_cls)
    merged_kwargs: dict[str, Any] = {
        "use_destination": env.use_destination,
        "dlt_dev_mode": False,
        "use_output_dir_for_pipeline_metadata": True,
        "log_config_file": None,
        **run.settings,
        **env_overrides,
    }

    # Resolve per-run isolated input_dir and output_dir
    if env.s3_input_bucket is not None:
        target_input: str | Path = f"s3://{env.s3_input_bucket}/{run_slug}/input"
    elif env_overrides.get("input_dir"):
        base_in = str(env_overrides["input_dir"]).rstrip("/")
        target_input = f"{base_in}/{run_slug}/input" if "://" in base_in else Path(base_in) / run_slug / "input"
    else:
        target_input = tmp_path / run_slug / "input"

    staged_input_dir = stage_input_directory(
        run.input_dir,
        target_input,
        input_subdir=run.input_subdir,
        s3_client=env.s3_client,
        dlt_secrets=env.dlt_secrets,
        use_destination=env.use_destination,
    )

    if env_overrides.get("output_dir"):
        run_output_dir = Path(str(env_overrides["output_dir"])) / run_slug / "output"
    else:
        run_output_dir = tmp_path / run_slug / "output"
    run_output_dir.mkdir(parents=True, exist_ok=True)

    merged_kwargs["input_dir"] = staged_input_dir
    merged_kwargs["output_dir"] = str(run_output_dir)

    settings = make_cts_settings(
        spec.settings_cls,
        tmp_path / run_slug,
        **merged_kwargs,
    )

    runner = PipelineTestRunner(
        mode=execution_mode,
        docker_image=docker_image,
        dlt_config_dir=env.dlt_config_dir,
        preserve_dlt_destination=env.preserve_dlt_destination
        and not (execution_mode == ExecutionMode.DOCKER and env.name == "local_fs"),
    )

    should_drop_existing = env.should_drop_existing_tables(case.pipeline, cli_flag=drop_existing_tables)
    should_drop_after = env.should_drop_tables_after(case.pipeline, cli_flag=drop_tables_after)
    dataset_name = str(getattr(settings, "dataset_name", None) or case.pipeline)
    is_iceberg = str(getattr(settings, "loader_file_format", "")) == "iceberg"
    dlt_cfg, dlt_sec = env.isolated_dlt_configs(run_slug, isolate_bucket_url=not is_iceberg)

    with isolated_dlt_config(config=dlt_cfg, secrets=dlt_sec):
        if is_iceberg and should_drop_existing:
            drop_iceberg_tables_for_dataset(
                dataset_name,
                _known_pipeline_tables(case.pipeline),
                include_namespace_tables=True,
            )
        pre_existing_tables = (
            list_iceberg_namespace_tables(dataset_name) if (is_iceberg and should_drop_after) else set()
        )
        try:
            load_info = runner.run(case.pipeline, settings=settings)
            assert_smoke_run_output(load_info, settings, run)
        finally:
            if is_iceberg and should_drop_after:
                current_tables = list_iceberg_namespace_tables(dataset_name)
                added_tables = (current_tables | set(run.expected_counts)) - pre_existing_tables
                drop_iceberg_tables_for_dataset(
                    dataset_name,
                    added_tables,
                    include_namespace_tables=False,
                )


@pytest.mark.parametrize(("case", "run"), SMOKE_RUN_PARAMS)
def test_pipeline_smoke_spec_pass_end_to_end(
    case: PipelineCase,
    run: PipelineRun,
    smoke_env: PipelineEnvSpec,
    execution_mode: ExecutionMode,
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> None:
    """Execute each pipeline smoke test case across configured environments and execution modes."""
    smoke_filter = request.config.getoption("--smoke-pipeline")
    if smoke_filter and smoke_filter not in {case.pipeline, case.family}:
        pytest.skip(f"Filtered to --smoke-pipeline={smoke_filter}")

    image_tag = request.config.getoption("--docker-image") if execution_mode == ExecutionMode.DOCKER else None
    drop_existing = bool(request.config.getoption("--drop-existing-tables"))
    drop_after = bool(request.config.getoption("--drop-tables-after"))
    execute_smoke_case_run(
        case,
        run,
        smoke_env,
        execution_mode,
        tmp_path,
        docker_image=image_tag,
        drop_existing_tables=drop_existing,
        drop_tables_after=drop_after,
    )


# Environment config file and TOML loading tests


@pytest.mark.parametrize("mode", [ExecutionMode.CORE, ExecutionMode.CLI], ids=str)
def test_custom_dlt_config_dir_with_settings_toml_pass_end_to_end(
    tmp_path: Path,
    mode: ExecutionMode,
) -> None:
    """A custom --dlt-config-dir with config.toml, secrets.toml, and settings.toml runs pubmed and ncbi_datasets."""
    conf_dir = tmp_path / "env_conf"
    conf_dir.mkdir()
    bucket_root = tmp_path / "configured_bucket"
    bucket_root.mkdir()
    output_root = tmp_path / "configured_output"
    output_root.mkdir()

    (conf_dir / "config.toml").write_text(
        f'[destination.env_fs]\ndestination_type = "filesystem"\nbucket_url = "{bucket_root.as_posix()}"\n',
        encoding="utf-8",
    )
    (conf_dir / "secrets.toml").write_text("", encoding="utf-8")
    (conf_dir / "settings.toml").write_text(
        'use_destination = "env_fs"\n'
        'loader_file_format = "parquet"\n'
        f'output_dir = "{output_root.as_posix()}"\n'
        "use_output_dir_for_pipeline_metadata = true\n\n"
        "[pipelines.pubmed]\n"
        "buffer_size = 50\n\n"
        "[pipelines.ncbi_datasets]\n"
        "buffer_size = 25\n",
        encoding="utf-8",
    )

    env = load_pipeline_env_from_dir(conf_dir)
    assert env.name == "env_conf"
    assert env.use_destination == "env_fs"
    assert env.preserve_dlt_destination is True
    assert env.settings_file == (conf_dir / "settings.toml").resolve()

    for pipeline_name in ("pubmed", "ncbi_datasets"):
        case = CASES_BY_PIPELINE[pipeline_name][0]
        run = case.runs[0]
        execute_smoke_case_run(case, run, env, mode, tmp_path)


def test_load_settings_file_and_env_parsing_pass_formats_and_filtering(tmp_path: Path) -> None:
    """load_settings_file supports TOML, YAML, and JSON, and settings_for_pipeline filters ClassVars and rejects unknown keys."""
    toml_file = tmp_path / "settings.toml"
    toml_file.write_text(
        'use_destination = "s3"\ndataset_name = "ignored_on_preset"\n[pipelines.pubmed]\nbuffer_size = 10\n',
        encoding="utf-8",
    )
    yaml_file = tmp_path / "settings.yaml"
    yaml_file.write_text("loader_file_format: iceberg\npubmed:\n  buffer_size: 20\n", encoding="utf-8")
    json_file = tmp_path / "settings.json"
    json_file.write_text('{"loader_file_format": "jsonl"}', encoding="utf-8")

    assert load_settings_file(toml_file)["use_destination"] == "s3"
    assert load_settings_file(yaml_file)["loader_file_format"] == "iceberg"
    assert load_settings_file(json_file)["loader_file_format"] == "jsonl"

    with pytest.raises(ValueError, match="Unsupported settings file extension"):
        load_settings_file(tmp_path / "bad.ini")

    global_ov, pipe_ov = parse_env_settings_doc(load_settings_file(toml_file))
    assert global_ov == {"use_destination": "s3", "dataset_name": "ignored_on_preset"}
    assert pipe_ov == {"pubmed": {"buffer_size": 10}}

    pubmed_spec = resolve_pipeline("pubmed")
    assert pubmed_spec.settings_cls is not None
    env = PipelineEnvSpec(
        name="test_env",
        use_destination="s3",
        settings_overrides=global_ov,
        pipeline_settings_overrides=pipe_ov,
    )
    resolved = env.settings_for_pipeline("pubmed", pubmed_spec.settings_cls)
    # dataset_name is a ClassVar on PubMedXmlToPyArrowSettings and is filtered out cleanly
    assert resolved == {"use_destination": "s3", "buffer_size": 10}

    bad_env = PipelineEnvSpec(
        name="bad_env",
        use_destination="s3",
        settings_overrides={"nonexistent_field": 123},
    )
    with pytest.raises(ValueError, match="Unknown field 'nonexistent_field'"):
        bad_env.settings_for_pipeline("pubmed", pubmed_spec.settings_cls)


def test_discover_env_settings_file_and_load_dlt_toml_files_fail_missing(tmp_path: Path) -> None:
    """discover_env_settings_file and load_dlt_toml_files raise FileNotFoundError for missing paths."""
    with pytest.raises(FileNotFoundError, match="dlt config directory does not exist"):
        load_dlt_toml_files(tmp_path / "missing_dir")

    with pytest.raises(FileNotFoundError, match="Settings config file does not exist"):
        discover_env_settings_file(None, tmp_path / "missing_settings.toml")


def test_ncbi_datasets_iceberg_with_home_pyiceberg_yaml_and_table_cleanup_pass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ncbi_datasets runs with loader_file_format=iceberg using $HOME/.pyiceberg.yaml, ignoring --dlt-config-dir/.pyiceberg.yaml and supporting table cleanup."""
    fake_home = tmp_path / "fake_home"
    fake_home.mkdir()
    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.delenv("PYICEBERG_HOME", raising=False)

    catalog_db = tmp_path / "polaris_sim_catalog.db"
    warehouse_dir = tmp_path / "warehouse"
    warehouse_dir.mkdir()
    (fake_home / ".pyiceberg.yaml").write_text(
        "catalog:\n"
        "  default:\n"
        "    type: sql\n"
        f"    uri: sqlite:///{catalog_db.as_posix()}\n"
        f"    warehouse: file://{warehouse_dir.as_posix()}\n",
        encoding="utf-8",
    )

    conf_dir = tmp_path / "dlt_conf"
    conf_dir.mkdir()
    # A bogus .pyiceberg.yaml inside --dlt-config-dir must be ignored
    (conf_dir / ".pyiceberg.yaml").write_text(
        "catalog:\n  default:\n    type: rest\n    uri: http://invalid-ignored-host:9999\n",
        encoding="utf-8",
    )
    (conf_dir / "config.toml").write_text(
        f'[destination.env_fs]\ndestination_type = "filesystem"\nbucket_url = "{warehouse_dir.as_posix()}"\n',
        encoding="utf-8",
    )
    (conf_dir / "settings.toml").write_text(
        'use_destination = "env_fs"\n'
        'loader_file_format = "iceberg"\n'
        "drop_existing_tables = true\n"
        "drop_tables_after = false\n",
        encoding="utf-8",
    )

    env = load_pipeline_env_from_dir(conf_dir)
    assert env.drop_existing_tables is True
    assert env.drop_tables_after is False

    ncbi_cases = CASES_BY_PIPELINE["ncbi_datasets"]
    # Run Case 1 and Case 2 consecutively against the same persistent catalog with drop_existing_tables=True
    for case in ncbi_cases[:2]:
        execute_smoke_case_run(case, case.runs[0], env, ExecutionMode.CORE, tmp_path)

    # Because drop_tables_after=False, Case 2's tables remain registered in the catalog
    with isolated_dlt_config(config=env.dlt_config, secrets=env.dlt_secrets):
        remaining = list_iceberg_namespace_tables("ncbi_datasets")
    assert {"annotation_report", "dataset_report"} <= remaining

    # Now run with drop_tables_after=True and verify added tables are removed on completion
    execute_smoke_case_run(
        ncbi_cases[0],
        ncbi_cases[0].runs[0],
        env,
        ExecutionMode.CORE,
        tmp_path,
        drop_existing_tables=True,
        drop_tables_after=True,
    )
    with isolated_dlt_config(config=env.dlt_config, secrets=env.dlt_secrets):
        assert list_iceberg_namespace_tables("ncbi_datasets") == set()


def test_pipeline_test_runner_docker_mode_copies_dlt_config_dir_and_preserves_remote_input(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Docker mode copies config.toml and secrets.toml from dlt_config_dir, stages $HOME/.pyiceberg.yaml, and preserves s3:// input_dir URLs."""
    fake_home = tmp_path / "fake_home"
    fake_home.mkdir()
    monkeypatch.setenv("HOME", str(fake_home))
    (fake_home / ".pyiceberg.yaml").write_text("catalog:\n  default:\n    type: rest\n", encoding="utf-8")

    conf_dir = tmp_path / "dlt_conf_src"
    conf_dir.mkdir()
    (conf_dir / "config.toml").write_text('[destination.s3]\ndestination_type = "filesystem"\n', encoding="utf-8")
    (conf_dir / "secrets.toml").write_text('[destination.s3.credentials]\naws_access_key_id = "k"\n', encoding="utf-8")
    (conf_dir / ".pyiceberg.yaml").write_text("ignored: true\n", encoding="utf-8")

    output_dir = tmp_path / "out"
    output_dir.mkdir()

    monkeypatch.setattr(framework, "_built_docker_images", {"test-image:latest"})
    captured_cmds: list[list[str]] = []

    def fake_docker_run(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        captured_cmds.append(cmd)
        (output_dir / "report.txt").write_text(
            "pipeline: pubmed\n"
            "dataset: pubmed\n"
            "started: 2026-02-01 00:00:00+00:00\n"
            "finished: 2026-02-01 00:00:05+00:00\n"
            "status: completed\n"
            "load_ids:\n"
            "  - '101'\n"
            "pipeline_load_info_errs: null\n",
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(cmd, returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_docker_run)
    runner = PipelineTestRunner(
        mode=ExecutionMode.DOCKER,
        docker_image="test-image:latest",
        dlt_config_dir=conf_dir,
        preserve_dlt_destination=True,
    )
    with isolated_dlt_config(
        config={"destination": {"s3": {"destination_type": "filesystem", "bucket_url": str(output_dir)}}},
    ):
        load_info = runner.run(
            "pubmed",
            cli_args=[
                "--input-dir",
                "s3://remote-bucket/pubmed-in",
                "--output-dir",
                str(output_dir),
                "--use-destination",
                "s3",
            ],
        )

    assert load_info is not None
    assert len(captured_cmds) == 1
    docker_cmd = captured_cmds[0]
    idx_in = docker_cmd.index("--input-dir")
    assert docker_cmd[idx_in + 1] == "s3://remote-bucket/pubmed-in"

    stage_dirs = sorted(output_dir.parent.glob(".docker_stage_*"))
    staged_dlt_conf = stage_dirs[-1] / "input_dir" / "dlt_conf"
    assert (staged_dlt_conf / "config.toml").read_text(encoding="utf-8") == (
        '[destination.s3]\ndestination_type = "filesystem"\n'
    )
    assert (staged_dlt_conf / "secrets.toml").read_text(encoding="utf-8") == (
        '[destination.s3.credentials]\naws_access_key_id = "k"\n'
    )
    assert (staged_dlt_conf / ".pyiceberg.yaml").read_text(encoding="utf-8") == (
        "catalog:\n  default:\n    type: rest\n"
    )
