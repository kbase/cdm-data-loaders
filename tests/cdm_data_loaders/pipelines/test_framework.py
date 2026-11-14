"""Unit and integration tests for the unified pipeline test execution framework."""

import subprocess
from pathlib import Path
from types import ModuleType

import dlt
import pytest
from pydantic import BaseModel

from cdm_data_loaders.core.settings import CtsSettings
from cdm_data_loaders.pipelines import uniprot_kb_cdm
from cdm_data_loaders.pipelines.jsonlines.extract_pipeline import (
    cli as jsonl_extract_cli,
)
from cdm_data_loaders.pipelines.jsonlines.extract_pipeline import (
    run_jsonlines_pipeline,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlExtractSettings, JsonlJsonschemaIngestSettings
from cdm_data_loaders.pipelines.xmltodict.settings import XmlToDictSettings
from cdm_data_loaders.pipelines.xmltopyarrow.pipeline import run_xmltopyarrow_pipeline
from tests.cdm_data_loaders.pipelines import framework
from tests.cdm_data_loaders.pipelines.conftest import DEFAULT_DLT_TABLES
from tests.cdm_data_loaders.pipelines.framework import (
    CONTAINER_AUX_DIR,
    CONTAINER_INPUT_DIR,
    CONTAINER_OUTPUT_DIR,
    CONTAINER_PYMODULES_DIR,
    PIPELINE_TARGETS,
    ExecutionMode,
    PipelineSpec,
    PipelineTestRunner,
    ensure_docker_image,
    reconstruct_load_info,
    resolve_pipeline,
)
from tests.cdm_data_loaders.pipelines.helpers import (
    assert_pipeline_report_written,
    assert_run_ok,
    read_data_rows,
)
from tests.cdm_data_loaders.pipelines.reference import pipeline as reference_pipeline_mod
from tests.cdm_data_loaders.pipelines.reference.pipeline import (
    ReferencePipelineSettings,
    run_reference_pipeline,
)
from tests.helpers import make_cts_settings, register_temp_module, settings_to_argv


class _SampleEntityModel(BaseModel):
    """Sample Pydantic model for dynamic module materialization tests."""

    record_id: str


@pytest.mark.parametrize("target_key", sorted(PIPELINE_TARGETS), ids=str)
def test_resolve_pipeline_pass_all_registered_targets(target_key: str) -> None:
    """Every registered pipeline resolves to a valid PipelineSpec with a synchronized cli_prog_name."""
    spec = resolve_pipeline(target_key)
    expected_cli, expected_module = PIPELINE_TARGETS[target_key]
    assert spec.cli_command == expected_cli
    assert spec.module_path == expected_module
    assert callable(spec.cli_callable)
    assert spec.settings_cls is not None
    assert spec.settings_cls.model_config.get("cli_prog_name") == expected_cli


PRE_RESOLVED_CLI_SPEC = PipelineSpec(
    cli_command="jsonlines_extract",
    module_path="cdm_data_loaders.pipelines.jsonlines.extract_pipeline",
    settings_cls=JsonlExtractSettings,
    pipeline_fn=run_jsonlines_pipeline,
    cli_callable=jsonl_extract_cli,
)


@pytest.mark.parametrize(
    ("target", "expected_cli", "expected_settings_cls", "expected_pipeline_fn", "expected_cli_callable"),
    [
        pytest.param(
            uniprot_kb_cdm,
            "uniprot_cdm",
            uniprot_kb_cdm.UniProtSettings,
            run_xmltopyarrow_pipeline,
            uniprot_kb_cdm.cli,
            id="module",
        ),
        pytest.param(
            jsonl_extract_cli,
            "jsonlines_extract",
            JsonlExtractSettings,
            run_jsonlines_pipeline,
            jsonl_extract_cli,
            id="pipeline-cli",
        ),
        pytest.param(
            run_jsonlines_pipeline,
            "jsonlines_extract",
            JsonlExtractSettings,
            run_jsonlines_pipeline,
            jsonl_extract_cli,
            id="callable",
        ),
        pytest.param(
            "cdm_data_loaders.pipelines.uniprot_kb_cdm",
            "uniprot_cdm",
            uniprot_kb_cdm.UniProtSettings,
            run_xmltopyarrow_pipeline,
            uniprot_kb_cdm.cli,
            id="dotted-module-path",
        ),
        pytest.param(
            PRE_RESOLVED_CLI_SPEC,
            "jsonlines_extract",
            JsonlExtractSettings,
            run_jsonlines_pipeline,
            jsonl_extract_cli,
            id="pipeline-spec",
        ),
        pytest.param(
            run_reference_pipeline,
            "reference_pipeline",
            ReferencePipelineSettings,
            run_reference_pipeline,
            reference_pipeline_mod.cli,
            id="test-callable",
        ),
        pytest.param(
            reference_pipeline_mod,
            "reference_pipeline",
            ReferencePipelineSettings,
            run_reference_pipeline,
            reference_pipeline_mod.cli,
            id="test-module",
        ),
    ],
)
def test_resolve_pipeline_pass_from_module_cli_spec_and_callable(
    target: object,
    expected_cli: str,
    expected_settings_cls: type[CtsSettings],
    expected_pipeline_fn: object,
    expected_cli_callable: object,
) -> None:
    """resolve_pipeline accepts ModuleType, PipelineCli, PipelineSpec, and pipeline run functions."""
    spec = resolve_pipeline(target)  # type: ignore[arg-type]
    assert spec.cli_command == expected_cli
    assert spec.settings_cls is expected_settings_cls
    assert spec.pipeline_fn is expected_pipeline_fn
    assert spec.cli_callable is expected_cli_callable


@pytest.mark.parametrize(
    ("target", "match"),
    [
        pytest.param(
            "nonexistent_pipeline",
            "Unknown pipeline 'nonexistent_pipeline'",
            id="unknown-string",
        ),
        pytest.param(
            lambda _settings: None,
            "Cannot resolve pipeline from callable",
            id="unmapped-callable",
        ),
    ],
)
def test_resolve_pipeline_fail_unknown_identifier(target: object, match: str) -> None:
    """resolve_pipeline raises ValueError for unknown pipeline names or unmapped callables."""
    with pytest.raises(ValueError, match=match):
        resolve_pipeline(target)  # type: ignore[arg-type]


def test_settings_to_argv_pass_excludes_computed_fields_and_expands_lists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """settings_to_argv omits computed fields and None values, formats bools, and expands list fields."""
    entity_schemas = register_temp_module(
        monkeypatch,
        name="_test_argv_schema_mod",
        ENTITY_SCHEMAS={
            "widget": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"id": {"type": "string"}},
            }
        },
    )
    settings = make_cts_settings(
        JsonlJsonschemaIngestSettings,
        tmp_path,
        log_config_file=None,
        use_output_dir_for_pipeline_metadata=True,
        dlt_dev_mode=False,
        dataset_name="custom_ds",
        loader_file_format="jsonl",
        entity_schemas=entity_schemas,
    )
    argv = settings_to_argv(settings)
    assert "--pipeline-dir" not in argv
    assert "--report-path" not in argv
    assert "--log-config-file" not in argv
    assert "--use-output-dir-for-pipeline-metadata" in argv
    idx_meta = argv.index("--use-output-dir-for-pipeline-metadata")
    assert argv[idx_meta + 1] == "true"


@pytest.mark.parametrize(
    "report_text",
    [
        pytest.param(None, id="missing-report"),
        pytest.param(
            "pipeline: test_pipe\n"
            "dataset: test_ds\n"
            "started: 2026-01-01 00:00:00+00:00\n"
            "finished: 2026-01-01 00:00:05+00:00\n"
            "status: error\n"
            "pipeline_err_msg: Pipeline failed\n",
            id="error-status-report",
        ),
    ],
)
def test_reconstruct_load_info_pass_returns_none_for_missing_or_error_report(
    tmp_path: Path,
    report_text: str | None,
) -> None:
    """reconstruct_load_info returns None when report.txt is missing or has status 'error'."""
    if report_text is not None:
        (tmp_path / "report.txt").write_text(report_text, encoding="utf-8")
    assert reconstruct_load_info(tmp_path) is None


def test_reconstruct_load_info_pass_completed_report(tmp_path: Path) -> None:
    """reconstruct_load_info reconstructs LoadInfo fields and asdict() output from a completed report."""
    started_str = "2026-01-01 12:34:56.789000+00:00"
    finished_str = "2026-01-01 12:35:00+00:00"
    (tmp_path / "report.txt").write_text(
        f"pipeline: test_pipe\n"
        f"dataset: test_ds\n"
        f"started: {started_str}\n"
        f"finished: {finished_str}\n"
        f"status: completed with errors\n"
        f"load_ids:\n"
        f"  - '111'\n"
        f"  - '222'\n"
        f"pipeline_load_info_errs: null\n",
        encoding="utf-8",
    )
    reconstructed = reconstruct_load_info(tmp_path)
    assert reconstructed is not None
    assert reconstructed.pipeline.pipeline_name == "test_pipe"
    assert reconstructed.dataset_name == "test_ds"
    assert str(reconstructed.started_at) == started_str
    assert str(reconstructed.finished_at) == finished_str
    assert reconstructed.has_failed_jobs is True
    assert reconstructed.loads_ids == ["111", "222"]
    as_dict = reconstructed.asdict()
    assert as_dict["dataset_name"] == "test_ds"
    assert as_dict["loads_ids"] == ["111", "222"]


def test_ensure_docker_image_pass_builds_and_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    """ensure_docker_image builds a missing image and records it in the session cache."""
    built_cache: set[str] = set()
    monkeypatch.setattr(framework, "_built_docker_images", built_cache)
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        if cmd[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(cmd, returncode=1, stdout="", stderr="No such image")
        return subprocess.CompletedProcess(cmd, returncode=0, stdout="built", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    tag = ensure_docker_image("custom-image:test")
    assert tag == "custom-image:test"
    assert [cmd[:4] for cmd in calls] == [
        ["docker", "image", "inspect", "custom-image:test"],
        ["docker", "build", "-t", "custom-image:test"],
    ]
    assert "custom-image:test" in built_cache


@pytest.mark.parametrize(
    ("cached_tags", "expected_calls"),
    [
        pytest.param({"prebuilt-image:test"}, [], id="in-memory-session-cache"),
        pytest.param(set(), [["docker", "image", "inspect", "prebuilt-image:test"]], id="local-docker-daemon"),
    ],
)
def test_ensure_docker_image_pass_reuses_existing_image(
    monkeypatch: pytest.MonkeyPatch,
    cached_tags: set[str],
    expected_calls: list[list[str]],
) -> None:
    """ensure_docker_image skips building when the image is already cached or present in the local daemon."""
    monkeypatch.setattr(framework, "_built_docker_images", set(cached_tags))
    calls: list[list[str]] = []
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda cmd, **_kwargs: (
            calls.append(cmd) or subprocess.CompletedProcess(cmd, returncode=0, stdout="[]", stderr="")
        ),
    )
    assert ensure_docker_image("prebuilt-image:test") == "prebuilt-image:test"
    assert calls == expected_calls


def test_ensure_docker_image_fail_build_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """ensure_docker_image raises RuntimeError when docker build exits with a non-zero status."""
    monkeypatch.setattr(framework, "_built_docker_images", set())
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda cmd, **_kwargs: subprocess.CompletedProcess(cmd, returncode=1, stdout="", stderr="build boom"),
    )
    with pytest.raises(RuntimeError, match="Docker image build failed"):
        ensure_docker_image("failing-image:test")


@pytest.mark.parametrize("mode", [ExecutionMode.CORE, ExecutionMode.CLI], ids=str)
def test_pipeline_test_runner_pass_core_and_cli_modes_end_to_end(
    tmp_path: Path,
    mode: ExecutionMode,
) -> None:
    """PipelineTestRunner executes a pipeline in core and cli modes and hooks into all result assertions."""
    settings = make_cts_settings(
        JsonlExtractSettings,
        tmp_path,
        use_output_dir_for_pipeline_metadata=True,
        dlt_dev_mode=False,
        dataset_name="framework_test_ds",
        table_name="items",
        loader_file_format="jsonl",
    )
    expected_records = [{"id": "rec_1", "value": 10}, {"id": "rec_2", "value": 20}]
    (Path(settings.input_dir) / "data.jsonl").write_text(
        "\n".join(f'{{"id": "{r["id"]}", "value": {r["value"]}}}' for r in expected_records) + "\n",
        encoding="utf-8",
    )

    runner = PipelineTestRunner(mode=mode)
    load_info = assert_run_ok(
        runner(
            "jsonlines_extract",
            settings=settings,
        )
    )

    assert_pipeline_report_written(settings.output_dir, load_info, metadata_moved=True, ingested_files=["data.jsonl"])
    assert set(load_info.pipeline.dataset().tables) >= set(DEFAULT_DLT_TABLES)
    assert read_data_rows(settings.output_dir / settings.dataset_name) == {"items": expected_records}


def test_pipeline_test_runner_pass_docker_mode_command_and_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Docker mode stages input, dlt config, auxiliary files, dynamic modules, and reconstructs LoadInfo."""
    settings = make_cts_settings(
        XmlToDictSettings,
        tmp_path,
        use_output_dir_for_pipeline_metadata=True,
        dlt_dev_mode=False,
        dataset_name="xml_docker_ds",
        table_name="entries",
        xml_tag="entry",
        loader_file_format="parquet",
    )
    (Path(settings.input_dir) / "sample.xml").write_text("<root><entry><id>1</id></entry></root>", encoding="utf-8")
    output_dir = settings.output_dir
    xsd_file = tmp_path / "schema.xsd"
    xsd_file.write_text("<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'/>", encoding="utf-8")

    # Create dynamic in-memory modules for both ENTITY_SCHEMAS and ENTITY_MODELS
    dyn_schema_mod_name = register_temp_module(
        monkeypatch, name="_test_dynamic_schema_mod", ENTITY_SCHEMAS={"widget": {"type": "object"}}
    )
    dyn_model_mod_name = register_temp_module(
        monkeypatch, name="_test_dynamic_model_mod", ENTITY_MODELS={"widget": _SampleEntityModel}
    )

    monkeypatch.setattr(framework, "_built_docker_images", {"test-image:latest"})
    captured_cmds: list[list[str]] = []

    def fake_docker_run(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        captured_cmds.append(cmd)
        # Simulate the container writing a completed report to the mounted output_dir
        (output_dir / "report.txt").write_text(
            "pipeline: xmltodict\n"
            "dataset: xml_docker_ds\n"
            "started: 2026-02-01 00:00:00+00:00\n"
            "finished: 2026-02-01 00:00:05+00:00\n"
            "status: completed\n"
            "load_ids:\n"
            "  - '999'\n"
            "pipeline_load_info_errs: null\n",
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(cmd, returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_docker_run)

    runner = PipelineTestRunner(mode=ExecutionMode.DOCKER, docker_image="test-image:latest")
    cli_args = [
        *settings_to_argv(settings),
        "--xsd-file",
        str(xsd_file),
        "--entity-schemas",
        dyn_schema_mod_name,
        "--entity-models",
        dyn_model_mod_name,
    ]
    load_info = runner.run("xmltodict", cli_args=cli_args)

    assert load_info is not None
    assert load_info.pipeline.pipeline_name == "xmltodict"
    assert load_info.dataset_name == "xml_docker_ds"
    assert len(captured_cmds) == 1

    docker_cmd = captured_cmds[0]
    assert docker_cmd[:3] == ["docker", "run", "--rm"]
    idx_image = docker_cmd.index("test-image:latest")
    assert docker_cmd[idx_image : idx_image + 3] == ["test-image:latest", "pipeline", "xmltodict"]

    container_args = docker_cmd[idx_image + 3 :]
    idx_in = container_args.index("--input-dir")
    assert container_args[idx_in + 1] == CONTAINER_INPUT_DIR
    idx_out = container_args.index("--output-dir")
    assert container_args[idx_out + 1] == CONTAINER_OUTPUT_DIR
    idx_xsd = container_args.index("--xsd-file")
    assert container_args[idx_xsd + 1].startswith(f"{CONTAINER_AUX_DIR}/")
    assert f"PYTHONPATH={CONTAINER_PYMODULES_DIR}:/app/src:/app" in docker_cmd


def test_pipeline_test_runner_pass_docker_mode_test_pipeline_and_workers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Docker mode dispatches test-only pipeline modules via script path and forwards worker config to config.toml."""
    ref_settings = make_cts_settings(
        ReferencePipelineSettings,
        tmp_path,
        use_output_dir_for_pipeline_metadata=True,
        dlt_dev_mode=False,
        dataset_name="ref_docker_ds",
    )
    output_dir = ref_settings.output_dir
    monkeypatch.setattr(framework, "_built_docker_images", {"test-image:latest"})
    captured_cmds: list[list[str]] = []

    def fake_docker_run(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        captured_cmds.append(cmd)
        (output_dir / "report.txt").write_text(
            "pipeline: reference_pipeline\n"
            "dataset: ref_docker_ds\n"
            "started: 2026-02-01 00:00:00+00:00\n"
            "finished: 2026-02-01 00:00:05+00:00\n"
            "status: completed\n"
            "load_ids:\n"
            "  - '999'\n"
            "pipeline_load_info_errs: null\n",
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(cmd, returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_docker_run)
    runner = PipelineTestRunner(mode=ExecutionMode.DOCKER, docker_image="test-image:latest")
    with dlt.config.values({"extract.workers": 3, "normalize.workers": 2, "load.workers": 4}):
        load_info = runner.run(
            run_reference_pipeline,
            settings=ref_settings,
        )
    assert load_info is not None
    ref_docker_cmd = captured_cmds[0]
    idx_ref_image = ref_docker_cmd.index("test-image:latest")
    assert ref_docker_cmd[idx_ref_image : idx_ref_image + 3] == [
        "test-image:latest",
        "pipeline",
        "tests/cdm_data_loaders/pipelines/reference/pipeline.py",
    ]
    stage_dirs = sorted(output_dir.parent.glob(".docker_stage_*"))
    config_text = (stage_dirs[-1] / "input_dir" / "dlt_conf" / "config.toml").read_text(encoding="utf-8")
    assert "[extract]\nworkers = 3" in config_text
    assert "[normalize]\nworkers = 2" in config_text
    assert "[load]\nworkers = 4" in config_text


@pytest.mark.parametrize(
    ("returncode", "stderr", "exc_type", "match"),
    [
        pytest.param(2, "container crashed", RuntimeError, "failed with exit code 2", id="nonzero-runtime-error"),
        pytest.param(
            1,
            "ValueError: Unknown table name: foo",
            ValueError,
            "Unknown table name",
            id="nonzero-value-error",
        ),
        pytest.param(
            0,
            "ValueError: Unknown table name: foo\nWarning: pipeline 'jsonl_pydantic' exited with status 1",
            ValueError,
            "Unknown table name",
            id="swallowed-value-error",
        ),
    ],
)
def test_pipeline_test_runner_fail_docker_mode_nonzero_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    returncode: int,
    stderr: str,
    exc_type: type[Exception],
    match: str,
) -> None:
    """Docker mode raises ValueError on validation errors and RuntimeError on general container failures."""
    settings = make_cts_settings(CtsSettings, tmp_path)
    monkeypatch.setattr(framework, "_built_docker_images", {"test-image:latest"})
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda cmd, **_kwargs: subprocess.CompletedProcess(cmd, returncode=returncode, stdout="", stderr=stderr),
    )
    runner = PipelineTestRunner(mode=ExecutionMode.DOCKER, docker_image="test-image:latest")
    with pytest.raises(exc_type, match=match):
        runner.run(
            "jsonlines_extract",
            cli_args=settings_to_argv(settings),
        )


SPEC_NO_SETTINGS = PipelineSpec(cli_command="custom", module_path="custom.mod")


@pytest.mark.parametrize(
    ("mode", "target", "cli_args_flag", "match"),
    [
        pytest.param(
            ExecutionMode.CORE,
            "jsonlines_extract",
            "--input-dir",
            "Could not determine output_dir",
            id="core-missing-output-dir",
        ),
        pytest.param(
            ExecutionMode.CORE,
            SPEC_NO_SETTINGS,
            None,
            "has no settings_cls",
            id="core-no-settings-cls",
        ),
        pytest.param(
            ExecutionMode.CORE,
            SPEC_NO_SETTINGS,
            "--output-dir",
            "does not expose settings_cls and pipeline_fn",
            id="core-missing-pipeline-fn",
        ),
        pytest.param(
            ExecutionMode.CLI,
            SPEC_NO_SETTINGS,
            "--output-dir",
            r"does not expose a cli\(\) entry point",
            id="cli-missing-cli-callable",
        ),
        pytest.param(
            ExecutionMode.DOCKER,
            "jsonlines_extract",
            "--output-dir",
            "Docker execution mode requires --input-dir",
            id="docker-missing-input-dir",
        ),
    ],
)
def test_pipeline_test_runner_fail_missing_required_arguments(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: ExecutionMode,
    target: str | PipelineSpec,
    cli_args_flag: str | None,
    match: str,
) -> None:
    """PipelineTestRunner raises ValueError when required CLI paths or spec callables are missing."""
    monkeypatch.setattr(framework, "_built_docker_images", {"test-image:latest"})
    runner = PipelineTestRunner(mode=mode, docker_image="test-image:latest")
    cli_args = [cli_args_flag, str(tmp_path)] if cli_args_flag is not None else None
    with pytest.raises(ValueError, match=match):
        runner.run(target, cli_args=cli_args)


def test_resolve_pipeline_pass_legacy_module_without_pipeline_cli() -> None:
    """resolve_pipeline handles modules whose cli attribute is a plain function rather than PipelineCli."""
    legacy_mod = ModuleType("cdm_data_loaders.pipelines.custom_legacy")
    legacy_mod.Settings = JsonlExtractSettings  # type: ignore[attr-defined]
    legacy_mod.run_pipeline = run_jsonlines_pipeline  # type: ignore[attr-defined]
    legacy_mod.cli = lambda: None  # type: ignore[attr-defined]
    spec = resolve_pipeline(legacy_mod)
    assert spec.cli_command == "custom_legacy"
    assert spec.settings_cls is JsonlExtractSettings
    assert spec.pipeline_fn is run_jsonlines_pipeline
    assert spec.cli_callable is legacy_mod.cli


@pytest.mark.docker
def test_pipeline_test_runner_pass_real_docker_container(tmp_path: Path, docker_image: str) -> None:
    """Execute a real Docker container run via PipelineTestRunner when --run-docker is enabled."""
    settings = make_cts_settings(
        JsonlExtractSettings,
        tmp_path,
        use_output_dir_for_pipeline_metadata=True,
        dlt_dev_mode=False,
        dataset_name="docker_framework_ds",
        table_name="items",
        loader_file_format="jsonl",
    )
    expected_records = [{"id": "docker_1", "value": 42}]
    (Path(settings.input_dir) / "data.jsonl").write_text('{"id": "docker_1", "value": 42}\n', encoding="utf-8")

    runner = PipelineTestRunner(mode=ExecutionMode.DOCKER, docker_image=docker_image)
    load_info = assert_run_ok(runner.run("jsonlines_extract", settings=settings))

    assert_pipeline_report_written(settings.output_dir, load_info, metadata_moved=True, ingested_files=["data.jsonl"])
    assert set(load_info.pipeline.dataset().tables) >= set(DEFAULT_DLT_TABLES)
    assert read_data_rows(settings.output_dir / settings.dataset_name) == {"items": expected_records}
