"""Tests for the settings classes and helpers in cdm_data_loaders.core.settings."""

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
from frozendict import frozendict
from pydantic import ValidationError
from pydantic_settings import CliApp, SettingsError

from cdm_data_loaders.core.fields import PIPELINE_DIR_NAME, REPORT_FILE_NAME
from cdm_data_loaders.core.settings import (
    DEFAULT_SETTINGS_CONFIG_DICT,
    FILE_FORMAT_CLI_SHORTCUTS,
    XML_CLI_SHORTCUTS,
    CdmDataLoadersBase,
    CtsSettings,
    FileFormatIngestSettings,
    InputLocalOutputSettings,
    LoggerSettings,
    SingleTableXmlIngestSettings,
    XmlIngestSettings,
    default_settings_with_shortcuts,
    select_cli_shortcuts,
)
from tests.cdm_data_loaders.conftest import (
    SETTINGS_SOURCES,
    SettingsFactory,
    check_settings,
    cts_expected,
    local_io_expected,
)
from tests.conftest import S3
from tests.helpers import (
    assert_cli_field_roundtrips,
    assert_no_cli_clashes,
    settings_to_argv,
)


@pytest.mark.parametrize(
    "settings_cls",
    [
        LoggerSettings,
        InputLocalOutputSettings,
        CtsSettings,
        FileFormatIngestSettings,
        XmlIngestSettings,
        SingleTableXmlIngestSettings,
    ],
    ids=[
        "logger",
        "local-io",
        "cts",
        "file-format-ingest",
        "xml-ingest",
        "single-table-xml-ingest",
    ],
)
def test_settings_pass_no_cli_clashes(settings_cls: type[CdmDataLoadersBase]) -> None:
    """Every core settings class has no CLI argument or alias collisions."""
    assert_no_cli_clashes(settings_cls)


def test_cts_settings_pass_all_params_set(
    test_cts_settings: frozendict[str, Any],
    test_cts_settings_reconciled: frozendict[str, Any],
) -> None:
    """CtsSettings populated with test_cts_settings matches test_cts_settings_reconciled."""
    check_settings(CtsSettings(**test_cts_settings), test_cts_settings_reconciled)


@pytest.mark.parametrize("field_name", list(CtsSettings.model_fields), ids=str)
def test_cts_settings_pass_cli_field_roundtrips(
    field_name: str,
    test_cts_settings: frozendict[str, Any],
    test_cts_settings_reconciled: frozendict[str, Any],
    test_defaults: dict[str, Any],
) -> None:
    """Every registered CLI option and shortcut for each CtsSettings field roundtrips to the expected value."""
    assert_cli_field_roundtrips(
        CtsSettings,
        field_name,
        test_cts_settings[field_name],
        test_cts_settings_reconciled[field_name],
        test_defaults,
    )


@pytest.mark.parametrize(
    ("kwargs", "extra_expected"),
    [
        pytest.param({}, {}, id="no_args"),
        pytest.param({"cli_prog_name": "load-cts"}, {"cli_prog_name": "load-cts"}, id="prog_name"),
        pytest.param({"cli_prog_name": ""}, {"cli_prog_name": ""}, id="empty_prog_name"),
        pytest.param({"cli_shortcuts": {}}, {"cli_shortcuts": {}}, id="empty_shortcuts"),
        pytest.param(
            {
                "cli_shortcuts": {
                    "input_dir": "i",
                    "dlt_dev_mode": "dev_mode",
                    "use_output_dir_for_pipeline_metadata": ["p", "m"],
                }
            },
            {
                "cli_shortcuts": {
                    "input-dir": "i",
                    "dlt-dev-mode": "dev_mode",
                    "use-output-dir-for-pipeline-metadata": ["p", "m"],
                }
            },
            id="targets_kebab_cased_shortcuts_unchanged",
        ),
        pytest.param(
            {"cli_shortcuts": frozendict({"output_dir": "o"})},
            {"cli_shortcuts": {"output-dir": "o"}},
            id="frozendict_shortcuts",
        ),
        pytest.param(
            {"cli_prog_name": "load-cts", "cli_shortcuts": {"input_dir": "i"}},
            {"cli_prog_name": "load-cts", "cli_shortcuts": {"input-dir": "i"}},
            id="prog_name_and_shortcuts",
        ),
    ],
)
def test_default_settings_with_shortcuts_pass(kwargs: dict[str, Any], extra_expected: dict[str, Any]) -> None:
    """Build a config dict from the defaults plus the given prog name and kebab-cased shortcut targets."""
    assert default_settings_with_shortcuts(**kwargs) == {**DEFAULT_SETTINGS_CONFIG_DICT, **extra_expected}


@pytest.mark.parametrize("source", SETTINGS_SOURCES, ids=str)
@pytest.mark.parametrize(
    ("values", "expected"),
    [
        pytest.param({}, {"log_config_file": None}, id="default"),
        pytest.param({"log_config_file": "logging.yaml"}, {"log_config_file": "logging.yaml"}, id="set"),
        pytest.param(
            {"log_config_file": "  logging.yaml\t"}, {"log_config_file": "logging.yaml"}, id="whitespace_stripped"
        ),
    ],
)
def test_logger_settings_pass_sources(
    make_settings: SettingsFactory, source: str, values: dict[str, str], expected: dict[str, Any]
) -> None:
    """Build LoggerSettings from each source, with surrounding whitespace stripped."""
    check_settings(make_settings(LoggerSettings, source, values), expected)


@pytest.mark.parametrize("source", SETTINGS_SOURCES, ids=str)
def test_logger_settings_fail_whitespace_only_log_config_file(make_settings: SettingsFactory, source: str) -> None:
    """Reject a log_config_file that is empty after whitespace stripping."""
    with pytest.raises(ValidationError) as exc_info:
        make_settings(LoggerSettings, source, {"log_config_file": "   "})
    assert {err["loc"][0] for err in exc_info.value.errors()} == {"log_config_file"}


@pytest.mark.parametrize("source", SETTINGS_SOURCES, ids=str)
def test_local_input_output_settings_pass_sources(
    make_settings: SettingsFactory, source: str, input_dir: Path, output_dir: Path
) -> None:
    """Build InputLocalOutputSettings from each source."""
    settings = make_settings(
        InputLocalOutputSettings, source, {"input_dir": str(input_dir), "output_dir": str(output_dir)}
    )
    check_settings(settings, local_io_expected(input_dir, output_dir))


@pytest.mark.parametrize("source", SETTINGS_SOURCES, ids=str)
@pytest.mark.parametrize(
    ("raw_input_dir", "expected_input_dir"),
    [
        pytest.param("s3://bucket/prefix/", "s3://bucket/prefix", id="s3"),
        pytest.param("memory://test-bucket/sub/", "memory://test-bucket/sub", id="memory"),
    ],
)
def test_local_input_output_settings_pass_remote_input_dir_sources(
    make_settings: SettingsFactory,
    source: str,
    raw_input_dir: str,
    expected_input_dir: str,
    output_dir: Path,
) -> None:
    """Build InputLocalOutputSettings with a remote fsspec input_dir from each source."""
    settings = make_settings(
        InputLocalOutputSettings, source, {"input_dir": raw_input_dir, "output_dir": str(output_dir)}
    )
    check_settings(settings, local_io_expected(expected_input_dir, output_dir))


def test_local_input_output_settings_pass_cli_shortcuts(input_dir: Path, output_dir: Path) -> None:
    """Set input_dir and output_dir with the -i and -o shortcuts."""
    settings = CliApp.run(InputLocalOutputSettings, cli_args=["-i", str(input_dir), "-o", str(output_dir)])
    check_settings(settings, local_io_expected(input_dir, output_dir))


@pytest.mark.parametrize("source", SETTINGS_SOURCES, ids=str)
@pytest.mark.parametrize(
    ("values", "overrides"),
    [
        pytest.param({"use_destination": "cts"}, {"use_destination": "cts"}, id="defaults"),
        pytest.param(
            {
                "log_config_file": "logging.yaml",
                "buffer_size": "250",
                "dlt_dev_mode": "true",
                "log_interval": "50",
                "use_destination": "s3",
                "use_output_dir_for_pipeline_metadata": "true",
            },
            {
                "log_config_file": "logging.yaml",
                "buffer_size": 250,
                "dlt_dev_mode": True,
                "log_interval": 50,
                "use_destination": "s3",
                "use_output_dir_for_pipeline_metadata": True,
            },
            id="all_fields_set",
        ),
    ],
)
def test_cts_settings_pass_sources(
    make_settings: SettingsFactory,
    source: str,
    values: dict[str, str],
    overrides: dict[str, Any],
    test_defaults: dict[str, Any],
) -> None:
    """Build CtsSettings from each source and check every field, including the computed pipeline_dir."""
    settings = make_settings(
        CtsSettings,
        source,
        {**{k: str(v) for k, v in test_defaults.items()}, **values},
    )
    pipeline_dir = (
        test_defaults["output_dir"] / PIPELINE_DIR_NAME
        if overrides.get("use_output_dir_for_pipeline_metadata")
        else None
    )
    check_settings(settings, cts_expected(**{**test_defaults, **overrides}, pipeline_dir=pipeline_dir))


def test_cts_settings_pass_cli_shortcuts(input_dir: Path, output_dir: Path) -> None:
    """Set fields with the -i, -o, -d and -p shortcuts, as CliApp.run does in production."""
    settings = CliApp.run(
        CtsSettings,
        cli_args=["-i", str(input_dir), "-o", str(output_dir), "-d", "s3", "-p", "true"],
    )
    check_settings(
        settings,
        cts_expected(
            input_dir,
            output_dir,
            use_destination="s3",
            use_output_dir_for_pipeline_metadata=True,
            pipeline_dir=output_dir.resolve() / PIPELINE_DIR_NAME,
        ),
    )


@pytest.mark.parametrize(
    "extra_args",
    [
        pytest.param(["--not-a-setting", "x"], id="unknown_arg"),
        pytest.param(["--buffer_size", "5"], id="snake_case_arg"),
    ],
)
def test_cts_settings_fail_unrecognised_cli_args(test_defaults: dict[str, Any], extra_args: list[str]) -> None:
    """Raise SettingsError, not SystemExit, for CLI args that are not registered."""
    base = CtsSettings(**test_defaults)
    with pytest.raises(SettingsError, match="unrecognized arguments"):
        CliApp.run(CtsSettings, cli_args=[*settings_to_argv(base), *extra_args])


@pytest.mark.parametrize(
    "settings_cls",
    [InputLocalOutputSettings, CtsSettings],
    ids=["local_input_output_settings", "cts_settings"],
)
@pytest.mark.parametrize("set_unprefixed_env", [False, True], ids=["nothing_set", "env_vars_without_prefix"])
def test_settings_fail_missing_dirs(
    monkeypatch: pytest.MonkeyPatch,
    input_dir: Path,
    output_dir: Path,
    settings_cls: type[CdmDataLoadersBase],
    set_unprefixed_env: bool,
) -> None:
    """Fail when input_dir and output_dir are missing; env vars without the CDL_ prefix are ignored."""
    if set_unprefixed_env:
        monkeypatch.setenv("INPUT_DIR", str(input_dir))
        monkeypatch.setenv("OUTPUT_DIR", str(output_dir))
    expected = {
        (("input_dir",), "missing"),
        (("output_dir",), "missing"),
    }
    if settings_cls is CtsSettings:
        expected.add((("use_destination",), "missing"))
    with pytest.raises(ValidationError) as exc_info:
        CliApp.run(settings_cls, cli_args=[])
    assert {(err["loc"], err["type"]) for err in exc_info.value.errors()} == expected


def test_file_format_ingest_settings_pass_cli_shortcuts(test_defaults: dict[str, Any]) -> None:
    """FileFormatIngestSettings supports FILE_FORMAT_CLI_SHORTCUTS (-f) in addition to CtsSettings shortcuts."""
    assert FILE_FORMAT_CLI_SHORTCUTS["loader_file_format"] == "f"
    base = FileFormatIngestSettings(**test_defaults, dataset_name="my_dataset")
    settings = CliApp.run(
        FileFormatIngestSettings,
        cli_args=[
            *settings_to_argv(base, exclude=["loader_file_format"]),
            "-f",
            "jsonl",
        ],
    )
    check_settings(
        settings,
        {
            **cts_expected(**test_defaults, dataset_name="my_dataset"),
            "loader_file_format": "jsonl",
        },
    )


@pytest.mark.parametrize("source", SETTINGS_SOURCES, ids=str)
def test_xml_ingest_settings_pass_defaults_across_sources(
    make_settings: SettingsFactory, source: str, test_defaults: dict[str, Any]
) -> None:
    """XmlIngestSettings defaults file_glob to '*.xml*' and loader_file_format to 'parquet' across sources."""
    default_glob_settings = make_settings(
        XmlIngestSettings,
        source,
        {
            **{k: str(v) for k, v in test_defaults.items()},
            "dataset_name": "xml_dataset",
        },
    )
    check_settings(
        default_glob_settings,
        {
            **cts_expected(**test_defaults, dataset_name="xml_dataset"),
            "loader_file_format": "parquet",
            "file_glob": "*.xml*",
        },
    )


def test_xml_ingest_settings_pass_cli_shortcuts(test_defaults: dict[str, Any]) -> None:
    """XmlIngestSettings supports XML_CLI_SHORTCUTS (-f, -g)."""
    assert XML_CLI_SHORTCUTS["loader_file_format"] == "f"
    assert XML_CLI_SHORTCUTS["file_glob"] == "g"
    base = XmlIngestSettings(**test_defaults, dataset_name="xml_dataset")
    cli_settings = CliApp.run(
        XmlIngestSettings,
        cli_args=[
            *settings_to_argv(base, exclude=["loader_file_format", "file_glob"]),
            "-f",
            "jsonl",
            "-g",
            "*.xml.gz",
        ],
    )
    check_settings(
        cli_settings,
        {
            **cts_expected(**test_defaults, dataset_name="xml_dataset"),
            "loader_file_format": "jsonl",
            "file_glob": "*.xml.gz",
        },
    )


@pytest.mark.parametrize("field", ["buffer_size", "log_interval"], ids=["buffer_size", "log_interval"])
def test_cts_settings_fail_non_positive_int_fields(test_defaults: dict[str, Any], field: str) -> None:
    """buffer_size and log_interval must be positive integers."""
    with pytest.raises(ValidationError):
        CtsSettings(**{**test_defaults, field: 0})


def test_cts_settings_fail_s3_output_dir(test_defaults: dict[str, Any]) -> None:
    """An s3:// output_dir raises ValidationError at construction because output_dir must be a local path."""
    with pytest.raises(ValidationError):
        CtsSettings(**{**test_defaults, "output_dir": "s3://some-bucket/path"})


@pytest.mark.parametrize("destination", [S3, "not_a_destination"], ids=["s3", "custom_destination"])
def test_cts_settings_pass_any_non_empty_use_destination_accepted_at_construction(
    test_defaults: dict[str, Any], destination: str
) -> None:
    """Any non-empty use_destination is accepted at settings construction time."""
    settings = CtsSettings(**{**test_defaults, "use_destination": destination})
    assert settings.use_destination == destination


@pytest.mark.parametrize("dataset_name", [None, ""], ids=["none", "empty"])
def test_file_format_ingest_settings_fail_missing_or_empty_dataset_name(
    test_defaults: dict[str, Any], dataset_name: str | None
) -> None:
    """Omitting or passing an empty dataset_name raises ValidationError."""
    with pytest.raises(ValidationError):
        FileFormatIngestSettings(**test_defaults, dataset_name=dataset_name)  # type: ignore[arg-type]


def test_single_table_xml_ingest_settings_pass(test_defaults: dict[str, Any]) -> None:
    """SingleTableXmlIngestSettings populates table_name, xml_tag, and default file_glob."""
    settings = SingleTableXmlIngestSettings(
        **test_defaults,
        dataset_name="xml_ds",
        table_name="entries",
        xml_tag="entry",
    )
    assert settings.table_name == "entries"
    assert settings.xml_tag == "entry"
    assert settings.file_glob == "*.xml*"


@pytest.mark.parametrize(
    ("shortcuts", "keys", "base", "expected"),
    [
        pytest.param(
            {"file_glob": "g"},
            (),
            FILE_FORMAT_CLI_SHORTCUTS,
            dict(FILE_FORMAT_CLI_SHORTCUTS),
            id="no_keys_returns_base",
        ),
        pytest.param(
            {},
            ("file_glob",),
            FILE_FORMAT_CLI_SHORTCUTS,
            dict(FILE_FORMAT_CLI_SHORTCUTS),
            id="empty_shortcuts_with_keys",
        ),
        pytest.param(
            {"file_glob": "g", "table_name": "t", "extra": "x"},
            ("file_glob", "table_name"),
            FILE_FORMAT_CLI_SHORTCUTS,
            {**FILE_FORMAT_CLI_SHORTCUTS, "file_glob": "g", "table_name": "t"},
            id="subset_of_keys_selected",
        ),
        pytest.param(
            {"file_glob": "g"},
            ("file_glob", "nonexistent"),
            FILE_FORMAT_CLI_SHORTCUTS,
            {**FILE_FORMAT_CLI_SHORTCUTS, "file_glob": "g"},
            id="unknown_keys_ignored",
        ),
        pytest.param(
            {"input_dir": "x"},
            ("input_dir",),
            FILE_FORMAT_CLI_SHORTCUTS,
            {**FILE_FORMAT_CLI_SHORTCUTS, "input_dir": "x"},
            id="overrides_base_key",
        ),
        pytest.param(
            {"file_glob": "g", "table_name": "t"},
            ("file_glob",),
            {"custom": "c"},
            {"custom": "c", "file_glob": "g"},
            id="custom_base_mapping",
        ),
        pytest.param(
            {"file_glob": "g"},
            ("file_glob",),
            {},
            {"file_glob": "g"},
            id="empty_base_mapping",
        ),
    ],
)
def test_select_cli_shortcuts_pass(
    shortcuts: Mapping[str, str],
    keys: tuple[str, ...],
    base: Mapping[str, str],
    expected: dict[str, str],
) -> None:
    """Merge selected shortcut keys over the base shortcut mapping."""
    assert select_cli_shortcuts(shortcuts, *keys, base=base) == expected


@pytest.mark.parametrize(
    ("use_output_dir_for_pipeline_metadata", "expected_rel_path"),
    [
        pytest.param(
            False,
            Path(REPORT_FILE_NAME),
            id="output_dir_root",
        ),
        pytest.param(
            True,
            Path(PIPELINE_DIR_NAME) / REPORT_FILE_NAME,
            id="pipeline_metadata_subdir",
        ),
    ],
)
def test_cts_settings_report_path_pass(
    test_defaults: dict[str, Any],
    use_output_dir_for_pipeline_metadata: bool,
    expected_rel_path: Path,
) -> None:
    """Resolve report_path in output_dir or pipeline_dir based on use_output_dir_for_pipeline_metadata."""
    settings = CtsSettings(
        **{
            **test_defaults,
            "use_output_dir_for_pipeline_metadata": use_output_dir_for_pipeline_metadata,
        }
    )
    assert settings.report_path == settings.output_dir / expected_rel_path


@pytest.mark.parametrize("missing_field", ["table_name", "xml_tag"], ids=["missing-table-name", "missing-xml-tag"])
def test_single_table_xml_ingest_settings_fail_missing_required_field(
    test_defaults: dict[str, Any],
    missing_field: str,
) -> None:
    """SingleTableXmlIngestSettings rejects missing table_name or xml_tag."""
    kwargs: dict[str, Any] = {
        **test_defaults,
        "dataset_name": "xml_ds",
        "table_name": "entries",
        "xml_tag": "entry",
        missing_field: None,
    }
    with pytest.raises(ValidationError):
        SingleTableXmlIngestSettings(**kwargs)
