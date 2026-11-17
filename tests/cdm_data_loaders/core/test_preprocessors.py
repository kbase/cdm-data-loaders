"""Unit tests for PreprocessorRegistry, validate_file_preprocessors, and TransformSettingsMixin pre-processor support."""

from types import MappingProxyType, SimpleNamespace
from typing import Any

import pytest
from dlt.common.storages.fsspec_filesystem import FileItemDict
from frozendict import frozendict
from pydantic import ValidationError

from cdm_data_loaders.core.md5_preprocessor import FILE_PREPROCESSORS, validate_md5_sidecar
from cdm_data_loaders.core.preprocessors import validate_file_preprocessors
from cdm_data_loaders.core.registry import (
    PreprocessorRegistry,
    load_file_preprocessors,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlExtractSettings
from tests.helpers.cli_assertions import register_temp_module


def _noop_preprocessor(file_item: FileItemDict) -> FileItemDict:
    """Return file_item unchanged."""
    return file_item


def _filter_preprocessor(_file_item: FileItemDict) -> None:
    """Filter out every file item."""


@pytest.mark.parametrize(
    ("registry", "table_name", "expected"),
    [
        pytest.param(
            PreprocessorRegistry(default=(_noop_preprocessor, _filter_preprocessor)),
            None,
            (_noop_preprocessor, _filter_preprocessor),
            id="sequence-none-table",
        ),
        pytest.param(
            PreprocessorRegistry(default=(_noop_preprocessor, _filter_preprocessor)),
            "any_table",
            (_noop_preprocessor, _filter_preprocessor),
            id="sequence-named-table",
        ),
        pytest.param(
            PreprocessorRegistry(
                registry={
                    "  table_a ": _noop_preprocessor,
                    "table_b": [_noop_preprocessor, _filter_preprocessor],
                }
            ),
            "table_a",
            (_noop_preprocessor,),
            id="dict-trimmed-key",
        ),
        pytest.param(
            PreprocessorRegistry(
                registry={
                    "  table_a ": _noop_preprocessor,
                    "table_b": [_noop_preprocessor, _filter_preprocessor],
                }
            ),
            "table_b",
            (_noop_preprocessor, _filter_preprocessor),
            id="dict-sequence-value",
        ),
        pytest.param(
            PreprocessorRegistry(
                registry={
                    "  table_a ": _noop_preprocessor,
                    "table_b": [_noop_preprocessor, _filter_preprocessor],
                }
            ),
            "unlisted",
            (),
            id="dict-unlisted-table",
        ),
        pytest.param(
            PreprocessorRegistry(
                registry={
                    "  table_a ": _noop_preprocessor,
                    "table_b": [_noop_preprocessor, _filter_preprocessor],
                }
            ),
            None,
            (_noop_preprocessor, _filter_preprocessor),
            id="dict-none-table-deduped",
        ),
    ],
)
def test_preprocessor_registry_pass_sequence_and_dict_modes(
    registry: PreprocessorRegistry,
    table_name: str | None,
    expected: tuple[Any, ...],
) -> None:
    """PreprocessorRegistry supports both pipeline-wide sequence mode and per-table dict mode."""
    if registry.registry is not None:
        assert isinstance(registry.registry, MappingProxyType)
    assert registry.get(table_name) == expected


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        pytest.param(
            {},
            "PreprocessorRegistry must define at least one preprocessor in 'default' or 'registry'",
            id="both-empty",
        ),
        pytest.param(
            {"default": (_noop_preprocessor,), "registry": {"t": (_noop_preprocessor,)}},
            "PreprocessorRegistry cannot define both 'default' and 'registry' preprocessors",
            id="both-populated",
        ),
        pytest.param(
            {"registry": {"_rejected": _noop_preprocessor}},
            "PreprocessorRegistry cannot use reserved table name '_rejected'",
            id="reserved-rejected-table",
        ),
        pytest.param(
            {"registry": {"t": []}},
            "Value should have at least 1 item after validation",
            id="empty-table-sequence",
        ),
        pytest.param(
            {"registry": {"t": "not_callable"}},
            "Input should be a valid tuple",
            id="invalid-table-entry",
        ),
    ],
)
def test_preprocessor_registry_fail_invalid_inputs(kwargs: dict[str, Any], match: str) -> None:
    """PreprocessorRegistry rejects empty, conflicting, reserved, or non-callable configurations."""
    with pytest.raises(ValidationError, match=match):
        PreprocessorRegistry(**kwargs)


@pytest.mark.parametrize(
    ("raw_attr", "expected"),
    [
        pytest.param(
            [_noop_preprocessor, _filter_preprocessor],
            PreprocessorRegistry(default=(_noop_preprocessor, _filter_preprocessor)),
            id="list-of-callables",
        ),
        pytest.param(
            (_noop_preprocessor,),
            PreprocessorRegistry(default=(_noop_preprocessor,)),
            id="tuple-of-callables",
        ),
        pytest.param(
            {"items": _noop_preprocessor, "other": [_filter_preprocessor]},
            PreprocessorRegistry(registry={"items": (_noop_preprocessor,), "other": (_filter_preprocessor,)}),
            id="dict-of-callables-and-lists",
        ),
    ],
)
def test_load_file_preprocessors_pass(
    monkeypatch: pytest.MonkeyPatch,
    raw_attr: Any,  # noqa: ANN401
    expected: PreprocessorRegistry,
) -> None:
    """load_file_preprocessors loads both sequence and dict FILE_PREPROCESSORS definitions."""
    mod_name = register_temp_module(monkeypatch, "_test_preprocessors_mod", FILE_PREPROCESSORS=raw_attr)
    assert load_file_preprocessors(mod_name) == expected


@pytest.mark.parametrize(
    ("attrs", "exc_type", "match"),
    [
        pytest.param(
            {},
            TypeError,
            r"must be a dictionary or a sequence of callables, not NoneType",
            id="missing-attribute",
        ),
        pytest.param(
            {"FILE_PREPROCESSORS": "not_a_seq_of_callables"},
            TypeError,
            r"must be a dictionary or a sequence of callables, not str",
            id="string-attribute",
        ),
        pytest.param(
            {"FILE_PREPROCESSORS": 123},
            TypeError,
            r"must be a dictionary or a sequence of callables, not int",
            id="int-attribute",
        ),
        pytest.param(
            {"FILE_PREPROCESSORS": []},
            ValidationError,
            "PreprocessorRegistry must define at least one preprocessor",
            id="empty-list",
        ),
        pytest.param(
            {"FILE_PREPROCESSORS": {}},
            ValidationError,
            "PreprocessorRegistry must define at least one preprocessor",
            id="empty-dict",
        ),
    ],
)
def test_load_file_preprocessors_fail(
    monkeypatch: pytest.MonkeyPatch,
    attrs: dict[str, Any],
    exc_type: type[Exception],
    match: str,
) -> None:
    """load_file_preprocessors raises TypeError or ValidationError on missing or malformed FILE_PREPROCESSORS."""
    mod_name = register_temp_module(monkeypatch, "_test_invalid_preprocessors_mod", **attrs)
    with pytest.raises(exc_type, match=match):
        load_file_preprocessors(mod_name)


@pytest.mark.parametrize(
    ("file_preprocessors", "valid_tables"),
    [
        pytest.param(None, {"items"}, id="none-registry"),
        pytest.param(
            PreprocessorRegistry(default=(_noop_preprocessor,)),
            {"items"},
            id="sequence-registry",
        ),
        pytest.param(
            PreprocessorRegistry(registry={"items": _noop_preprocessor}),
            {"items", "other"},
            id="dict-registry-subset",
        ),
    ],
)
def test_validate_file_preprocessors_pass(
    file_preprocessors: PreprocessorRegistry | None,
    valid_tables: set[str],
) -> None:
    """validate_file_preprocessors allows None, sequence registries, and valid table keys."""
    validate_file_preprocessors(SimpleNamespace(file_preprocessors=file_preprocessors), valid_tables)


def test_validate_file_preprocessors_fail_unknown_tables() -> None:
    """validate_file_preprocessors rejects unknown table names in a dict registry."""
    bad_settings = SimpleNamespace(
        file_preprocessors=PreprocessorRegistry(
            registry={"items": _noop_preprocessor, "extra": _noop_preprocessor, " bogus ": _noop_preprocessor}
        )
    )
    with pytest.raises(ValueError, match=r"^Unknown table names in preprocessors_module: bogus, extra$"):
        validate_file_preprocessors(bad_settings, {"items"})


def test_transform_settings_mixin_pass_preprocessors_module(
    monkeypatch: pytest.MonkeyPatch,
    test_cts_settings: frozendict[str, Any],
) -> None:
    """TransformSettingsMixin loads preprocessors_module in both sequence and dict modes."""
    assert list(FILE_PREPROCESSORS) == [validate_md5_sidecar]
    seq_settings = JsonlExtractSettings(
        **test_cts_settings,
        dataset_name="ds",
        table_name="items",
        preprocessors_module="cdm_data_loaders.core.md5_preprocessor",
    )
    assert seq_settings.get_file_preprocessors("items") == (validate_md5_sidecar,)

    dict_mod = register_temp_module(
        monkeypatch,
        "_test_dict_preprocessors_mod",
        FILE_PREPROCESSORS={"items": [_noop_preprocessor, _filter_preprocessor]},
    )
    dict_settings = JsonlExtractSettings(
        **test_cts_settings,
        dataset_name="ds",
        table_name="items",
        preprocessors_module=dict_mod,
    )
    assert dict_settings.get_file_preprocessors("items") == (_noop_preprocessor, _filter_preprocessor)
    assert dict_settings.get_file_preprocessors("other") == ()


def test_transform_settings_mixin_fail_unknown_preprocessor_table(
    monkeypatch: pytest.MonkeyPatch,
    test_cts_settings: frozendict[str, Any],
) -> None:
    """TransformSettingsMixin raises ValidationError when preprocessors_module references an unknown table."""
    dict_mod = register_temp_module(
        monkeypatch,
        "_test_unknown_table_preprocessors_mod",
        FILE_PREPROCESSORS={"wrong_table": _noop_preprocessor},
    )
    with pytest.raises(ValidationError, match="Unknown table names in preprocessors_module: wrong_table"):
        JsonlExtractSettings(
            **test_cts_settings,
            dataset_name="ds",
            table_name="items",
            preprocessors_module=dict_mod,
        )
