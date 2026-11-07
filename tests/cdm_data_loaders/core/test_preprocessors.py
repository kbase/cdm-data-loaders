"""Unit tests for file pre-processors, PreprocessorRegistry, and TransformSettingsMixin pre-processor support."""

import gzip
from types import MappingProxyType, SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
from dlt.common.storages.fsspec_filesystem import FileItemDict
from frozendict import frozendict
from pydantic import ValidationError

from cdm_data_loaders.core.preprocessors import (
    FILE_PREPROCESSORS,
    FilePreprocessError,
    parse_md5_sidecar,
    validate_file_preprocessors,
    validate_md5_sidecar,
)
from cdm_data_loaders.core.registry import (
    PreprocessorRegistry,
    load_file_preprocessors,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlExtractSettings
from cdm_data_loaders.utils.checksums import new_hasher
from tests.fs_backends import FsBackend
from tests.helpers import register_temp_module

SAMPLE_DIGEST = "d41d8cd98f00b204e9800998ecf8427e"


def _noop_preprocessor(file_item: FileItemDict) -> FileItemDict:
    """Return file_item unchanged."""
    return file_item


def _filter_preprocessor(_file_item: FileItemDict) -> None:
    """Filter out every file item."""


def _md5_hex(data: bytes) -> str:
    """Compute the MD5 hex digest of bytes."""
    hasher = new_hasher("md5")
    hasher.update(data)
    return hasher.hexdigest()


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        pytest.param(
            f"MD5(pubmed26n0001.xml.gz)= {SAMPLE_DIGEST}\n",
            SAMPLE_DIGEST,
            id="ncbi-bsd-format",
        ),
        pytest.param(
            f"\n  MD5 ( pubmed26n0001.xml.gz ) = {SAMPLE_DIGEST.upper()}  \n",
            SAMPLE_DIGEST,
            id="ncbi-bsd-uppercase-and-whitespace",
        ),
        pytest.param(
            f"{SAMPLE_DIGEST}  pubmed26n0001.xml.gz\n",
            SAMPLE_DIGEST,
            id="gnu-md5sum-two-spaces",
        ),
        pytest.param(
            f"{SAMPLE_DIGEST} *pubmed26n0001.xml.gz\n",
            SAMPLE_DIGEST,
            id="gnu-md5sum-binary-star",
        ),
        pytest.param(
            f"  {SAMPLE_DIGEST.upper()} \n",
            SAMPLE_DIGEST,
            id="bare-hex-digest",
        ),
    ],
)
def test_parse_md5_sidecar_pass_formats(content: str, expected: str) -> None:
    """parse_md5_sidecar extracts lowercase 32-char hex digests across BSD, GNU md5sum, and bare formats."""
    assert parse_md5_sidecar(content, "pubmed26n0001.xml.gz") == expected


@pytest.mark.parametrize(
    ("content", "match"),
    [
        pytest.param("", "Empty MD5 sidecar file for data.xml.gz", id="empty"),
        pytest.param("   \n\t\n  ", "Empty MD5 sidecar file for data.xml.gz", id="whitespace-only"),
        pytest.param("not-a-valid-md5", "Invalid MD5 sidecar content for data.xml.gz", id="malformed-text"),
        pytest.param("abc123", "Invalid MD5 sidecar content for data.xml.gz", id="short-hex"),
        pytest.param(
            f"SHA256(data.xml.gz)= {SAMPLE_DIGEST}",
            "Invalid MD5 sidecar content for data.xml.gz",
            id="wrong-algorithm-prefix",
        ),
    ],
)
def test_parse_md5_sidecar_fail_invalid_content(content: str, match: str) -> None:
    """parse_md5_sidecar raises FilePreprocessError on empty or malformed sidecar content."""
    with pytest.raises(FilePreprocessError, match=match):
        parse_md5_sidecar(content, "data.xml.gz")


@pytest.mark.parametrize(
    "sidecar_template",
    [
        pytest.param("MD5(sample.xml.gz)= {digest}\n", id="bsd-format"),
        pytest.param("{digest}  sample.xml.gz\n", id="gnu-format"),
        pytest.param("{digest}\n", id="bare-format"),
    ],
)
def test_validate_md5_sidecar_pass_any_backend(
    fs_backend: FsBackend,
    sidecar_template: str,
) -> None:
    """validate_md5_sidecar hashes raw compressed .xml.gz bytes and validates against the .md5 sidecar across backends."""
    compressed_bytes = gzip.compress(b"<PubmedArticleSet><PubmedArticle/></PubmedArticleSet>")
    digest = _md5_hex(compressed_bytes)
    fs_backend.put("sub/sample.xml.gz", compressed_bytes)
    fs_backend.put("sub/sample.xml.gz.md5", sidecar_template.format(digest=digest).encode("utf-8"))

    items = fs_backend.file_items(file_glob="**/*.xml.gz")
    assert len(items) == 1
    validated = validate_md5_sidecar(items[0])
    assert validated is items[0]


def test_validate_md5_sidecar_fail_missing_sidecar(fs_backend: FsBackend) -> None:
    """validate_md5_sidecar raises FilePreprocessError when the accompanying .md5 file does not exist."""
    fs_backend.put("sample.xml.gz", gzip.compress(b"<root/>"))
    items = fs_backend.file_items(file_glob="*.xml.gz")
    assert len(items) == 1

    with pytest.raises(FilePreprocessError, match=r"Missing MD5 sidecar file for sample\.xml\.gz"):
        validate_md5_sidecar(items[0])


def test_validate_md5_sidecar_fail_mismatch(fs_backend: FsBackend) -> None:
    """validate_md5_sidecar raises FilePreprocessError when the computed MD5 does not match the sidecar digest."""
    compressed_bytes = gzip.compress(b"<root>actual</root>")
    actual_digest = _md5_hex(compressed_bytes)
    wrong_digest = "0" * 32
    fs_backend.put("sample.xml.gz", compressed_bytes)
    fs_backend.put("sample.xml.gz.md5", f"MD5(sample.xml.gz)= {wrong_digest}\n".encode())
    items = fs_backend.file_items(file_glob="*.xml.gz")

    with pytest.raises(
        FilePreprocessError,
        match=rf"MD5 mismatch for sample\.xml\.gz: expected {wrong_digest}, got {actual_digest}",
    ):
        validate_md5_sidecar(items[0])


def test_validate_md5_sidecar_fail_oserror_reading_sidecar(fs_backend: FsBackend) -> None:
    """validate_md5_sidecar wraps non-FileNotFoundError OSErrors in FilePreprocessError."""
    fs_backend.put("sample.xml.gz", gzip.compress(b"<root/>"))
    file_item = fs_backend.file_items(file_glob="*.xml.gz")[0]

    with (
        patch.object(file_item.fsspec, "open", side_effect=PermissionError("permission denied")),
        pytest.raises(
            FilePreprocessError,
            match=r"Failed to read MD5 sidecar file for sample\.xml\.gz: permission denied",
        ),
    ):
        validate_md5_sidecar(file_item)


def test_preprocessor_registry_pass_sequence_and_dict_modes() -> None:
    """PreprocessorRegistry supports both pipeline-wide sequence mode and per-table dict mode."""
    seq_reg = PreprocessorRegistry(default=(_noop_preprocessor, _filter_preprocessor))
    assert seq_reg.get() == (_noop_preprocessor, _filter_preprocessor)
    assert seq_reg.get("any_table") == (_noop_preprocessor, _filter_preprocessor)

    dict_reg = PreprocessorRegistry(
        registry={
            "  table_a ": _noop_preprocessor,
            "table_b": [_noop_preprocessor, _filter_preprocessor],
        }
    )
    assert isinstance(dict_reg.registry, MappingProxyType)
    assert dict_reg.get("table_a") == (_noop_preprocessor,)
    assert dict_reg.get("table_b") == (_noop_preprocessor, _filter_preprocessor)
    assert dict_reg.get("unlisted") == ()
    assert dict_reg.get(None) == (_noop_preprocessor, _filter_preprocessor)


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


def test_validate_file_preprocessors_pass_and_fail() -> None:
    """validate_file_preprocessors allows sequence registries and valid table keys, and rejects unknown tables."""
    validate_file_preprocessors(SimpleNamespace(file_preprocessors=None), {"items"})
    validate_file_preprocessors(
        SimpleNamespace(file_preprocessors=PreprocessorRegistry(default=(_noop_preprocessor,))),
        {"items"},
    )
    validate_file_preprocessors(
        SimpleNamespace(file_preprocessors=PreprocessorRegistry(registry={"items": _noop_preprocessor})),
        {"items", "other"},
    )

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
        preprocessors_module="cdm_data_loaders.core.preprocessors",
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
