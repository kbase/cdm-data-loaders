"""Tests for cdm_data_loaders.core.destination."""

import re
from typing import Any, Final

import pytest

from cdm_data_loaders.core.destination import (
    PIPELINE_METADATA_NOT_LOCAL,
    destinations_from_dlt_config,
    is_local,
    normalise_dir,
    resolve_output_dir,
    url_protocol,
)


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        pytest.param("", "", id="empty"),
        pytest.param("/", "/", id="root"),
        pytest.param("///", "/", id="root-repeated-slashes"),
        pytest.param("/data/out", "/data/out", id="no-trailing-slash"),
        pytest.param("/data/out/", "/data/out", id="one-trailing-slash"),
        pytest.param("/data/out///", "/data/out", id="many-trailing-slashes"),
        pytest.param("data/out/", "data/out", id="relative-path"),
        pytest.param("s3://bucket/prefix/", "s3://bucket/prefix", id="s3-prefix"),
        pytest.param("s3://bucket/", "s3://bucket", id="s3-bucket-root"),
        pytest.param("file:///", "file:///", id="file-protocol-root-kept"),
        pytest.param("s3://", "s3://", id="bare-protocol-kept"),
    ],
)
def test_normalise_dir(path: str, expected: str) -> None:
    """Trailing slashes are removed, except where that would break a root path or URL."""
    assert normalise_dir(path) == expected


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        pytest.param("/abs/path", "file", id="absolute-path"),
        pytest.param("rel/path", "file", id="relative-path"),
        pytest.param("", "file", id="empty"),
        pytest.param("file:///abs/path", "file", id="file-url"),
        pytest.param("local://abs/path", "local", id="local-url"),
        pytest.param("s3://bucket/key", "s3", id="s3"),
        pytest.param("S3://bucket/key", "s3", id="upper-case"),
        pytest.param("s3a://bucket/key", "s3", id="s3a-alias"),
        pytest.param("S3A://bucket/key", "s3", id="upper-case-alias"),
        pytest.param("gs://bucket/key", "gs", id="unaliased-remote"),
        pytest.param("c://dir", "file", id="single-letter-protocol-is-drive"),
    ],
)
def test_url_protocol(url: str, expected: str) -> None:
    """Protocols are lower-cased and aliased; plain paths are 'file'."""
    assert url_protocol(url) == expected


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        pytest.param("/abs/path", True, id="absolute-path"),
        pytest.param("rel/path", True, id="relative-path"),
        pytest.param("file:///abs/path", True, id="file-url"),
        pytest.param("FILE:///abs/path", True, id="file-url-upper-case"),
        pytest.param("local://abs/path", True, id="local-url"),
        pytest.param("s3://bucket/key", False, id="s3"),
        pytest.param("s3a://bucket/key", False, id="s3a"),
        pytest.param("gs://bucket/key", False, id="gs"),
        pytest.param("memory://thing", False, id="memory"),
    ],
)
def test_is_local(url: str, *, expected: bool) -> None:
    """Only file and local protocols count as local."""
    assert is_local(url) is expected


@pytest.mark.parametrize(
    ("config", "expected"),
    [
        pytest.param({}, {}, id="no-destination-key"),
        pytest.param({"destination": None}, {}, id="destination-none"),
        pytest.param({"destination": {}}, {}, id="destination-empty"),
        pytest.param(
            {"destination": {"s3": {"bucket_url": "s3://b", "credentials": {"aws_secret_access_key": "secret"}}}},
            {"s3": {"bucket_url": "s3://b"}},
            id="credentials-dropped",
        ),
        pytest.param(
            {"destination": {"local_fs": {"bucket_url": "/out"}, "minio": {"destination_type": "filesystem"}}},
            {"local_fs": {"bucket_url": "/out"}, "minio": {"bucket_url": None}},
            id="section-without-bucket-url",
        ),
        pytest.param(
            {"destination": {"odd": "not-a-table"}},
            {"odd": {"bucket_url": None}},
            id="non-mapping-section",
        ),
        pytest.param(
            {"destination": {1: {"bucket_url": "/out"}}},
            {"1": {"bucket_url": "/out"}},
            id="non-string-name-stringified",
        ),
        pytest.param(
            {"destination": {"s3": {"bucket_url": "s3://from-toml"}}, "destination.s3.bucket_url": "s3://from-env"},
            {"s3": {"bucket_url": "s3://from-env"}},
            id="dotted-override-wins",
        ),
        pytest.param(
            {"destination": {"s3": {}}, "destination.s3.bucket_url": "s3://from-env"},
            {"s3": {"bucket_url": "s3://from-env"}},
            id="dotted-override-fills-missing",
        ),
        pytest.param(
            {"destination": {"s3": {"bucket_url": "s3://from-toml"}}, "destination.s3.bucket_url": ""},
            {"s3": {"bucket_url": "s3://from-toml"}},
            id="empty-override-ignored",
        ),
    ],
)
def test_destinations_from_dlt_config(config: dict[Any, Any], expected: dict[str, dict[str, Any]]) -> None:
    """Each destination is reduced to its bucket_url, with dotted keys taking priority."""
    assert destinations_from_dlt_config(config) == expected


@pytest.mark.parametrize(
    ("sections", "type_name"),
    [
        pytest.param("oops", "str", id="string"),
        pytest.param(["local_fs"], "list", id="list"),
        pytest.param(42, "int", id="int"),
    ],
)
def test_destinations_from_dlt_config_rejects_non_table(sections: object, type_name: str) -> None:
    """A truthy non-mapping 'destination' value is an error."""
    expected = f"Expected the dlt 'destination' config to be a table, got {type_name}"
    with pytest.raises(ValueError, match=f"^{re.escape(expected)}$"):
        destinations_from_dlt_config({"destination": sections})


DESTINATIONS: Final[dict[str, Any]] = {
    "local_fs": {"bucket_url": "/data/out/"},
    "s3": {"bucket_url": "s3://bucket/prefix"},
    "creds_only": {},
    "none_section": None,
}


@pytest.mark.parametrize(
    ("use_destination", "output_dir", "metadata", "expected"),
    [
        pytest.param("local_fs", None, False, "/data/out", id="configured-url-normalised"),
        pytest.param("local_fs", "", False, "/data/out", id="empty-output-dir-falls-back"),
        pytest.param("local_fs", "/other/", False, "/other", id="output-dir-overrides"),
        pytest.param("local_fs", "file:///other", False, "file:///other", id="file-url-matches-plain-path"),
        pytest.param("s3", None, False, "s3://bucket/prefix", id="remote-configured-url"),
        pytest.param("s3", "s3a://other/", False, "s3a://other", id="s3a-alias-matches-s3"),
        pytest.param("creds_only", "s3://any", False, "s3://any", id="block-without-url-accepts-remote"),
        pytest.param("creds_only", "/local", True, "/local", id="block-without-url-accepts-local"),
        pytest.param("none_section", "/local", False, "/local", id="none-section"),
        pytest.param("local_fs", None, True, "/data/out", id="metadata-with-local-configured-url"),
        pytest.param("local_fs", "/other", True, "/other", id="metadata-with-local-output-dir"),
    ],
)
def test_resolve_output_dir(use_destination: str, output_dir: str | None, *, metadata: bool, expected: str) -> None:
    """An explicit output_dir wins over the configured bucket_url, and the result is normalised."""
    actual = resolve_output_dir(use_destination, output_dir, DESTINATIONS, pipeline_metadata_in_output_dir=metadata)
    assert actual == expected


def test_resolve_output_dir_metadata_flag_defaults_to_false() -> None:
    """Without the keyword argument, a remote location is accepted."""
    assert resolve_output_dir("s3", None, DESTINATIONS) == "s3://bucket/prefix"


@pytest.mark.parametrize(
    ("destinations", "use_destination", "output_dir", "metadata", "message"),
    [
        pytest.param(
            {}, "local_fs", "/x", False, "No valid destinations found in dlt configuration.", id="no-destinations"
        ),
        pytest.param(
            DESTINATIONS,
            "missing",
            "/x",
            False,
            "use_destination must be one of ['creds_only', 'local_fs', 'none_section', 's3'], got 'missing'",
            id="unknown-destination",
        ),
        pytest.param(
            DESTINATIONS,
            "",
            "/x",
            False,
            "use_destination must be one of ['creds_only', 'local_fs', 'none_section', 's3'], got ''",
            id="empty-destination-name",
        ),
        pytest.param(
            DESTINATIONS,
            "creds_only",
            None,
            False,
            "No output_dir given and no bucket_url configured for destination 'creds_only'",
            id="no-location-empty-section",
        ),
        pytest.param(
            DESTINATIONS,
            "none_section",
            None,
            False,
            "No output_dir given and no bucket_url configured for destination 'none_section'",
            id="no-location-none-section",
        ),
        pytest.param(
            {"blank": {"bucket_url": ""}},
            "blank",
            "",
            False,
            "No output_dir given and no bucket_url configured for destination 'blank'",
            id="no-location-blank-values",
        ),
        pytest.param(
            DESTINATIONS,
            "local_fs",
            "s3://bucket/x/",
            False,
            "output_dir 's3://bucket/x' uses protocol 's3', but destination 'local_fs' is configured for 'file' "
            "('/data/out/'). Choose a destination configured for 's3', or omit output_dir.",
            id="remote-override-of-local-block",
        ),
        pytest.param(
            DESTINATIONS,
            "s3",
            "/data/elsewhere",
            False,
            "output_dir '/data/elsewhere' uses protocol 'file', but destination 's3' is configured for 's3' "
            "('s3://bucket/prefix'). Choose a destination configured for 'file', or omit output_dir.",
            id="local-override-of-remote-block",
        ),
        pytest.param(
            DESTINATIONS,
            "local_fs",
            "s3://bucket/x",
            True,
            "output_dir 's3://bucket/x' uses protocol 's3', but destination 'local_fs' is configured for 'file' "
            "('/data/out/'). Choose a destination configured for 's3', or omit output_dir.",
            id="protocol-mismatch-reported-before-metadata",
        ),
        pytest.param(
            DESTINATIONS,
            "s3",
            None,
            True,
            PIPELINE_METADATA_NOT_LOCAL.format(url="s3://bucket/prefix"),
            id="metadata-with-remote-configured-url",
        ),
        pytest.param(
            DESTINATIONS,
            "creds_only",
            "s3://any/",
            True,
            PIPELINE_METADATA_NOT_LOCAL.format(url="s3://any"),
            id="metadata-with-remote-output-dir",
        ),
    ],
)
def test_resolve_output_dir_errors(
    destinations: dict[str, Any],
    use_destination: str,
    output_dir: str | None,
    *,
    metadata: bool,
    message: str,
) -> None:
    """Unknown destinations, missing locations, protocol mismatches, and remote metadata are rejected."""
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
        resolve_output_dir(use_destination, output_dir, destinations, pipeline_metadata_in_output_dir=metadata)
