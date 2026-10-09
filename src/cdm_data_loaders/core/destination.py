"""Work out where a pipeline writes its output.

The output location comes from two places: the ``output_dir`` setting (from the CLI or an env var)
and the ``bucket_url`` of the dlt destination block chosen by ``use_destination``. The functions
here combine the two. They only read their inputs and return values; they never change dlt.config,
so they can be tested with plain dicts.

Precedence: an explicit ``output_dir`` wins over ``destination.<use_destination>.bucket_url``.

``use_destination`` only picks a config block (credentials, endpoint, default bucket_url). Its name
has no meaning. Whether the output is local or remote comes from the URL itself.
"""

from collections.abc import Mapping
from typing import Any, Final, Protocol

from frozendict import frozendict
from fsspec.core import split_protocol

LOCAL_PROTOCOLS: Final[frozenset[str]] = frozenset({"file", "local"})
# protocols that are interchangeable for the purpose of matching an override to its config block
PROTOCOL_ALIASES: Final[frozendict[str, str]] = frozendict({"s3a": "s3"})

PIPELINE_METADATA_NOT_LOCAL: Final[str] = (
    "Pipeline metadata must be stored on a local filesystem; "
    "use_output_dir_for_pipeline_metadata cannot be used with output location {url!r}."
)


class ConfigLookup(Protocol):
    """Anything that supports dlt.config-style ``get``: the dlt config accessor, or a plain dict in tests."""

    def get(self, key: str, /) -> Any:  # noqa: ANN401
        """Return the value stored under ``key``."""
        ...


def normalise_dir(path: str) -> str:
    """Remove trailing slashes from a directory path or URL, keeping roots such as '/' and 'file:///'.

    :param path: directory path or URL
    :type path: str
    :return: the normalised path
    :rtype: str
    """
    if not path:
        return path
    stripped = path.rstrip("/")
    if not stripped:
        return "/"
    if stripped.endswith(":"):
        # bare protocol root, e.g. "file:///" or "s3://" -- stripping would leave an invalid URL
        return path
    return stripped


def url_protocol(url: str) -> str:
    """Return the canonical protocol of a path or URL. Plain paths count as 'file'.

    :param url: path or URL
    :type url: str
    :return: lower-case protocol name, with aliases (e.g. s3a -> s3) applied
    :rtype: str
    """
    protocol, _ = split_protocol(url)
    protocol = (protocol or "file").lower()
    return PROTOCOL_ALIASES.get(protocol, protocol)


def is_local(url: str) -> bool:
    """Whether a path or URL is on the local filesystem."""
    return url_protocol(url) in LOCAL_PROTOCOLS


def destinations_from_dlt_config(config: ConfigLookup) -> dict[str, dict[str, Any]]:
    """Take a snapshot of the configured destination names and their bucket_urls, as plain dicts.

    ``bucket_url`` is looked up with a dotted key, so with the real dlt accessor the env var
    ``DESTINATION__<NAME>__BUCKET_URL`` still takes priority over config.toml. With a plain nested
    dict (as in tests), the dotted lookup finds nothing and the value from the section is used.

    Only bucket_url is copied, so credentials never end up in the snapshot.

    :param config: dlt.config, or a nested dict of the form {"destination": {name: {...}}}
    :type config: ConfigLookup
    :return: mapping of destination name to {"bucket_url": str | None}
    :rtype: dict[str, dict[str, Any]]
    """
    if config is None:
        err_msg = "dlt config is None"
        raise ValueError(err_msg)
    sections = config.get("destination") or {}
    if not isinstance(sections, Mapping):
        err_msg = f"Expected the dlt 'destination' config to be a table, got {type(sections).__name__}"
        raise ValueError(err_msg)  # noqa: TRY004

    snapshot: dict[str, dict[str, Any]] = {}
    for name, section in sections.items():
        section_url = section.get("bucket_url") if isinstance(section, Mapping) else None
        override_url = config.get(f"destination.{name}.bucket_url")
        snapshot[str(name)] = {"bucket_url": override_url or section_url}
    return snapshot


def resolve_output_dir(
    use_destination: str,
    output_dir: str | None,
    destinations: Mapping[str, Mapping[str, Any]],
    *,
    pipeline_metadata_in_output_dir: bool = False,
) -> str:
    """Work out the output location from the settings and the configured destinations.

    :param use_destination: name of the dlt destination block to use
    :type use_destination: str
    :param output_dir: explicit output location from the CLI or env, or None if not set
    :type  output_dir: str | None
    :param destinations: destination name -> config section (only "bucket_url" is read)
    :type  destinations: Mapping[str, Mapping[str, Any]]
    :param pipeline_metadata_in_output_dir: whether pipeline metadata will be written to the output dir
    :type  pipeline_metadata_in_output_dir: bool
    :raises ValueError: if the destination is unknown, no location can be found, the override's
        protocol does not match the destination's configured bucket_url, or pipeline metadata
        would be written somewhere that is not local
    :return: the normalised output location
    :rtype : str
    """
    if not destinations:
        err_msg = "No valid destinations found in dlt configuration."
        raise ValueError(err_msg)
    if use_destination not in destinations:
        err_msg = f"use_destination must be one of {sorted(destinations)}, got {use_destination!r}"
        raise ValueError(err_msg)

    configured_url: str | None = (destinations[use_destination] or {}).get("bucket_url") or None
    bucket_url = output_dir or configured_url
    if not bucket_url:
        err_msg = f"No output_dir given and no bucket_url configured for destination {use_destination!r}"
        raise ValueError(err_msg)
    bucket_url = normalise_dir(bucket_url)

    # Only an override can disagree with the block it overrides. A block without a bucket_url is a
    # protocol-agnostic credentials profile, so any URL is accepted for it.
    if output_dir and configured_url and url_protocol(configured_url) != url_protocol(bucket_url):
        err_msg = (
            f"output_dir {bucket_url!r} uses protocol {url_protocol(bucket_url)!r}, but destination "
            f"{use_destination!r} is configured for {url_protocol(configured_url)!r} ({configured_url!r}). "
            f"Choose a destination configured for {url_protocol(bucket_url)!r}, or omit output_dir."
        )
        raise ValueError(err_msg)

    if pipeline_metadata_in_output_dir and not is_local(bucket_url):
        raise ValueError(PIPELINE_METADATA_NOT_LOCAL.format(url=bucket_url))

    return bucket_url
