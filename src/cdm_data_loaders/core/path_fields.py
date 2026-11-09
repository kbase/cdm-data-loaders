"""Path-related fields and utils for pipeline settings."""

from pathlib import Path
from typing import Annotated, Any, Final

from fsspec import get_filesystem_class
from fsspec.core import split_protocol
from pydantic import AfterValidator, BeforeValidator, DirectoryPath, Field, StringConstraints
from pydantic_core import PydanticCustomError

VALID_PATH_ONE_CHAR: Final[str] = "A valid path must contain at least one character"
FILE_PROTOCOL: Final[str] = "file"
PATH_EMPTY: Final[str] = "Path must not be empty."
PATH_NOT_LOCAL: Final[str] = "Path must be on the local filesystem, not a URL"
PATH_NOT_DIRECTORY_ERROR_TYPE: Final[str] = "path_not_directory"
PATH_NOT_DIRECTORY: Final[str] = "Path does not point to a directory"
DIR_NOT_CREATED: Final[str] = "Cannot create directory "


def url_protocol(url: str) -> str:
    """Return the canonical protocol of a path or URL. Plain paths count as 'file'.

    :param url: path or URL
    :type url: str
    :return: lower-case protocol name, with aliases (e.g. s3a -> s3) applied
    :rtype: str
    """
    protocol, _ = split_protocol(url)
    return (protocol or FILE_PROTOCOL).lower()


def is_local(url: str) -> bool:
    """Whether a path or URL is on the local filesystem."""
    return url_protocol(url) == FILE_PROTOCOL


def check_local_path(value: Any) -> Any:  # noqa: ANN401
    """Strip whitespace and any local protocol from a string path.

    Values that are not strings are returned unchanged.

    :param value: path to check
    :type value: Any
    :raises ValueError: if the path is empty or not on the local filesystem
    :return: the path without a protocol
    :rtype: Any
    """
    if not isinstance(value, str):
        # pydantic's Path validation handles these
        return value
    value = value.strip()
    if not is_local(value):
        err_msg = f"{PATH_NOT_LOCAL}: {value!r}"
        raise ValueError(err_msg)
    _, path = split_protocol(value)
    # check after removing the protocol so that "file://" is rejected; Path("") would become Path(".")
    if not path:
        raise ValueError(PATH_EMPTY)
    return path


def to_absolute(path: Path) -> Path:
    """Return the absolute, resolved form of a path.

    :param path: the path to resolve
    :type  path: Path
    :return: absolute path
    :rtype : Path
    """
    return path.resolve()


def ensure_dir(path: Path) -> Path:
    """Create a directory, converting OS errors to ValueError so pydantic reports them.

    :param path: the directory to create
    :type  path: Path
    :return: the path object
    :rtype : Path
    """
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        msg = f"{DIR_NOT_CREATED}{path!r}: {e}"
        raise ValueError(msg) from e
    return path


def _resolve_existing_local_dir(path: Path) -> str:
    """Validate that ``path`` is an existing directory and return its resolved absolute path string."""
    if not path.is_dir():
        raise PydanticCustomError(
            PATH_NOT_DIRECTORY_ERROR_TYPE,
            PATH_NOT_DIRECTORY,
        )
    return str(to_absolute(path))


def _format_protocol_url(protocol: str | None, path: str) -> str:
    """Format ``protocol`` and ``path`` with ``:`` for ``data`` URLs or ``://`` for other protocols."""
    if not protocol:
        return path
    sep = ":" if protocol == "data" else "://"
    return f"{protocol}{sep}{path}"


def normalise_dir(full_path: str | None) -> str:
    """Remove trailing slashes from a directory path or URL, keeping roots such as '/' and 'file:///'.

        Does not permit protocol-only paths, such as `https://`, `data:`, or `file://`

    :param full_path: directory path or URL; may include the protocol
    :type full_path: str | None
    :raises ValueError: if the path is empty or only contains a protocol
    :return: the normalised path
    :rtype : str
    """
    if full_path is None or not full_path.strip():
        raise ValueError(VALID_PATH_ONE_CHAR)
    full_path = full_path.strip()
    protocol, path = split_protocol(full_path)
    if not path:
        raise ValueError(PATH_EMPTY)
    stripped = path.rstrip("/")
    # path was '/' or multiple '/'s
    if path and not stripped:
        stripped = "/"
    return _format_protocol_url(protocol, stripped)


def check_input_dir(value: Any) -> Any:  # noqa: ANN401
    """Validate and normalise a local directory path or fsspec directory URL.

    Values that are not strings or ``Path`` objects are returned unchanged.
    Local paths (``Path`` objects, plain string paths, or ``file://`` URLs)
    must point to an existing directory and are resolved to an absolute path
    string. Non-local URLs are stripped of trailing slashes, checked against
    ``fsspec``'s registered protocols, and returned with a lower-case protocol.

    :param value: path or URL to validate
    :type value: Any
    :raises ValueError: if the path is empty, protocol-only, or uses an unknown or uninstalled fsspec protocol
    :raises PydanticCustomError: if a local path does not exist or is not a directory
    :return: resolved absolute local path string or normalised fsspec URL string
    :rtype: Any
    """
    if isinstance(value, Path):
        return _resolve_existing_local_dir(value)
    if not isinstance(value, str):
        return value
    value = value.strip()
    if is_local(value):
        return _resolve_existing_local_dir(Path(check_local_path(value)))
    normalised = normalise_dir(value)
    protocol = url_protocol(normalised)
    try:
        get_filesystem_class(protocol)
    except ImportError as e:
        raise ValueError(str(e)) from e
    _, path = split_protocol(normalised)
    return _format_protocol_url(protocol, path)


InputDir = Annotated[
    str,
    StringConstraints(strict=True),
    Field(
        description="Local directory or fsspec URL containing file(s) to import",
    ),
    BeforeValidator(check_input_dir),
]

LocalInputDir = Annotated[
    DirectoryPath,
    Field(
        description="Local directory containing file(s) to import",
    ),
    BeforeValidator(check_local_path),
    AfterValidator(to_absolute),
]

LocalOutputDir = Annotated[
    Path,
    Field(
        description="Local directory to save output and, if use_output_dir_for_pipeline_metadata is set, pipeline metadata to. Required. Use `use_destination` to set where dlt loads data.",
    ),
    BeforeValidator(check_local_path),
    AfterValidator(to_absolute),
    AfterValidator(ensure_dir),
]
