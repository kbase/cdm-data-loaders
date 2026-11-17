"""Exception hierarchy for the XSV (CSV/TSV) ingestion pipeline."""

from typing import ClassVar, Final

from cdm_data_loaders.core.cli import SpecError


class XsvSchemaError(SpecError, RuntimeError):
    """Raised when an XSV JSON Schema cannot be validated or converted to a PyArrow schema.

    Subclasses carry a stable specification ``code`` and a short ``title``; their docstrings state
    why the schema is rejected. The XSV specification document is generated from these attributes.
    """


class UnsupportedXsvDialectError(XsvSchemaError):
    """Only JSON Schema Draft 2020-12 and the KBase Draft 2020-12 XSV metaschema are supported."""

    code: ClassVar[str] = "E1"
    title: ClassVar[str] = "Missing or unsupported `$schema` dialect"


class MissingRequiredColumnsError(XsvSchemaError):
    """XSV schemas must declare a non-empty list of column headers in the top-level `required` array."""

    code: ClassVar[str] = "E2"
    title: ClassVar[str] = "Missing or empty `required` column list"


class InvalidXsvSchemaDefinitionError(XsvSchemaError):
    """The document is not valid JSON Schema or contains an invalid `x-xsv-config` parser block."""

    code: ClassVar[str] = "E3"
    title: ClassVar[str] = "Invalid JSON Schema or `x-xsv-config` block"


class UnresolvableXsvRefError(XsvSchemaError):
    """All `$ref` pointers in an XSV schema must resolve against local `$defs` before conversion."""

    code: ClassVar[str] = "E4"
    title: ClassVar[str] = "Unresolvable `$ref` pointer"


class UnsupportedXsvArrowConstructError(XsvSchemaError):
    """Every XSV column must convert to a supported PyArrow type under a top-level `object` schema."""

    code: ClassVar[str] = "E5"
    title: ClassVar[str] = "Unsupported PyArrow column construct"


XSV_SCHEMA_ERRORS: Final[tuple[type[XsvSchemaError], ...]] = (
    UnsupportedXsvDialectError,
    MissingRequiredColumnsError,
    InvalidXsvSchemaDefinitionError,
    UnresolvableXsvRefError,
    UnsupportedXsvArrowConstructError,
)
