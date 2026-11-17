"""Exception hierarchy for the XML-to-PyArrow ingestion pipeline."""

from typing import ClassVar, Final

from cdm_data_loaders.core.cli import SpecError


class XmlToPyArrowConfigError(SpecError, ValueError):
    """Base error raised when XML-to-PyArrow target selection, XSD schema resolution, or hook validation fails."""


class InvalidXmlTargetSelectionError(XmlToPyArrowConfigError):
    """`xml_level` cannot be combined with `xml_tag` or `table_name`; `xml_tag` and `table_name` are both required when `xml_level` is not set; `root_tag` is only valid when `xml_level` is set."""

    code: ClassVar[str] = "E1"
    title: ClassVar[str] = "Invalid or mutually exclusive XML target selection"


class InvalidXsdDocumentError(XmlToPyArrowConfigError):
    """`xsd_file` must be a well-formed XML document and a valid W3C XML Schema (`xs:schema`)."""

    code: ClassVar[str] = "E2"
    title: ClassVar[str] = "Malformed XML or invalid W3C XML Schema"


class XmlTargetNotFoundError(XmlToPyArrowConfigError):
    """The configured `xml_tag`, `root_tag`, or `xml_level` does not resolve to any element declaration in the XSD."""

    code: ClassVar[str] = "E3"
    title: ClassVar[str] = "Target XML tag, root tag, or level not found in XSD"


class UnsupportedXsdTargetError(XmlToPyArrowConfigError):
    """The target element must map to a PyArrow struct table schema and cannot contain unsupported XSD constructs (such as scalar-only roots or recursive non-JSON types)."""

    code: ClassVar[str] = "E4"
    title: ClassVar[str] = "Target element or descendant uses an unsupported XSD construct"


class InvalidXmlPipelineHookError(XmlToPyArrowConfigError):
    """Configured `preprocessors_module`, `transforms_module`, or `output_schemas_module` references unknown table names or omits required output schemas."""

    code: ClassVar[str] = "E5"
    title: ClassVar[str] = "Invalid pre-processor, transform, or output schema hook"


XMLTOPYARROW_CONFIG_ERRORS: Final[tuple[type[XmlToPyArrowConfigError], ...]] = (
    InvalidXmlTargetSelectionError,
    InvalidXsdDocumentError,
    XmlTargetNotFoundError,
    UnsupportedXsdTargetError,
    InvalidXmlPipelineHookError,
)
