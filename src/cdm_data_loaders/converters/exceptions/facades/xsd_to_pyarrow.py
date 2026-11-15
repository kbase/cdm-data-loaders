"""Exception hierarchy for the XSD-to-PyArrow converter."""

from typing import ClassVar, Final

from frozendict import frozendict

from cdm_data_loaders.converters.core.errors import ConversionError, build_error_by_code


class XsdToPyArrowError(ConversionError):
    """Base class for XSD constructs that cannot be converted to a PyArrow schema.

    Each subclass carries a stable specification ``code`` and a short ``title``; its docstring states the reason
    the construct is rejected. The converter specification document is generated from these attributes.
    """


class XmlTagNotFoundError(XsdToPyArrowError):
    """The target element is not declared anywhere in the XSD."""

    code: ClassVar[str] = "E1"
    title: ClassVar[str] = "`xml_tag` not found"


class AmbiguousXmlTagError(XsdToPyArrowError):
    """Declarations of `xml_tag` at the shallowest matching depth have different types, so no single `pa.Schema` can be chosen."""

    code: ClassVar[str] = "E2"
    title: ClassVar[str] = "Ambiguous `xml_tag` type"


class NonStructXmlTagError(XsdToPyArrowError):
    """A `pa.Table` schema needs top-level columns: the type of `xml_tag` must yield at least one attribute, `#text`, or child element field."""

    code: ClassVar[str] = "E3"
    title: ClassVar[str] = "Non-struct top-level `xml_tag`"


class WildcardElementError(XsdToPyArrowError):
    """Arbitrary undeclared child elements cannot be represented as fixed `pa.struct` fields."""

    code: ClassVar[str] = "E4"
    title: ClassVar[str] = "Element wildcard (`xs:any`)"


class WildcardAttributeError(XsdToPyArrowError):
    """Arbitrary undeclared attributes cannot be represented as fixed `pa.struct` fields."""

    code: ClassVar[str] = "E5"
    title: ClassVar[str] = "Attribute wildcard (`xs:anyAttribute`)"


class AbstractComponentError(XsdToPyArrowError):
    """Concrete instances of an abstract element or type require `xsi:type` or substitution polymorphism, which has no single schema."""

    code: ClassVar[str] = "E6"
    title: ClassVar[str] = 'Abstract element or type (`abstract="true"`)'


class SubstitutionGroupError(XsdToPyArrowError):
    """Substituted elements introduce element names and types that are not declared in the parent content model."""

    code: ClassVar[str] = "E7"
    title: ClassVar[str] = "Substitution group"


class FieldNameCollisionError(XsdToPyArrowError):
    """An attribute (prefixed `_`) and a child element map to the same field name in one `pa.struct`."""

    code: ClassVar[str] = "E8"
    title: ClassVar[str] = "Attribute / child field name collision"


class RecursiveNonJsonTypeError(XsdToPyArrowError):
    """Subtrees truncated beyond `max_recursion_depth` are serialized with `json.dumps`, which cannot encode `datetime.date` or `bytes` without lossy conversion."""

    code: ClassVar[str] = "E9"
    title: ClassVar[str] = "Non-JSON-serializable scalar under recursion"


XSD_TO_PYARROW_ERRORS: Final[tuple[type[XsdToPyArrowError], ...]] = (
    XmlTagNotFoundError,
    AmbiguousXmlTagError,
    NonStructXmlTagError,
    WildcardElementError,
    WildcardAttributeError,
    AbstractComponentError,
    SubstitutionGroupError,
    FieldNameCollisionError,
    RecursiveNonJsonTypeError,
)

ERROR_BY_CODE: Final[frozendict[str, type[XsdToPyArrowError]]] = build_error_by_code(XSD_TO_PYARROW_ERRORS)
