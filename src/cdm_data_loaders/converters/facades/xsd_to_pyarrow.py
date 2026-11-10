"""Convert XML Schema Definition (XSD) documents into PyArrow schemas and empty tables."""

from collections.abc import Mapping
from pathlib import Path
from typing import Any, ClassVar, Final

import pyarrow as pa
import xmlschema
from frozendict import frozendict
from pydantic import BaseModel, ConfigDict, Field, PositiveInt
from xmlschema.validators import XsdType

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.guards import DEFAULT_MAX_RECURSION_DEPTH
from cdm_data_loaders.converters.core.ir import SchemaDocument, TypedNode
from cdm_data_loaders.converters.emitters.pyarrow import PyArrowEmitter
from cdm_data_loaders.converters.readers.xsd import (
    XSD_SCALAR_SPEC,
    XsdReader,
    _simple_type_to_ir_node,
)

DEFAULT_EMITTER: Final[PyArrowEmitter] = PyArrowEmitter(max_nesting=100)

SCALAR_ARROW_TYPES: Final[Mapping[str, pa.DataType]] = frozendict(
    {
        xsd_name: DEFAULT_EMITTER.emit_node(TypedNode(type=node_type, hints=hints))
        for xsd_name, (node_type, hints) in XSD_SCALAR_SPEC.items()
    }
)


class XsdToPyArrowError(ConversionError):
    """Base class for XSD constructs that cannot be converted to a PyArrow schema.

    Each subclass carries a stable specification ``code`` and a short ``title``; its docstring states the reason
    the construct is rejected. The converter specification document is generated from these attributes.
    """

    code: ClassVar[str] = ""
    title: ClassVar[str] = ""


class XmlTagNotFoundError(XsdToPyArrowError):
    """The target element is not declared anywhere in the XSD."""

    code = "E1"
    title = "`xml_tag` not found"


class AmbiguousXmlTagError(XsdToPyArrowError):
    """Declarations of `xml_tag` at the shallowest matching depth have different types, so no single `pa.Schema` can be chosen."""

    code = "E2"
    title = "Ambiguous `xml_tag` type"


class NonStructXmlTagError(XsdToPyArrowError):
    """A `pa.Table` schema needs top-level columns: the type of `xml_tag` must yield at least one attribute, `#text`, or child element field."""

    code = "E3"
    title = "Non-struct top-level `xml_tag`"


class WildcardElementError(XsdToPyArrowError):
    """Arbitrary undeclared child elements cannot be represented as fixed `pa.struct` fields."""

    code = "E4"
    title = "Element wildcard (`xs:any`)"


class WildcardAttributeError(XsdToPyArrowError):
    """Arbitrary undeclared attributes cannot be represented as fixed `pa.struct` fields."""

    code = "E5"
    title = "Attribute wildcard (`xs:anyAttribute`)"


class AbstractComponentError(XsdToPyArrowError):
    """Concrete instances of an abstract element or type require `xsi:type` or substitution polymorphism, which has no single schema."""

    code = "E6"
    title = 'Abstract element or type (`abstract="true"`)'


class SubstitutionGroupError(XsdToPyArrowError):
    """Substituted elements introduce element names and types that are not declared in the parent content model."""

    code = "E7"
    title = "Substitution group"


class FieldNameCollisionError(XsdToPyArrowError):
    """An attribute (prefixed `_`) and a child element map to the same field name in one `pa.struct`."""

    code = "E8"
    title = "Attribute / child field name collision"


class RecursiveNonJsonTypeError(XsdToPyArrowError):
    """Subtrees truncated beyond `max_recursion_depth` are serialized with `json.dumps`, which cannot encode `datetime.date` or `bytes` without lossy conversion."""

    code = "E9"
    title = "Non-JSON-serializable scalar under recursion"


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

ERROR_BY_CODE: Final[frozendict[str, type[XsdToPyArrowError]]] = frozendict(
    {cls.code: cls for cls in XSD_TO_PYARROW_ERRORS}
)


def resolve_scalar_type(simple_type: XsdType) -> tuple[pa.DataType, str]:
    """Map a non-list XSD simpleType (atomic or union) to ``(pa.DataType, xsd_type_str)``.

    :param simple_type: XSD simpleType to resolve.
    :type simple_type: XsdType
    :return: Tuple of ``(pa.DataType, xsd_type_str)``.
    :rtype: tuple[pa.DataType, str]
    """
    node = _simple_type_to_ir_node(simple_type, (), nullable=True)
    arrow_type = DEFAULT_EMITTER.emit_node(node)
    xsd_type_str = str(node.provenance.metadata.get("xsd_type", "xs:string")) if node.provenance else "xs:string"
    return arrow_type, xsd_type_str


def generate_pyarrow_struct(
    schema: xmlschema.XMLSchema,
    xml_tag: str,
    *,
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
    struct_cache: dict[tuple[int, bool], Any] | None = None,
) -> pa.StructType:
    """Generate a reusable ``pa.StructType`` component for the target XML element from an XSD schema.

    :param schema: loaded XMLSchema instance
    :type  schema: xmlschema.XMLSchema
    :param xml_tag: target XML element name (local name or Clark-notation ``{ns}local``)
    :type  xml_tag: str
    :param max_recursion_depth: maximum structural recursion depth before JSON serialization
    :type  max_recursion_depth: int
    :param struct_cache: optional cache for sharing complex type components across calls
    :type  struct_cache: dict[tuple[int, bool], Any] | None
    :return: PyArrow StructType component whose fields correspond to the target element's fields
    :rtype : pa.StructType
    """
    doc = XsdReader(
        xml_tag=xml_tag,
        max_recursion_depth=max_recursion_depth,
        error_type=XsdToPyArrowError,
        error_by_code=ERROR_BY_CODE,
    ).read(
        schema,
        xml_tag,
        struct_cache=struct_cache,
    )
    emitted = DEFAULT_EMITTER.emit_node(doc.root)
    if not isinstance(emitted, pa.StructType):
        msg = f"Top-level xml_tag {xml_tag!r} did not emit a StructType"
        raise NonStructXmlTagError(msg)
    return emitted


def generate_pyarrow_schema(
    schema: xmlschema.XMLSchema,
    xml_tag: str,
    *,
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
) -> pa.Schema:
    """Generate a PyArrow schema for the target XML element from an XSD schema.

    :param schema: loaded XMLSchema instance
    :type  schema: xmlschema.XMLSchema
    :param xml_tag: target XML element name (local name or Clark-notation ``{ns}local``)
    :type  xml_tag: str
    :param max_recursion_depth: maximum structural recursion depth before JSON serialization
    :type  max_recursion_depth: int
    :return: PyArrow schema whose top-level fields correspond to the target element's fields
    :rtype : pa.Schema
    """
    doc = XsdReader(
        xml_tag=xml_tag,
        max_recursion_depth=max_recursion_depth,
        error_type=XsdToPyArrowError,
        error_by_code=ERROR_BY_CODE,
    ).read(schema, xml_tag)
    return DEFAULT_EMITTER.emit(doc)


def generate_pyarrow_schemas_at_level(
    schema: xmlschema.XMLSchema,
    level: int,
    *,
    root_tag: str | None = None,
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
) -> dict[str, pa.Schema]:
    """Generate PyArrow table schemas for all XML element types at depth ``level`` in an XSD schema.

    Reusable struct components are cached and shared across all table schemas assembled
    at ``level``. Elements whose XSD type resolves to a scalar or mixed-text leaf are represented as
    a single-column ``pa.Schema`` with a nullable ``#text`` field.

    :param schema: loaded XMLSchema instance
    :type  schema: xmlschema.XMLSchema
    :param level: non-negative depth relative to the root element(s)
    :type  level: int
    :param root_tag: optional explicit root element name to anchor the traversal
    :type  root_tag: str | None
    :param max_recursion_depth: maximum structural recursion depth before JSON serialization
    :type  max_recursion_depth: int
    :return: mapping of element local tag name (table name) to its assembled PyArrow schema
    :rtype : dict[str, pa.Schema]
    """
    docs = XsdReader(
        max_recursion_depth=max_recursion_depth,
        error_type=XsdToPyArrowError,
        error_by_code=ERROR_BY_CODE,
    ).read_at_level(schema, level, root_tag=root_tag)
    return {tag_name: DEFAULT_EMITTER.emit(doc) for tag_name, doc in docs.items()}


class XsdToPyArrow(BaseModel):
    """Facade converting XML Schema Definition (XSD) documents into PyArrow schemas and empty tables."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    xml_tag: str | None = None
    max_recursion_depth: PositiveInt = Field(default=DEFAULT_MAX_RECURSION_DEPTH)

    def _read(self, schema: xmlschema.XMLSchema | str | Path, xml_tag: str | None = None) -> SchemaDocument:
        """Read an XSD schema element into a converter IR ``SchemaDocument``.

        :param schema: Loaded ``XMLSchema`` instance or path to an XSD file.
        :type schema: xmlschema.XMLSchema | str | Path
        :param xml_tag: Target XML element name.
        :type xml_tag: str | None
        :return: Parsed ``SchemaDocument``.
        :rtype: SchemaDocument
        """
        return XsdReader(
            xml_tag=self.xml_tag,
            max_recursion_depth=self.max_recursion_depth,
            error_type=XsdToPyArrowError,
            error_by_code=ERROR_BY_CODE,
        ).read(schema, xml_tag)

    def _read_level(
        self,
        schema: xmlschema.XMLSchema | str | Path,
        level: int,
        *,
        root_tag: str | None = None,
    ) -> dict[str, SchemaDocument]:
        """Read all XML element types at depth ``level`` into ``SchemaDocument`` instances.

        :param schema: Loaded ``XMLSchema`` instance or path to an XSD file.
        :type schema: xmlschema.XMLSchema | str | Path
        :param level: Non-negative depth relative to the root element(s).
        :type level: int
        :param root_tag: Optional explicit root element name to anchor the traversal.
        :type root_tag: str | None
        :return: Mapping of element tag name to ``SchemaDocument``.
        :rtype: dict[str, SchemaDocument]
        """
        return XsdReader(
            max_recursion_depth=self.max_recursion_depth,
            error_type=XsdToPyArrowError,
            error_by_code=ERROR_BY_CODE,
        ).read_at_level(schema, level, root_tag=root_tag)

    def convert(self, schema: xmlschema.XMLSchema | str | Path, xml_tag: str | None = None) -> pa.Schema:
        """Convert an XSD schema element into a ``pa.Schema``.

        :param schema: loaded ``XMLSchema`` instance or path to an XSD file
        :type schema: xmlschema.XMLSchema | str | Path
        :param xml_tag: target XML element name (defaults to ``self.xml_tag`` or sole root element)
        :type xml_tag: str | None
        :return: converted PyArrow schema
        :rtype: pa.Schema
        """
        return DEFAULT_EMITTER.emit(self._read(schema, xml_tag))

    def convert_to_table(self, schema: xmlschema.XMLSchema | str | Path, xml_tag: str | None = None) -> pa.Table:
        """Convert an XSD schema element into a zero-row ``pa.Table``.

        :param schema: loaded ``XMLSchema`` instance or path to an XSD file
        :type schema: xmlschema.XMLSchema | str | Path
        :param xml_tag: target XML element name (defaults to ``self.xml_tag`` or sole root element)
        :type xml_tag: str | None
        :return: zero-row PyArrow table with the converted schema
        :rtype: pa.Table
        """
        return DEFAULT_EMITTER.emit_table(self._read(schema, xml_tag))

    def convert_to_python(
        self,
        schema: xmlschema.XMLSchema | str | Path,
        xml_tag: str | None = None,
        *,
        docstring: str | None = None,
    ) -> str:
        """Convert an XSD schema element into a static Python schema module.

        :param schema: loaded ``XMLSchema`` instance or path to an XSD file
        :type schema: xmlschema.XMLSchema | str | Path
        :param xml_tag: target XML element name (defaults to ``self.xml_tag`` or sole root element)
        :type xml_tag: str | None
        :param docstring: optional module docstring header
        :type docstring: str | None
        :return: formatted Python source code defining ``*_STRUCT``, ``*_SCHEMA``, ``SCHEMA``, and ``SCHEMAS``
        :rtype: str
        """
        return DEFAULT_EMITTER.emit_python(self._read(schema, xml_tag), docstring=docstring)

    def convert_to_python_file(
        self,
        schema: xmlschema.XMLSchema | str | Path,
        output: str | Path,
        xml_tag: str | None = None,
        *,
        docstring: str | None = None,
    ) -> Path:
        """Convert an XSD schema element and write a static Python schema module to ``output``.

        :param schema: loaded ``XMLSchema`` instance or path to an XSD file
        :type schema: xmlschema.XMLSchema | str | Path
        :param output: destination file path for the generated Python module
        :type output: str | Path
        :param xml_tag: target XML element name (defaults to ``self.xml_tag`` or sole root element)
        :type xml_tag: str | None
        :param docstring: optional module docstring header
        :type docstring: str | None
        :return: resolved output ``Path``
        :rtype: Path
        """
        return DEFAULT_EMITTER.emit_python_file(self._read(schema, xml_tag), output, docstring=docstring)

    def convert_level_to_python(
        self,
        schema: xmlschema.XMLSchema | str | Path,
        level: int = 1,
        *,
        root_tag: str | None = None,
        docstring: str | None = None,
    ) -> str:
        """Convert all XML element types at depth ``level`` in an XSD schema into a static Python schema module.

        :param schema: loaded ``XMLSchema`` instance or path to an XSD file
        :type schema: xmlschema.XMLSchema | str | Path
        :param level: non-negative depth relative to the root element(s)
        :type level: int
        :param root_tag: optional explicit root element name to anchor the traversal
        :type root_tag: str | None
        :param docstring: optional module docstring header
        :type docstring: str | None
        :return: formatted Python source code defining ``*_STRUCT``, ``*_SCHEMA``, ``SCHEMA``, and ``SCHEMAS``
        :rtype: str
        """
        return DEFAULT_EMITTER.emit_python(self._read_level(schema, level, root_tag=root_tag), docstring=docstring)

    def convert_level_to_python_file(
        self,
        schema: xmlschema.XMLSchema | str | Path,
        output: str | Path,
        level: int = 1,
        *,
        root_tag: str | None = None,
        docstring: str | None = None,
    ) -> Path:
        """Convert all XML element types at depth ``level`` in an XSD schema and write a Python module to ``output``.

        :param schema: loaded ``XMLSchema`` instance or path to an XSD file
        :type schema: xmlschema.XMLSchema | str | Path
        :param output: destination file path for the generated Python module
        :type output: str | Path
        :param level: non-negative depth relative to the root element(s)
        :type level: int
        :param root_tag: optional explicit root element name to anchor the traversal
        :type root_tag: str | None
        :param docstring: optional module docstring header
        :type docstring: str | None
        :return: resolved output ``Path``
        :rtype: Path
        """
        return DEFAULT_EMITTER.emit_python_file(
            self._read_level(schema, level, root_tag=root_tag),
            output,
            docstring=docstring,
        )
