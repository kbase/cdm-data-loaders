"""Built-in XSD simpleType scalar, list, union, and documentation helpers."""

from collections.abc import Mapping
from typing import Any, Final, Literal

from frozendict import frozendict
from xmlschema.validators import XsdSimpleType, XsdType

XSD_NAMESPACE: Final[str] = "{http://www.w3.org/2001/XMLSchema}"

FORBIDDEN_RECURSIVE_XSD_TYPES: Final[frozenset[str]] = frozenset(
    {
        "xs:date",
        "xs:dateTime",
        "xs:hexBinary",
        "xs:base64Binary",
    }
)

BUILTIN_LIST_ITEM_TYPES: Final[Mapping[str, str]] = frozendict(
    {
        "xs:NMTOKENS": "xs:NMTOKEN",
        "xs:IDREFS": "xs:IDREF",
        "xs:ENTITIES": "xs:ENTITY",
    }
)

type XsdScalarKind = Literal[
    "string",
    "boolean",
    "int8",
    "int16",
    "int32",
    "int64",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float64",
    "date",
    "binary",
]

XSD_SCALAR_KINDS: Final[Mapping[str, XsdScalarKind]] = frozendict(
    {
        **dict.fromkeys(
            (
                "xs:string",
                "xs:normalizedString",
                "xs:token",
                "xs:language",
                "xs:Name",
                "xs:NCName",
                "xs:NMTOKEN",
                "xs:ID",
                "xs:IDREF",
                "xs:ENTITY",
                "xs:anyURI",
                "xs:QName",
                "xs:NOTATION",
                "xs:dateTime",
                "xs:time",
                "xs:duration",
                "xs:gYear",
                "xs:gYearMonth",
                "xs:gMonthDay",
                "xs:gDay",
                "xs:gMonth",
            ),
            "string",
        ),
        "xs:boolean": "boolean",
        "xs:byte": "int8",
        "xs:short": "int16",
        "xs:int": "int32",
        **dict.fromkeys(
            (
                "xs:long",
                "xs:integer",
                "xs:positiveInteger",
                "xs:nonNegativeInteger",
                "xs:negativeInteger",
                "xs:nonPositiveInteger",
            ),
            "int64",
        ),
        "xs:unsignedByte": "uint8",
        "xs:unsignedShort": "uint16",
        "xs:unsignedInt": "uint32",
        "xs:unsignedLong": "uint64",
        **dict.fromkeys(("xs:float", "xs:double", "xs:decimal"), "float64"),
        "xs:date": "date",
        **dict.fromkeys(("xs:base64Binary", "xs:hexBinary"), "binary"),
    }
)

INTEGER_SCALAR_KINDS: Final[frozenset[XsdScalarKind]] = frozenset(
    {"int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64"}
)


def extract_doc_comment(component: object, fallback_type: object = None) -> str | None:
    """Extract whitespace-normalized ``<xs:documentation>`` text from an XSD component or its type.

    :param component: XSD element or attribute component
    :type component: object
    :param fallback_type: fallback XSD type inspected when ``component`` has no documentation
    :type fallback_type: object
    :return: normalized documentation string, or ``None`` when absent
    :rtype: str | None
    """
    candidates = [component]
    ref = getattr(component, "ref", None)
    if ref is not None and ref is not component:
        candidates.append(ref)
    if fallback_type is not None:
        candidates.append(fallback_type)

    for candidate in candidates:
        annotation = getattr(candidate, "annotation", None)
        docs = getattr(annotation, "documentation", None)
        if not docs:
            continue
        parts = [" ".join(doc.text.split()) for doc in docs if getattr(doc, "text", None) and doc.text.strip()]
        if parts:
            return " ".join(parts)
    return None


def builtin_xsd_name(simple_type: XsdType) -> str:
    """Resolve an atomic simpleType or restriction to its built-in ``xs:<name>`` identifier.

    :param simple_type: XSD simpleType or restriction
    :type simple_type: XsdType
    :return: built-in ``xs:<name>`` identifier, defaulting to ``'xs:string'``
    :rtype: str
    """
    cur: Any = simple_type
    while cur is not None:
        name = getattr(cur, "name", None)
        if isinstance(name, str) and name.startswith(XSD_NAMESPACE):
            return f"xs:{cur.local_name}"
        cur = getattr(cur, "base_type", None)
    return "xs:string"


def unwrap_member_types(simple_type: XsdType) -> list[XsdSimpleType]:
    """Return the member types of an ``xs:union`` simpleType, unwrapping restrictions.

    :param simple_type: XSD union simpleType or restriction
    :type simple_type: XsdType
    :return: ordered list of union member simpleTypes
    :rtype: list[XsdSimpleType]
    """
    cur: Any = simple_type
    while cur is not None:
        members = getattr(cur, "member_types", None)
        if members:
            return list(members)
        cur = getattr(cur, "base_type", None)
    return []


def unwrap_item_type(simple_type: XsdType) -> XsdSimpleType | None:
    """Return the item type of an ``xs:list`` simpleType, unwrapping restrictions.

    :param simple_type: XSD list simpleType or restriction
    :type simple_type: XsdType
    :return: list item simpleType, or ``None`` if unavailable
    :rtype: XsdSimpleType | None
    """
    cur: Any = simple_type
    while cur is not None:
        item_type = getattr(cur, "item_type", None)
        if item_type is not None:
            return item_type
        cur = getattr(cur, "base_type", None)
    return None


def resolve_xsd_scalar_kind(simple_type: XsdType) -> tuple[XsdScalarKind, str]:
    """Map a non-list XSD simpleType (atomic or union) to ``(XsdScalarKind, xsd_type_str)``.

    :param simple_type: atomic or union XSD simpleType
    :type simple_type: XsdType
    :return: tuple of ``(scalar_kind, xsd_type_str)``
    :rtype: tuple[XsdScalarKind, str]
    """
    if simple_type.is_union():
        members = unwrap_member_types(simple_type)
        member_kinds = [resolve_xsd_scalar_kind(m) for m in members]
        xsd_type_str = " | ".join(xsd_name for _, xsd_name in member_kinds)
        unique_kinds = {kind for kind, _ in member_kinds}
        scalar_kind: XsdScalarKind = next(iter(unique_kinds)) if len(unique_kinds) == 1 else "string"
        return scalar_kind, xsd_type_str

    xsd_name = builtin_xsd_name(simple_type)
    scalar_kind = XSD_SCALAR_KINDS.get(xsd_name, "string")
    return scalar_kind, xsd_name


def iter_simple_builtin_names(simple_type: XsdType) -> list[str]:
    """Return all constituent built-in ``xs:*`` type names for an atomic, list, or union simpleType.

    :param simple_type: XSD simpleType to inspect
    :type simple_type: XsdType
    :return: list of constituent built-in ``xs:*`` names
    :rtype: list[str]
    """
    builtin_name = builtin_xsd_name(simple_type)
    if builtin_name in BUILTIN_LIST_ITEM_TYPES:
        return [BUILTIN_LIST_ITEM_TYPES[builtin_name]]
    if simple_type.is_list():
        item_type = unwrap_item_type(simple_type)
        return iter_simple_builtin_names(item_type) if item_type is not None else ["xs:string"]
    if simple_type.is_union():
        names: list[str] = []
        for member in unwrap_member_types(simple_type):
            names.extend(iter_simple_builtin_names(member))
        return names
    return [builtin_name]
