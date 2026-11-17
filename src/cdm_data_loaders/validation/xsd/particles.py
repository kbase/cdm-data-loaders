"""XSD model-group particle cardinality computation and wildcard/abstract validation."""

from collections.abc import Mapping
from typing import NoReturn

import xmlschema
from frozendict import frozendict
from xmlschema.validators import (
    XsdAnyAttribute,
    XsdAnyElement,
    XsdElement,
    XsdGroup,
)


def raise_xsd_code(
    code: str,
    msg: str,
    *,
    error_type: type[Exception] = ValueError,
    error_by_code: Mapping[str, type[Exception]] = frozendict(),
) -> NoReturn:
    """Raise the configured exception subclass for ``code`` or fall back to ``error_type``.

    :param code: specification error code (``E1``-``E9``)
    :type code: str
    :param msg: diagnostic error message
    :type msg: str
    :param error_type: fallback exception class
    :type error_type: type[Exception]
    :param error_by_code: mapping from specification error code to exception class
    :type error_by_code: Mapping[str, type[Exception]]
    :raises Exception: always raised using the resolved error class
    """
    cls = error_by_code.get(code, error_type)
    raise cls(msg)


def group_min_occurs_by_name(group: XsdGroup, local_name: str) -> int:
    """Compute minimum occurrences of elements with ``local_name`` across a model group.

    :param group: XSD model group (``sequence``, ``choice``, or ``all``)
    :type group: XsdGroup
    :param local_name: target child element local name
    :type local_name: str
    :return: minimum number of occurrences across ``group``
    :rtype: int
    """
    if group.min_occurs == 0 or group.max_occurs == 0:
        return 0
    target_group = group.ref if group.ref is not None else group
    counts: list[int] = []
    for item in target_group.content:
        if isinstance(item, XsdGroup):
            counts.append(group_min_occurs_by_name(item, local_name))
        elif isinstance(item, XsdElement) and item.max_occurs != 0 and item.local_name == local_name:
            counts.append(item.min_occurs)
        else:
            counts.append(0)
    if not counts:
        return 0
    branch_min = min(counts) if target_group.model == "choice" else sum(counts)
    return group.min_occurs * branch_min


def particle_max_occurs_by_name(item: object, local_name: str) -> int | None:
    """Compute maximum occurrences (``None`` for unbounded) of ``local_name`` for a single particle.

    :param item: group particle (``XsdGroup``, ``XsdElement``, or wildcard)
    :type item: object
    :param local_name: target child element local name
    :type local_name: str
    :return: maximum occurrences, or ``None`` if unbounded
    :rtype: int | None
    """
    if isinstance(item, XsdGroup):
        return group_max_occurs_by_name(item, local_name)
    if isinstance(item, XsdElement) and item.max_occurs != 0 and item.local_name == local_name:
        return item.max_occurs
    return 0


def group_max_occurs_by_name(group: XsdGroup, local_name: str) -> int | None:
    """Compute maximum occurrences (``None`` for unbounded) of elements with ``local_name`` across a model group.

    :param group: XSD model group
    :type group: XsdGroup
    :param local_name: target child element local name
    :type local_name: str
    :return: maximum occurrences across ``group``, or ``None`` if unbounded
    :rtype: int | None
    """
    if group.max_occurs == 0:
        return 0
    target_group = group.ref if group.ref is not None else group
    branch_maxes = [particle_max_occurs_by_name(item, local_name) for item in target_group.content]
    if not branch_maxes:
        return 0
    if any(m is None for m in branch_maxes):
        inner_max: int | None = None
    elif target_group.model == "choice":
        inner_max = max(m for m in branch_maxes if m is not None)
    else:
        inner_max = sum(m for m in branch_maxes if m is not None)
    if inner_max == 0:
        return 0
    if inner_max is None or group.max_occurs is None:
        return None
    return group.max_occurs * inner_max


def is_any_element(item: object) -> bool:
    """Return ``True`` if ``item`` is an ``xs:any`` wildcard particle.

    :param item: XSD model-group item
    :type item: object
    :return: ``True`` when ``item`` is an ``XsdAnyElement``
    :rtype: bool
    """
    return isinstance(item, XsdAnyElement)


def is_any_attribute(attr: object) -> bool:
    """Return ``True`` if ``attr`` is an ``xs:anyAttribute`` wildcard declaration.

    :param attr: XSD attribute entry
    :type attr: object
    :return: ``True`` when ``attr`` is an ``XsdAnyAttribute``
    :rtype: bool
    """
    return isinstance(attr, XsdAnyAttribute)


def validate_group_particles(
    schema: xmlschema.XMLSchema,
    group: XsdGroup,
    *,
    error_type: type[Exception] = ValueError,
    error_by_code: Mapping[str, type[Exception]] = frozendict(),
) -> None:
    """Validate that a model group contains no wildcards, abstract elements/types, or substitution groups.

    :param schema: loaded ``XMLSchema`` instance
    :type schema: xmlschema.XMLSchema
    :param group: XSD model group to validate
    :type group: XsdGroup
    :param error_type: fallback exception class
    :type error_type: type[Exception]
    :param error_by_code: mapping of specification error codes to exception classes
    :type error_by_code: Mapping[str, type[Exception]]
    """
    target_group = group.ref if group.ref is not None else group
    for item in target_group.content:
        if is_any_element(item):
            msg = "Unsupported XSD feature: xs:any wildcard element is not supported"
            raise_xsd_code("E4", msg, error_type=error_type, error_by_code=error_by_code)
        if isinstance(item, XsdGroup):
            validate_group_particles(schema, item, error_type=error_type, error_by_code=error_by_code)
        elif isinstance(item, XsdElement):
            target_el = item.ref if item.ref is not None else item
            if getattr(item, "abstract", False) or getattr(target_el, "abstract", False):
                msg = f"Unsupported XSD feature: abstract element {item.local_name!r} is not supported"
                raise_xsd_code("E6", msg, error_type=error_type, error_by_code=error_by_code)
            if (
                item.name in schema.substitution_groups
                or target_el.name in schema.substitution_groups
                or getattr(item, "substitution_group", None) is not None
                or getattr(target_el, "substitution_group", None) is not None
            ):
                msg = f"Unsupported XSD feature: substitutionGroup on element {item.local_name!r} is not supported"
                raise_xsd_code("E7", msg, error_type=error_type, error_by_code=error_by_code)
            if item.type is not None and getattr(item.type, "abstract", False):
                msg = f"Unsupported XSD feature: abstract type {item.type.local_name!r} is not supported"
                raise_xsd_code("E6", msg, error_type=error_type, error_by_code=error_by_code)


def resolve_child_cardinality(
    model_group: XsdGroup,
    child_decl: XsdElement,
    *,
    parent_nillable: bool = False,
) -> tuple[bool, bool]:
    """Return ``(is_list, nullable)`` for ``child_decl`` within ``model_group``.

    :param model_group: parent XSD model group
    :type model_group: XsdGroup
    :param child_decl: child XSD element declaration
    :type child_decl: XsdElement
    :param parent_nillable: whether an enclosing nillable parent forces child fields nullable
    :type parent_nillable: bool
    :return: tuple ``(is_list, nullable)``
    :rtype: tuple[bool, bool]
    """
    child_name = child_decl.local_name or ""
    max_occurs = group_max_occurs_by_name(model_group, child_name)
    min_occurs = group_min_occurs_by_name(model_group, child_name)
    is_list = max_occurs is None or max_occurs > 1
    nullable = parent_nillable or child_decl.nillable or min_occurs < 1
    return is_list, nullable
