"""Render and write static Python schema modules from converter IR documents."""

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING

from cdm_data_loaders.converters.core.ir import SchemaDocument
from cdm_data_loaders.converters.emitters.pyarrow_codegen.collector import IRComponentCollector
from cdm_data_loaders.converters.emitters.pyarrow_codegen.formatting import (
    format_schema_exports,
    format_struct_assignment,
    resolve_field_placeholders,
)

if TYPE_CHECKING:
    from cdm_data_loaders.converters.emitters.pyarrow import PyArrowEmitter
    from cdm_data_loaders.converters.emitters.pyarrow_codegen.naming import ComponentKey, RenderedFieldSpec


def render_python_schema_module(
    emitter: "PyArrowEmitter",
    document: SchemaDocument | Mapping[str, SchemaDocument],
    *,
    docstring: str | None = None,
) -> str:
    """Render a formatted Python schema module from one or more ``SchemaDocument`` instances.

    :param emitter: ``PyArrowEmitter`` instance used to resolve Arrow types and field metadata.
    :type emitter: PyArrowEmitter
    :param document: A single ``SchemaDocument`` or a mapping of table names to ``SchemaDocument`` objects.
    :type document: SchemaDocument | Mapping[str, SchemaDocument]
    :param docstring: Optional module docstring header.
    :type docstring: str | None
    :return: Formatted Python module source code defining ``*_STRUCT``, ``*_SCHEMA``, ``SCHEMA``, and ``SCHEMAS``.
    :rtype: str
    :raises ValueError: If ``document`` is an empty mapping.
    """
    if isinstance(document, SchemaDocument):
        tag_name = document.name or "root"
        docs_by_name: dict[str, SchemaDocument] = {tag_name: document}
        default_doc = (
            f"PyArrow schema and reusable struct components for {document.name}."
            if document.name
            else "PyArrow schema and reusable struct components."
        )
    else:
        docs_by_name = dict(document)
        if not docs_by_name:
            msg = "Cannot emit a Python schema module from an empty SchemaDocument mapping"
            raise ValueError(msg)
        default_doc = "PyArrow schemas and reusable struct components."

    module_doc = docstring if docstring is not None else default_doc
    pa_schemas = {name: emitter.emit(doc) for name, doc in docs_by_name.items()}

    collector = IRComponentCollector()
    for name, doc in docs_by_name.items():
        collector.register_document(name, doc)

    table_roots: dict[str, ComponentKey | RenderedFieldSpec] = {}
    for name, doc in docs_by_name.items():
        table_roots[name] = collector.collect_document(name, doc, pa_schemas[name])

    component_names = collector.assign_component_names()

    lines: list[str] = [
        f'"""{module_doc}"""',
        "",
        "from typing import Final",
        "",
        "import pyarrow as pa",
        "",
        "from cdm_data_loaders.utils.arrow_schema import assemble_pyarrow_schema",
        "",
    ]

    for key in collector.order:
        const_name = component_names[key]
        comp_spec = collector.components[key]
        resolved_fields = [resolve_field_placeholders(f, component_names) for f in comp_spec.fields]
        lines.append(format_struct_assignment(const_name, resolved_fields))
        lines.append("")

    lines.extend(format_schema_exports(table_roots, component_names))
    return "\n".join(lines)


def write_python_schema_file(
    emitter: "PyArrowEmitter",
    document: SchemaDocument | Mapping[str, SchemaDocument],
    output: str | Path,
    *,
    docstring: str | None = None,
) -> Path:
    """Render and write a static Python schema module to ``output``.

    :param emitter: ``PyArrowEmitter`` instance used to resolve Arrow types and field metadata.
    :type emitter: PyArrowEmitter
    :param document: A single ``SchemaDocument`` or a mapping of table names to ``SchemaDocument`` objects.
    :type document: SchemaDocument | Mapping[str, SchemaDocument]
    :param output: Destination file path for the generated Python module.
    :type output: str | Path
    :param docstring: Optional module docstring header.
    :type docstring: str | None
    :return: Resolved output ``Path``.
    :rtype: Path
    """
    out_path = Path(output)
    source = render_python_schema_module(emitter, document, docstring=docstring)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(source, encoding="utf-8")
    return out_path
