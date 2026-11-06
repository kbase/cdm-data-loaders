"""Generate component-based PyArrow schema modules from XSD schemas.

Usage::

    uv run python -m scripts.gen_pyarrow_schema            # write every configured schema.py in CONFIGS
    uv run python -m scripts.gen_pyarrow_schema --check    # exit 1 if a committed schema.py is stale
    uv run python -m scripts.gen_pyarrow_schema --xsd-file <xsd> --output <py> [--xml-level 1] [--root-tag <tag>]
"""

import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Annotated, Final, Self

import xmlschema
from pydantic import BaseModel, ConfigDict, Field, FilePath, PositiveInt, model_validator
from pydantic_settings import BaseSettings, CliApp, CliImplicitFlag, SettingsConfigDict
from xmlschema.validators import XsdElement

from cdm_data_loaders.converters.core.ir import Provenance, SchemaDocument
from cdm_data_loaders.converters.emitters._pyarrow_codegen import (
    MAX_LINE_LENGTH,
    PA_SCALAR_EXPR,
    TYPE_SUFFIX,
    ComponentKey,
    _decode_metadata,
    _fmt_str,
    _format_field_args_single,
    _format_field_indented,
    _format_metadata_single,
    _format_schema_exports,
    _format_struct_assignment,
    _format_type_arg_indented,
    _IRComponentCollector,
    _render_field_call,
    _render_struct_expr,
    _render_type_expr,
    _RenderedFieldSpec,
    _replace_placeholders,
    _resolve_field_placeholders,
    _StructComponentSpec,
    _to_upper_snake,
)
from cdm_data_loaders.converters.emitters.pyarrow import PyArrowEmitter
from cdm_data_loaders.converters.readers.xsd import (
    _build_element_struct_node,
    _build_element_table_doc,
    _TraversalContext,
)
from cdm_data_loaders.converters.xsd_to_pyarrow import (
    DEFAULT_MAX_RECURSION_DEPTH,
    NonStructXmlTagError,
    XsdToPyArrow,
    _is_empty_marker_complex_type,
    _require_complex_type,
)
from cdm_data_loaders.core.fields import NonEmptyStr
from cdm_data_loaders.core.settings import DEFAULT_SETTINGS_CONFIG_DICT

REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
SCHEMAS_DIR: Final[Path] = REPO_ROOT / "src" / "cdm_data_loaders" / "schemas"
_DEFAULT_EMITTER: Final[PyArrowEmitter] = PyArrowEmitter(max_nesting=100)


class SchemaCodegenConfig(BaseModel):
    """Configuration for generating a component-based PyArrow ``schema.py`` module from an XSD file."""

    model_config = ConfigDict(frozen=True)

    xsd_file: Path
    output: Path
    docstring: str
    xml_level: int | None = 1
    xml_tag: str | None = None
    root_tag: str | None = None
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH


class GenPyArrowSchemaSettings(BaseSettings):
    """CLI settings for generating or checking component-based PyArrow schema modules."""

    model_config = SettingsConfigDict(
        **DEFAULT_SETTINGS_CONFIG_DICT,
        cli_prog_name="gen_pyarrow_schema",
        cli_implicit_flags=True,
        extra="forbid",
    )

    check: CliImplicitFlag[bool] = Field(
        default=False,
        description="Fail if a committed schema.py is out of date.",
    )
    xsd_file: Annotated[
        FilePath | None,
        Field(
            default=None,
            description="Path to an XSD file for custom schema generation.",
        ),
    ]
    output: Annotated[
        Path | None,
        Field(
            default=None,
            description="Output Python file path when --xsd-file is provided.",
        ),
    ]
    xml_level: Annotated[
        PositiveInt,
        Field(
            default=1,
            description="Element depth relative to root (default: 1).",
        ),
    ]
    xml_tag: Annotated[
        NonEmptyStr | None,
        Field(
            default=None,
            description="Single target XML tag (overrides --xml-level).",
        ),
    ]
    root_tag: Annotated[
        NonEmptyStr | None,
        Field(
            default=None,
            description="Explicit root XML tag for level discovery.",
        ),
    ]
    max_recursion_depth: Annotated[
        PositiveInt,
        Field(
            default=DEFAULT_MAX_RECURSION_DEPTH,
            description="Maximum recursion depth before JSON string truncation.",
        ),
    ]

    @model_validator(mode="after")
    def validate_output_for_custom_xsd(self) -> Self:
        """Ensure ``output`` is provided when ``xsd_file`` is specified.

        :return: Validated settings instance.
        :rtype: Self
        :raises ValueError: If ``xsd_file`` is set without ``output``.
        """
        if self.xsd_file is not None and self.output is None:
            msg = "--output is required when --xsd-file is specified"
            raise ValueError(msg)
        return self


class _ComponentCollector:
    """Traverse XSD elements via ``XsdReader`` IR and collect reusable ``pa.StructType`` components in post-order."""

    def __init__(
        self,
        schema: xmlschema.XMLSchema,
        *,
        max_recursion_depth: int,
    ) -> None:
        """Initialize the component collector.

        :param schema: Loaded XMLSchema instance.
        :type schema: xmlschema.XMLSchema
        :param max_recursion_depth: Maximum allowed occurrences of any complex type along a root-to-leaf path.
        :type max_recursion_depth: int
        """
        self.schema = schema
        self.max_recursion_depth = max_recursion_depth
        self._ir_collector = _IRComponentCollector()
        self.components = self._ir_collector.components
        self.order = self._ir_collector.order
        self.el_name_types = self._ir_collector.el_name_types

    def register_element_tree(self, el: XsdElement) -> None:
        """Pre-scan ``el`` and its descendants to count how many distinct complex types share each element name.

        :param el: Root or table-level XSD element to scan.
        :type el: XsdElement
        """
        tag_name = el.local_name or "root"
        doc = _build_element_table_doc(
            self.schema,
            el,
            tag_name,
            max_recursion_depth=self.max_recursion_depth,
            struct_cache={},
        )
        self._ir_collector.register_document(tag_name, doc)

    def assign_component_names(self) -> dict[ComponentKey, str]:
        """Assign unique ``*_STRUCT`` constant names to all collected components in post-order.

        :return: Mapping from ``ComponentKey`` to Python constant name.
        :rtype: dict[ComponentKey, str]
        """
        return self._ir_collector.assign_component_names()

    def collect_top_level_element(
        self,
        target_elem: XsdElement,
        tag_name: str,
        *,
        allow_scalar_leaf: bool = True,
    ) -> ComponentKey | _RenderedFieldSpec:
        """Collect components for a top-level element and return its ``ComponentKey`` or scalar ``#text`` field.

        :param target_elem: Resolved table-level XSD element.
        :type target_elem: XsdElement
        :param tag_name: Local XML tag name of ``target_elem``.
        :type tag_name: str
        :param allow_scalar_leaf: Whether scalar/mixed leaf elements are allowed as ``#text`` tables.
        :type allow_scalar_leaf: bool
        :return: ``ComponentKey`` for complex struct elements, or a ``#text`` ``_RenderedFieldSpec`` for scalar elements.
        :rtype: ComponentKey | _RenderedFieldSpec
        :raises NonStructXmlTagError: If ``target_elem`` has an empty marker complex type or a disallowed scalar type.
        """
        if allow_scalar_leaf:
            doc = _build_element_table_doc(
                self.schema,
                target_elem,
                tag_name,
                max_recursion_depth=self.max_recursion_depth,
                struct_cache={},
            )
        else:
            if isinstance(target_elem.type, xmlschema.validators.XsdComplexType) and _is_empty_marker_complex_type(
                target_elem.type
            ):
                msg = f"Top-level xml_tag {tag_name!r} has an empty complex type with no attributes or child elements"
                raise NonStructXmlTagError(msg)
            elem_type = _require_complex_type(target_elem.type, tag_name)
            ctx = _TraversalContext(
                schema=self.schema,
                type_counts={id(elem_type): 1},
                max_recursion_depth=self.max_recursion_depth,
                struct_cache={},
            )
            root_node = _build_element_struct_node(target_elem, tag_name, ctx)
            doc = SchemaDocument(root=root_node, name=tag_name, provenance=Provenance("xsd", (tag_name,)))
        pa_schema = _DEFAULT_EMITTER.emit(doc)
        return self._ir_collector.collect_document(tag_name, doc, pa_schema)


def render_schema_module(config: SchemaCodegenConfig) -> str:
    """Render a complete, formatted Python schema module from ``config``.

    :param config: Code generation configuration.
    :type config: SchemaCodegenConfig
    :return: Formatted Python module source code.
    :rtype: str
    """
    converter = XsdToPyArrow(
        xml_tag=config.xml_tag,
        max_recursion_depth=config.max_recursion_depth,
    )
    if config.xml_tag is not None:
        return converter.convert_to_python(config.xsd_file, docstring=config.docstring)
    level = config.xml_level if config.xml_level is not None else 1
    return converter.convert_level_to_python(
        config.xsd_file,
        level,
        root_tag=config.root_tag,
        docstring=config.docstring,
    )


UNIREF_SCHEMA_CONFIG: Final[SchemaCodegenConfig] = SchemaCodegenConfig(
    xsd_file=SCHEMAS_DIR / "uniref" / "uniref.xsd",
    output=SCHEMAS_DIR / "uniref" / "schema.py",
    docstring="PyArrow schema and reusable struct components for UniRef (xml_tag='entry').",
    xml_level=1,
)

UNIPROT_SCHEMA_CONFIG: Final[SchemaCodegenConfig] = SchemaCodegenConfig(
    xsd_file=SCHEMAS_DIR / "uniprot" / "uniprot.xsd",
    output=SCHEMAS_DIR / "uniprot" / "schema.py",
    docstring="PyArrow schemas and reusable struct components for UniProt XML.",
    xml_level=1,
)

PUBMED_SCHEMA_CONFIG: Final[SchemaCodegenConfig] = SchemaCodegenConfig(
    xsd_file=SCHEMAS_DIR / "pubmed" / "pubmed.xsd",
    output=SCHEMAS_DIR / "pubmed" / "schema.py",
    docstring="PyArrow schemas and reusable struct components for PubMed XML.",
    xml_level=1,
    root_tag="PubmedArticleSet",
)

CONFIGS: Final[tuple[SchemaCodegenConfig, ...]] = (
    UNIREF_SCHEMA_CONFIG,
    UNIPROT_SCHEMA_CONFIG,
    PUBMED_SCHEMA_CONFIG,
)


def _rel(path: Path, start: Path) -> str:
    """Return a POSIX relative path string from ``start`` to ``path``.

    :param path: Target path.
    :type path: Path
    :param start: Base directory path.
    :type start: Path
    :return: POSIX relative path string.
    :rtype: str
    """
    return Path(os.path.relpath(path, start)).as_posix()


def main(argv: Sequence[str] | None = None) -> int:
    """Write or check configured PyArrow schema modules, or generate one from CLI arguments.

    :param argv: Optional sequence of CLI arguments (defaults to ``sys.argv[1:]``).
    :type argv: Sequence[str] | None
    :return: Process exit code (``0`` on success, ``1`` if ``--check`` detects stale files).
    :rtype: int
    """
    cli_args = list(argv) if argv is not None else sys.argv[1:]
    settings = CliApp.run(GenPyArrowSchemaSettings, cli_args=cli_args)

    if settings.xsd_file is not None and settings.output is not None:
        custom_config = SchemaCodegenConfig(
            xsd_file=settings.xsd_file,
            output=settings.output,
            docstring=f"PyArrow schemas and reusable struct components generated from {settings.xsd_file.name}.",
            xml_level=None if settings.xml_tag else settings.xml_level,
            xml_tag=settings.xml_tag,
            root_tag=settings.root_tag,
            max_recursion_depth=settings.max_recursion_depth,
        )
        text = render_schema_module(custom_config)
        settings.output.parent.mkdir(parents=True, exist_ok=True)
        settings.output.write_text(text, encoding="utf-8")
        return 0

    stale: list[Path] = []
    for config in CONFIGS:
        text = render_schema_module(config)
        current = config.output.read_text(encoding="utf-8") if config.output.exists() else None
        if current == text:
            continue
        if settings.check:
            stale.append(config.output)
        else:
            config.output.write_text(text, encoding="utf-8")
    for path in stale:
        sys.stderr.write(f"stale: {_rel(path, REPO_ROOT)}\n")
    return 1 if stale else 0


__all__ = [
    "CONFIGS",
    "MAX_LINE_LENGTH",
    "PA_SCALAR_EXPR",
    "PUBMED_SCHEMA_CONFIG",
    "REPO_ROOT",
    "SCHEMAS_DIR",
    "TYPE_SUFFIX",
    "UNIPROT_SCHEMA_CONFIG",
    "UNIREF_SCHEMA_CONFIG",
    "ComponentKey",
    "GenPyArrowSchemaSettings",
    "SchemaCodegenConfig",
    "_ComponentCollector",
    "_RenderedFieldSpec",
    "_StructComponentSpec",
    "_decode_metadata",
    "_fmt_str",
    "_format_field_args_single",
    "_format_field_indented",
    "_format_metadata_single",
    "_format_schema_exports",
    "_format_struct_assignment",
    "_format_type_arg_indented",
    "_render_field_call",
    "_render_struct_expr",
    "_render_type_expr",
    "_replace_placeholders",
    "_resolve_field_placeholders",
    "_to_upper_snake",
    "main",
    "render_schema_module",
]


if __name__ == "__main__":
    sys.exit(main())
