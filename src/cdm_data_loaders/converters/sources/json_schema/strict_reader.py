"""Read strict Draft 2020-12 JSON Schema documents with ``$defs`` recursion into the IR."""

import copy
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any, NoReturn

from frozendict import frozendict
from jsonschema.validators import validator_for

from cdm_data_loaders.converters.core.errors import ConversionError, raise_spec_error
from cdm_data_loaders.converters.core.extensions import DEFAULT_EXTENSIONS, ExtensionRegistry
from cdm_data_loaders.converters.core.guards import (
    DEFAULT_MAX_RECURSION_DEPTH,
    require_object_root,
    require_schema_keyword,
)
from cdm_data_loaders.converters.core.inference import unify_scalar_specs
from cdm_data_loaders.converters.core.ir import (
    Field,
    NodeHints,
    NodeType,
    Provenance,
    SchemaDocument,
    TypedNode,
)
from cdm_data_loaders.converters.sources.json_schema.common import extract_constraints, extract_extensions
from cdm_data_loaders.converters.sources.json_schema.strict_combinators import (
    classify_node_kind,
    extract_object_parts_with_refs,
    merge_property_definitions,
)
from cdm_data_loaders.converters.sources.json_schema.strict_refs import (
    SUPPORTED_SCHEMA_DIALECTS,
    StrictBuildContext,
    StrictNodeMeta,
    check_no_forbidden_types_in_recursive_cycle,
    get_inherited_description,
    has_null_possibility,
    is_null_branch,
    resolve_ref,
    validate_node,
)
from cdm_data_loaders.converters.sources.json_schema.strict_scalars import resolve_scalar
from cdm_data_loaders.validation.jsonschema.metaschema import CUSTOM_META_SCHEMA_URI, register_xsv_validator


@dataclass(frozen=True, slots=True)
class StrictJsonSchemaReader:
    """Read Draft 2020-12 JSON Schema documents under the strict tabular conversion specification."""

    extension_registry: ExtensionRegistry = DEFAULT_EXTENSIONS
    error_type: type[ConversionError] = ConversionError
    converter_name: str = "StrictJsonSchemaReader"
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH
    error_by_code: Mapping[str, type[ConversionError]] = field(default_factory=frozendict)

    def _raise_err(self, code: str, msg: str) -> NoReturn:
        """Raise the configured exception subclass for ``code`` or fall back to ``error_type``.

        :param code: Specification error code (``E1``..``E12``).
        :type code: str
        :param msg: Diagnostic error message.
        :type msg: str
        :raises ConversionError: Raised with the resolved error class.
        """
        return raise_spec_error(code, msg, error_type=self.error_type, error_by_code=self.error_by_code)

    def _validate(self, node: object) -> dict[str, Any]:
        """Validate ``node`` using the reader's extension registry and error catalog.

        :param node: Raw JSON Schema node.
        :type node: object
        :return: Validated schema dictionary.
        :rtype: dict[str, Any]
        """
        return validate_node(
            node,
            extension_registry=self.extension_registry,
            error_type=self.error_type,
            error_by_code=self.error_by_code,
        )

    def _classify(self, node: object, defs: Mapping[str, Any]) -> str:
        """Classify ``node`` into its structural shape using the reader's error catalog.

        :param node: Raw JSON Schema node.
        :type node: object
        :param defs: Root ``$defs`` mapping.
        :type defs: Mapping[str, Any]
        :return: Structural shape string.
        :rtype: str
        """
        return classify_node_kind(
            node,
            defs,
            extension_registry=self.extension_registry,
            error_type=self.error_type,
            error_by_code=self.error_by_code,
        )

    def read(self, source: Mapping[str, Any]) -> SchemaDocument:
        """Read and lower a Draft 2020-12 JSON Schema under the strict specification.

        :param source: Input JSON Schema mapping.
        :type source: Mapping[str, Any]
        :return: Lowered ``SchemaDocument``.
        :rtype: SchemaDocument
        """
        if not isinstance(source, Mapping):
            msg = f"Missing or unsupported $schema dialect: {None!r}; expected JSON Schema Draft 2020-12"
            self._raise_err("E1", msg)
        schema = dict(source)
        require_schema_keyword(
            schema,
            self.error_by_code.get("E1", self.error_type),
            converter_name=self.converter_name,
        )
        if schema.get("$schema") not in SUPPORTED_SCHEMA_DIALECTS:
            msg = (
                f"Missing or unsupported $schema dialect: {schema.get('$schema')!r}; expected JSON Schema Draft 2020-12"
            )
            self._raise_err("E1", msg)
        if schema.get("$schema") == CUSTOM_META_SCHEMA_URI:
            register_xsv_validator()
        require_object_root(
            schema,
            self.error_by_code.get("E2", self.error_type),
            target="a struct schema",
        )

        defs = schema.get("$defs", {})
        if not isinstance(defs, Mapping):
            msg = "$defs must be a mapping of definition names to schema objects"
            raise self.error_type(msg)

        root_dict = self._validate(schema)
        ref_counts: dict[str, int] = {}
        if "$ref" in root_dict and "properties" not in root_dict:
            ref_name, target = resolve_ref(
                root_dict["$ref"], defs, error_type=self.error_type, error_by_code=self.error_by_code
            )
            ref_counts[ref_name] = 1
            root_dict = self._validate(target)

        root_kind = self._classify(root_dict, defs)
        if root_kind == "map":
            msg = "Top-level root schema cannot be a dynamic map without fixed properties; root JSON Schema did not resolve to a struct"
            self._raise_err("E2", msg)
        if root_kind != "object":
            msg = f"Top-level root schema must resolve to a fixed-property object, got {root_kind!r}"
            self._raise_err("E2", msg)

        ctx = StrictBuildContext(
            defs=defs,
            ref_counts=ref_counts,
            max_recursion_depth=self.max_recursion_depth,
            extension_registry=self.extension_registry,
            error_type=self.error_type,
            error_by_code=self.error_by_code,
        )
        root_meta = self._build_node_meta(root_dict, defs, (), nullable=False, facet_source=schema)
        node = self._build_merged_object(root_dict, ctx, root_meta)
        return SchemaDocument(
            root=node,
            dialect=schema["$schema"],
            identifier=schema.get("$id"),
            annotations=node.annotations,
            extensions=node.extensions,
            provenance=Provenance("json-schema"),
            jsonschema_validator_cls=validator_for(schema),
        )

    def read_node(self, source: object) -> TypedNode:
        """Read a standalone schema fragment under strict rules without root-object guards.

        :param source: JSON Schema fragment mapping or boolean schema.
        :type source: object
        :return: Lowered ``TypedNode``.
        :rtype: TypedNode
        """
        ctx = StrictBuildContext(
            defs={},
            ref_counts={},
            max_recursion_depth=self.max_recursion_depth,
            extension_registry=self.extension_registry,
            error_type=self.error_type,
            error_by_code=self.error_by_code,
        )
        return self._build_field_node(source, ctx, (), required_in_parent=True)

    def _build_node_meta(
        self,
        node: Mapping[str, Any],
        defs: Mapping[str, Any],
        path: tuple[str | int, ...],
        *,
        nullable: bool,
        facet_source: Mapping[str, Any] | None = None,
    ) -> StrictNodeMeta:
        """Build a ``StrictNodeMeta`` with inherited description, extensions, and constraints.

        :param node: Validated JSON Schema node used for description inheritance.
        :type node: Mapping[str, Any]
        :param defs: Root ``$defs`` mapping.
        :type defs: Mapping[str, Any]
        :param path: JSON pointer path tuple.
        :type path: tuple[str | int, ...]
        :param nullable: Whether the node is nullable.
        :type nullable: bool
        :param facet_source: Optional mapping from which to extract extensions and constraints.
        :type facet_source: Mapping[str, Any] | None
        :return: Populated ``StrictNodeMeta``.
        :rtype: StrictNodeMeta
        """
        source = facet_source if facet_source is not None else node
        desc = get_inherited_description(node, defs, error_type=self.error_type, error_by_code=self.error_by_code)
        return StrictNodeMeta(
            path=path,
            nullable=nullable,
            annotations={"description": desc} if desc else {},
            extensions=extract_extensions(source, self.extension_registry),
            constraints=extract_constraints(source),
        )

    def _resolve_and_unify_scalar_branches(
        self,
        scalar_branches: list[dict[str, Any]],
        ctx: StrictBuildContext,
        path: tuple[str | int, ...],
    ) -> tuple[NodeType, NodeHints, str]:
        """Build each scalar branch node and unify their scalar types and labels.

        :param scalar_branches: List of scalar branch schema dictionaries.
        :type scalar_branches: list[dict[str, Any]]
        :param ctx: Active strict build context.
        :type ctx: StrictBuildContext
        :param path: Provenance path for scalar branch evaluation.
        :type path: tuple[str | int, ...]
        :return: Unified ``(NodeType, NodeHints, jsonschema_type)`` tuple.
        :rtype: tuple[NodeType, NodeHints, str]
        """
        resolved_scalars: list[tuple[NodeType, NodeHints, str]] = []
        branch_meta = StrictNodeMeta(path=path)
        for scalar_branch in scalar_branches:
            s_node = self._build_type(scalar_branch, ctx, branch_meta)
            s_label = s_node.provenance.metadata.get("jsonschema_type") if s_node.provenance else None
            if not isinstance(s_label, str):
                msg = f"Expected scalar branch with jsonschema_type label, got {scalar_branch!r}"
                raise self.error_type(msg)
            resolved_scalars.append((s_node.type, s_node.hints, s_label))
        return unify_scalar_specs(resolved_scalars)

    def _build_merged_object(
        self,
        node: dict[str, Any],
        ctx: StrictBuildContext,
        meta: StrictNodeMeta,
        *,
        force_all_nullable: bool = False,
        scalar_branches: list[dict[str, Any]] | None = None,
    ) -> TypedNode:
        """Build an object ``TypedNode`` from an object schema and its combinator branches.

        :param node: Object JSON Schema node dictionary.
        :type node: dict[str, Any]
        :param ctx: Active strict build context.
        :type ctx: StrictBuildContext
        :param meta: Node metadata payload.
        :type meta: StrictNodeMeta
        :param force_all_nullable: Whether to force all properties to be nullable.
        :type force_all_nullable: bool
        :param scalar_branches: Optional scalar union branches to synthesize into ``_value``.
        :type scalar_branches: list[dict[str, Any]] | None
        :return: Object ``TypedNode``.
        :rtype: TypedNode
        """
        props_by_name, required, extracted_scalars = extract_object_parts_with_refs(node, ctx)
        all_scalars = list(scalar_branches or [])
        for scalar_node in extracted_scalars:
            if scalar_node not in all_scalars:
                all_scalars.append(scalar_node)

        if all_scalars:
            force_all_nullable = True
            if "_value" in props_by_name:
                msg = "Property name '_value' in object branch collides with synthesized '_value' field in scalar+object union"
                self._raise_err("E12", msg)

        fields: list[Field] = []
        for prop_name, (merged_prop, prop_ctx) in props_by_name.items():
            is_req = (prop_name in required) and not force_all_nullable
            prop_node = self._build_field_node(
                merged_prop, prop_ctx, (*meta.path, "properties", prop_name), required_in_parent=is_req
            )
            fields.append(
                Field(prop_name, prop_node, required=not bool(prop_node.nullable), provenance=prop_node.provenance)
            )

        if all_scalars:
            val_path = (*meta.path, "_value")
            val_type, val_hints, val_label = self._resolve_and_unify_scalar_branches(all_scalars, ctx, val_path)
            val_prov = Provenance("json-schema", val_path, {"jsonschema_type": val_label})
            val_node = TypedNode(type=val_type, nullable=True, hints=val_hints, provenance=val_prov)
            fields.append(Field("_value", val_node, required=False, provenance=val_prov))

        if not fields:
            msg = "Empty object schema with no properties cannot be converted to a PyArrow struct"
            self._raise_err("E3", msg)
        return TypedNode(
            type="object",
            nullable=meta.nullable,
            hints=NodeHints(logical_type="struct"),
            properties=tuple(fields),
            required_names=tuple(f.name for f in fields if f.required),
            additional_properties=TypedNode(type="never", nullable=False),
            annotations=meta.annotations,
            extensions=meta.extensions,
            constraints=meta.constraints,
            provenance=Provenance("json-schema", meta.path),
        )

    def _build_map(
        self,
        node: dict[str, Any],
        ctx: StrictBuildContext,
        meta: StrictNodeMeta,
    ) -> TypedNode:
        """Build a map ``TypedNode`` from a dynamic map object schema.

        :param node: Map JSON Schema node dictionary.
        :type node: dict[str, Any]
        :param ctx: Active strict build context.
        :type ctx: StrictBuildContext
        :param meta: Node metadata payload.
        :type meta: StrictNodeMeta
        :return: Map ``TypedNode``.
        :rtype: TypedNode
        """
        val_schemas: list[dict[str, Any]] = []
        if isinstance(node.get("additionalProperties"), dict):
            val_schemas.append(self._validate(node["additionalProperties"]))
        val_schemas.extend(self._validate(pat_schema) for pat_schema in node.get("patternProperties", {}).values())
        val_schema = merge_property_definitions(val_schemas)
        val_node = self._build_field_node(
            val_schema, ctx, (*meta.path, "additionalProperties"), required_in_parent=True
        )
        key_node = TypedNode(
            type="string",
            nullable=False,
            hints=NodeHints(logical_type="string"),
            provenance=Provenance("json-schema", meta.path, {"jsonschema_type": "string"}),
        )
        return TypedNode(
            type="map",
            nullable=meta.nullable,
            hints=NodeHints(logical_type="map"),
            key_type=key_node,
            value_type=val_node,
            annotations=meta.annotations,
            extensions=meta.extensions,
            constraints=meta.constraints,
            provenance=Provenance("json-schema", meta.path),
        )

    def _build_array(
        self,
        node: dict[str, Any],
        ctx: StrictBuildContext,
        meta: StrictNodeMeta,
    ) -> TypedNode:
        """Build a list ``TypedNode`` or closed tuple-struct ``TypedNode`` (``prefixItems``).

        :param node: Array JSON Schema node dictionary.
        :type node: dict[str, Any]
        :param ctx: Active strict build context.
        :type ctx: StrictBuildContext
        :param meta: Node metadata payload.
        :type meta: StrictNodeMeta
        :return: Array or tuple-struct ``TypedNode``.
        :rtype: TypedNode
        """
        if "prefixItems" in node:
            if node.get("items") not in (False, None):
                msg = "Open tuple combining prefixItems with schema-valued items is not supported"
                self._raise_err("E5", msg)
            min_items = node.get("minItems", 0)
            fields: list[Field] = []
            for idx, item_schema in enumerate(node["prefixItems"]):
                item_node = self._build_field_node(
                    item_schema, ctx, (*meta.path, "prefixItems", idx), required_in_parent=idx < min_items
                )
                fields.append(
                    Field(
                        f"item_{idx}",
                        item_node,
                        required=not bool(item_node.nullable),
                        provenance=item_node.provenance,
                    )
                )
            if not fields:
                msg = "Empty prefixItems tuple cannot be converted to a PyArrow struct"
                self._raise_err("E3", msg)
            return TypedNode(
                type="object",
                nullable=meta.nullable,
                hints=NodeHints(logical_type="struct"),
                properties=tuple(fields),
                required_names=tuple(f.name for f in fields if f.required),
                additional_properties=TypedNode(type="never", nullable=False),
                annotations=meta.annotations,
                extensions=meta.extensions,
                constraints=meta.constraints,
                provenance=Provenance("json-schema", meta.path),
            )

        validated_items = self._validate(node.get("items"))
        item_node = self._build_field_node(validated_items, ctx, (*meta.path, "items"), required_in_parent=True)
        return TypedNode(
            type="array",
            nullable=meta.nullable,
            hints=NodeHints(logical_type="list"),
            items=item_node,
            annotations=meta.annotations,
            extensions=meta.extensions,
            constraints=meta.constraints,
            provenance=Provenance("json-schema", meta.path),
        )

    def _build_union_type(
        self,
        node: dict[str, Any],
        ctx: StrictBuildContext,
        meta: StrictNodeMeta,
    ) -> TypedNode:
        """Resolve an ``anyOf`` or ``oneOf`` combinator node into a ``TypedNode``.

        :param node: Union JSON Schema node dictionary.
        :type node: dict[str, Any]
        :param ctx: Active strict build context.
        :type ctx: StrictBuildContext
        :param meta: Node metadata payload.
        :type meta: StrictNodeMeta
        :return: Unified ``TypedNode``.
        :rtype: TypedNode
        """
        kw = "anyOf" if "anyOf" in node else "oneOf"
        non_null = [self._validate(b) for b in node[kw] if not (isinstance(b, Mapping) and is_null_branch(b))]
        if not non_null:
            msg = f"Standalone null-only {kw} combinator cannot be converted to a PyArrow field"
            self._raise_err("E11", msg)
        if len(non_null) == 1:
            return self._build_type(non_null[0], ctx, meta)

        obj_branches: list[dict[str, Any]] = []
        scalar_branches: list[dict[str, Any]] = []
        for branch in non_null:
            kind = self._classify(branch, ctx.defs)
            if kind == "object":
                obj_branches.append(branch)
            elif kind == "scalar":
                scalar_branches.append(branch)
            else:
                msg = f"Incompatible branch shape {kind!r} in {kw} combinator"
                self._raise_err("E8", msg)

        if scalar_branches and not obj_branches:
            u_type, u_hints, u_label = self._resolve_and_unify_scalar_branches(scalar_branches, ctx, meta.path)
            return TypedNode(
                type=u_type,
                nullable=meta.nullable,
                hints=u_hints,
                annotations=meta.annotations,
                extensions=meta.extensions,
                constraints=meta.constraints,
                provenance=Provenance("json-schema", meta.path, {"jsonschema_type": u_label}),
            )

        if obj_branches and not scalar_branches:
            return self._build_merged_object(node, ctx, meta)

        return self._build_merged_object(
            node,
            ctx,
            meta,
            force_all_nullable=True,
            scalar_branches=scalar_branches,
        )

    def _build_ref_type(
        self,
        validated: dict[str, Any],
        ctx: StrictBuildContext,
        meta: StrictNodeMeta,
    ) -> TypedNode:
        """Resolve and build a ``$ref`` schema node with recursion depth tracking.

        :param validated: Validated schema dictionary containing ``$ref``.
        :type validated: dict[str, Any]
        :param ctx: Active strict build context.
        :type ctx: StrictBuildContext
        :param meta: Node metadata payload.
        :type meta: StrictNodeMeta
        :return: Resolved ``TypedNode``.
        :rtype: TypedNode
        """
        ref_name, target = resolve_ref(
            validated["$ref"], ctx.defs, error_type=self.error_type, error_by_code=self.error_by_code
        )
        count = ctx.ref_counts.get(ref_name, 0)
        if count >= 1:
            check_no_forbidden_types_in_recursive_cycle(
                ref_name, target, ctx.defs, error_type=self.error_type, error_by_code=self.error_by_code
            )
        if count >= ctx.max_recursion_depth:
            return TypedNode(
                type="string",
                nullable=meta.nullable,
                hints=NodeHints(logical_type="string"),
                annotations=meta.annotations,
                extensions=meta.extensions,
                constraints=meta.constraints,
                provenance=Provenance("json-schema", meta.path),
            )
        next_ctx = ctx.with_ref(ref_name)
        if len(validated) == 1:
            result = self._build_type(target, next_ctx, meta)
            if result.type == "object":
                prov_meta = dict(result.provenance.metadata) if result.provenance is not None else {}
                prov_meta.setdefault("component_id", hash(("jsonschema_ref", ref_name)))
                prov_meta.setdefault("type_name", ref_name)
                if count >= 1:
                    prov_meta["inline_struct"] = True
                return replace(
                    result,
                    provenance=Provenance(
                        result.provenance.source if result.provenance is not None else "json-schema",
                        result.provenance.path if result.provenance is not None else meta.path,
                        prov_meta,
                    ),
                )
            return result
        merged = {**copy.deepcopy(target), **{k: v for k, v in validated.items() if k not in {"$ref", "description"}}}
        return self._build_type(merged, next_ctx, meta)

    def _build_type(
        self,
        node: object,
        ctx: StrictBuildContext,
        meta: StrictNodeMeta,
    ) -> TypedNode:
        """Convert any JSON Schema node into a strict ``TypedNode``.

        :param node: Raw JSON Schema node.
        :type node: object
        :param ctx: Active strict build context.
        :type ctx: StrictBuildContext
        :param meta: Node metadata payload.
        :type meta: StrictNodeMeta
        :return: Lowered ``TypedNode``.
        :rtype: TypedNode
        """
        validated = self._validate(node)
        if is_null_branch(validated):
            msg = "Standalone null-only schema cannot be converted to a PyArrow field"
            self._raise_err("E11", msg)

        if "$ref" in validated:
            return self._build_ref_type(validated, ctx, meta)

        if (
            ("anyOf" in validated or "oneOf" in validated)
            and "properties" not in validated
            and "allOf" not in validated
        ):
            return self._build_union_type(validated, ctx, meta)

        kind = self._classify(validated, ctx.defs)
        if kind == "object":
            return self._build_merged_object(validated, ctx, meta)
        if kind == "map":
            return self._build_map(validated, ctx, meta)
        if kind == "array":
            return self._build_array(validated, ctx, meta)
        s_type, s_hints, s_label = resolve_scalar(
            validated, error_type=self.error_type, error_by_code=self.error_by_code
        )
        raw_decl = validated.get("type")
        declared_type: str | tuple[str, ...] | None = (
            tuple(str(t) for t in raw_decl)
            if isinstance(raw_decl, list)
            else (raw_decl if isinstance(raw_decl, str) else None)
        )
        return TypedNode(
            type=s_type,
            declared_type=declared_type,
            nullable=meta.nullable,
            hints=s_hints,
            annotations=meta.annotations,
            extensions=meta.extensions,
            constraints=meta.constraints,
            provenance=Provenance("json-schema", meta.path, {"jsonschema_type": s_label}),
        )

    def _build_field_node(
        self,
        node: object,
        ctx: StrictBuildContext,
        path: tuple[str | int, ...],
        *,
        required_in_parent: bool,
    ) -> TypedNode:
        """Build a ``TypedNode`` with nullability, description, extensions, and constraints for a field or item.

        :param node: Raw field or item JSON Schema node.
        :type node: object
        :param ctx: Active strict build context.
        :type ctx: StrictBuildContext
        :param path: JSON pointer path tuple.
        :type path: tuple[str | int, ...]
        :param required_in_parent: Whether the field is required in its parent object.
        :type required_in_parent: bool
        :return: Lowered ``TypedNode``.
        :rtype: TypedNode
        """
        validated = self._validate(node)
        nullable = (not required_in_parent) or has_null_possibility(
            validated, ctx.defs, error_type=self.error_type, error_by_code=self.error_by_code
        )
        meta = self._build_node_meta(validated, ctx.defs, path, nullable=nullable)
        return self._build_type(validated, ctx, meta)
