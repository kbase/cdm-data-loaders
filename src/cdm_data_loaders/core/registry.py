"""Validated registries and module loaders for entity models, JSON Schemas, PyArrow schemas, transforms, and pre-processors."""

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from importlib import import_module
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import MappingProxyType, ModuleType
from typing import Annotated, Any, Final, Literal, Self, overload

import pyarrow as pa
from dlt.common.storages.fsspec_filesystem import FileItemDict
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, InstanceOf, StringConstraints, model_validator

from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME
from cdm_data_loaders.validation.jsonschema.models import ValidatedSchema

DEFAULT_PREPROCESSORS_ATTR: Final[str] = "FILE_PREPROCESSORS"


@dataclass(frozen=True, slots=True)
class TransformContext:
    """Metadata describing the input table and source file for a table transform invocation.

    :param table_name: configured input table name
    :type table_name: str
    :param source_file: relative path or identifier of the file being parsed, if any
    :type source_file: str | None
    :param file_name: basename of ``source_file``, if any
    :type file_name: str | None
    """

    table_name: str
    source_file: str | None = None
    file_name: str | None = None

    @classmethod
    def for_source(cls, table_name: str, source_file: str | None = None) -> Self:
        """Create a transform context with ``file_name`` derived from ``source_file``.

        :param table_name: configured input table name
        :type table_name: str
        :param source_file: relative path or identifier of the file being parsed, if any
        :type source_file: str | None
        :return: populated transform context
        :rtype: Self
        """
        file_name = Path(source_file).name if source_file else None
        return cls(table_name=table_name, source_file=source_file, file_name=file_name)


RegistryKey = Annotated[str, StringConstraints(min_length=1, strip_whitespace=True, strict=True)]
type RoutedTransformOutput = Mapping[str, Sequence[dict[str, Any]] | pa.Table]
type RecordTransformFn = (
    Callable[[dict[str, Any]], RoutedTransformOutput]
    | Callable[[dict[str, Any], TransformContext], RoutedTransformOutput]
)
type ArrowTableTransformFn = (
    Callable[[pa.Table], RoutedTransformOutput] | Callable[[pa.Table, TransformContext], RoutedTransformOutput]
)
type TableTransformFn = RecordTransformFn | ArrowTableTransformFn
type FilePreprocessFn = Callable[[FileItemDict], FileItemDict | None]


def _coerce_validated_schema(v: object) -> object:
    """Wrap raw schema inputs so ``ValidatedSchema`` validates them.

    :param v: raw schema input or ``ValidatedSchema`` instance
    :type v: object
    :return: ``v`` unchanged if ``ValidatedSchema``, else ``{"jsonschema": v}``
    :rtype: object
    """
    if isinstance(v, ValidatedSchema):
        return v
    return {"jsonschema": v}


def _coerce_preprocessor_seq(v: object) -> object:
    """Coerce a single callable into a one-element tuple, or convert sequences to a tuple.

    :param v: callable or sequence of callables
    :type v: object
    :return: normalized tuple of callables or ``v`` unchanged
    :rtype: object
    """
    if callable(v):
        return (v,)
    if isinstance(v, Sequence) and not isinstance(v, (str, bytes)):
        return tuple(v)
    return v


def _coerce_transform_entry(v: object) -> object:
    """Convert non-string sequences of transform callables into a tuple while preserving single values.

    :param v: raw transform registry value
    :type v: object
    :return: tuple when ``v`` is a non-bytes/string sequence, else ``v`` unchanged
    :rtype: object
    """
    if isinstance(v, Sequence) and not isinstance(v, (str, bytes, bytearray)):
        return tuple(v)
    return v


type PreprocessorTuple = Annotated[
    tuple[FilePreprocessFn, ...],
    BeforeValidator(_coerce_preprocessor_seq),
    Field(min_length=1),
]
type TransformTuple = Annotated[
    tuple[TableTransformFn, ...],
    Field(min_length=1),
]
type TransformEntry = Annotated[
    TableTransformFn | TransformTuple,
    BeforeValidator(_coerce_transform_entry),
]


class RejectedFreezerMixin(BaseModel):
    """Mixin to forbid the use of the reserved ``_rejected`` table name in registries."""

    @model_validator(mode="after")
    def _validate_and_freeze(self) -> Self:
        """Forbid ``_rejected`` in registry keys and freeze the registry mapping.

        :return: validated registry instance
        :rtype: Self
        :raises ValueError: if ``_rejected`` is present in ``self.registry``
        """
        if REJECTED_TABLE_NAME in self.registry:  # pyright: ignore[reportAttributeAccessIssue]
            err_msg = (
                f"{self.__class__.__name__} cannot use reserved table name {REJECTED_TABLE_NAME!r}; "
                f"{REJECTED_TABLE_NAME!r} is reserved for pipeline error records."
            )
            raise ValueError(err_msg)
        object.__setattr__(self, "registry", MappingProxyType(dict(self.registry)))  # pyright: ignore[reportAttributeAccessIssue]
        return self


class ModelRegistry(RejectedFreezerMixin, BaseModel):
    """A validated registry of entity models."""

    model_config = ConfigDict(str_strip_whitespace=True)

    registry: Annotated[dict[RegistryKey, type[BaseModel]], Field(min_length=1)]


class SchemaRegistry(RejectedFreezerMixin, BaseModel):
    """A validated registry of validated entity schemas."""

    model_config = ConfigDict(str_strip_whitespace=True)

    registry: Annotated[
        dict[RegistryKey, Annotated[ValidatedSchema, BeforeValidator(_coerce_validated_schema)]],
        Field(min_length=1),
    ]


class ArrowSchemaRegistry(RejectedFreezerMixin, BaseModel):
    """A validated registry of PyArrow schemas."""

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    registry: Annotated[
        dict[RegistryKey, InstanceOf[pa.Schema]],
        Field(min_length=1),
    ]


class TransformRegistry(RejectedFreezerMixin, BaseModel):
    """A validated registry of per-table record or PyArrow table transform functions."""

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    registry: Annotated[dict[RegistryKey, TransformEntry], Field(min_length=1)]

    def get_tuple(self, table_name: str) -> tuple[TableTransformFn, ...]:
        """Return the configured transform functions for ``table_name`` as an ordered tuple.

        :param table_name: input table name
        :type table_name: str
        :return: tuple of configured transform callables (empty if unconfigured)
        :rtype: tuple[TableTransformFn, ...]
        """
        entry = self.registry.get(table_name)
        if entry is None:
            return ()
        if isinstance(entry, tuple):
            return entry
        return (entry,)


class PreprocessorRegistry(RejectedFreezerMixin, BaseModel):
    """A validated registry of pipeline-wide or per-table file pre-processor functions."""

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    default: tuple[FilePreprocessFn, ...] = ()
    registry: dict[RegistryKey, PreprocessorTuple] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_mode(self) -> Self:
        """Require either ``default`` or ``registry`` to be populated, not both.

        :return: validated registry instance
        :rtype: Self
        :raises ValueError: if neither or both ``default`` and ``registry`` are populated
        """
        if not self.default and not self.registry:
            err_msg = "PreprocessorRegistry must define at least one preprocessor in 'default' or 'registry'."
            raise ValueError(err_msg)
        if self.default and self.registry:
            err_msg = "PreprocessorRegistry cannot define both 'default' and 'registry' preprocessors."
            raise ValueError(err_msg)
        return self

    def get(self, table_name: str | None = None) -> tuple[FilePreprocessFn, ...]:
        """Return the file pre-processors configured for ``table_name`` or pipeline-wide default.

        :param table_name: target table name, or ``None`` for multi-table streams
        :type table_name: str | None
        :return: tuple of file pre-processor callables to execute in order
        :rtype: tuple[FilePreprocessFn, ...]
        """
        if self.default:
            return self.default
        if table_name is not None:
            return self.registry.get(table_name, ())
        seen: list[FilePreprocessFn] = []
        for funcs in self.registry.values():
            for fn in funcs:
                if fn not in seen:
                    seen.append(fn)
        return tuple(seen)


def split_module_attr_spec(spec: str) -> tuple[str, str | None]:
    r"""Split ``spec`` into ``(module_or_path, optional_attribute_name)``.

    Preserves Windows drive-letter prefixes such as ``C:\path\to\module.py``.

    :param spec: dotted module path or Python file path with optional ``:ATTR`` suffix
    :type spec: str
    :return: tuple of ``(target_spec, attr_name_or_none)``
    :rtype: tuple[str, str | None]
    """
    head, sep, tail = spec.rpartition(":")
    if not sep or not head or not tail:
        return spec, None
    if (len(head) == 1 and head.isalpha()) or "/" in tail or "\\" in tail:
        return spec, None
    return head, tail


def load_python_module(target: str) -> ModuleType:
    """Import ``target`` from a ``.py`` file path or as a dotted Python module path.

    :param target: Python file path or dotted module import path
    :type target: str
    :return: loaded Python module object
    :rtype: ModuleType
    :raises ValueError: if a file path cannot be loaded as a Python module
    """
    candidate_path = Path(target)
    if candidate_path.suffix == ".py" or candidate_path.is_file():
        resolved = candidate_path.resolve()
        spec = spec_from_file_location(f"_dynamic_module_{resolved.stem}", resolved)
        if spec is None or spec.loader is None:
            msg = f"Cannot load Python module from path: {resolved}"
            raise ValueError(msg)
        module = module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    return import_module(target)


@overload
def load_registry_dict(module_path: str, attribute_name: Literal["ENTITY_MODELS"]) -> ModelRegistry: ...


@overload
def load_registry_dict(module_path: str, attribute_name: Literal["ENTITY_SCHEMAS"]) -> SchemaRegistry: ...


@overload
def load_registry_dict(module_path: str, attribute_name: Literal["TABLE_TRANSFORMS"]) -> TransformRegistry: ...


@overload
def load_registry_dict(module_path: str, attribute_name: Literal["OUTPUT_SCHEMAS"]) -> ArrowSchemaRegistry: ...


def load_registry_dict(
    module_path: str,
    attribute_name: str,
) -> ModelRegistry | SchemaRegistry | TransformRegistry | ArrowSchemaRegistry:
    """Import ``module_path`` and return its validated registry for ``attribute_name``.

    :param module_path: dotted import path or ``.py`` file path of the registry module (optionally ``:ATTR``)
    :type module_path: str
    :param attribute_name: default dictionary attribute name on the module
        (``ENTITY_MODELS``, ``ENTITY_SCHEMAS``, ``TABLE_TRANSFORMS``, or ``OUTPUT_SCHEMAS``)
    :type attribute_name: str
    :return: validated registry instance
    :rtype: ModelRegistry | SchemaRegistry | TransformRegistry | ArrowSchemaRegistry
    :raises ValueError: if ``attribute_name`` is unsupported or uses the reserved ``_rejected`` table name
    :raises TypeError: if the resolved attribute is not a dictionary
    """
    cls_map = {
        "ENTITY_MODELS": ModelRegistry,
        "ENTITY_SCHEMAS": SchemaRegistry,
        "TABLE_TRANSFORMS": TransformRegistry,
        "OUTPUT_SCHEMAS": ArrowSchemaRegistry,
    }
    if attribute_name not in cls_map:
        err_msg = f"Unsupported registry attribute: {attribute_name}"
        raise ValueError(err_msg)

    target, explicit_attr = split_module_attr_spec(module_path)
    resolved_attr = explicit_attr or attribute_name
    module = load_python_module(target)
    raw_registry = getattr(module, resolved_attr, None)
    if not isinstance(raw_registry, dict):
        err_msg = f"{target}.{resolved_attr} must be a dictionary, not {type(raw_registry).__name__}"
        raise TypeError(err_msg)

    return cls_map[attribute_name](registry=raw_registry)  # pyright: ignore[reportArgumentType]


def load_file_preprocessors(
    module_path: str,
    attribute_name: str = DEFAULT_PREPROCESSORS_ATTR,
) -> PreprocessorRegistry:
    """Import ``module_path`` and return its validated :class:`PreprocessorRegistry`.

    :param module_path: dotted import path or ``.py`` file path of the pre-processors module
    :type module_path: str
    :param attribute_name: name of the attribute on the module (defaults to ``FILE_PREPROCESSORS``)
    :type attribute_name: str
    :return: validated file pre-processor registry
    :rtype: PreprocessorRegistry
    :raises TypeError: if the attribute is neither a dictionary nor a sequence of callables
    :raises ValueError: if the registry is empty or uses the reserved ``_rejected`` table name
    """
    target, explicit_attr = split_module_attr_spec(module_path)
    resolved_attr = explicit_attr or attribute_name
    module = load_python_module(target)
    raw_preprocessors = getattr(module, resolved_attr, None)
    if isinstance(raw_preprocessors, dict):
        return PreprocessorRegistry(registry=raw_preprocessors)
    if isinstance(raw_preprocessors, Sequence) and not isinstance(raw_preprocessors, (str, bytes)):
        return PreprocessorRegistry(default=tuple(raw_preprocessors))
    err_msg = (
        f"{target}.{resolved_attr} must be a dictionary or a sequence of callables, "
        f"not {type(raw_preprocessors).__name__}"
    )
    raise TypeError(err_msg)


def validate_registry_table_names(
    registry_keys: Iterable[str],
    valid_tables: Iterable[str],
    module_label: str,
) -> None:
    """Raise ``ValueError`` if ``registry_keys`` contains table names not in ``valid_tables``.

    :param registry_keys: table names configured in a registry
    :type registry_keys: Iterable[str]
    :param valid_tables: valid destination/input table names for the pipeline
    :type valid_tables: Iterable[str]
    :param module_label: setting attribute label reported in the error message
    :type module_label: str
    :return: ``None``
    :rtype: None
    :raises ValueError: if ``registry_keys`` contains unknown table names
    """
    unknown_tables = sorted(set(registry_keys) - set(valid_tables))
    if unknown_tables:
        err_msg = f"Unknown table names in {module_label}: {', '.join(unknown_tables)}"
        raise ValueError(err_msg)
