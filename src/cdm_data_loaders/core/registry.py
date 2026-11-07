"""Validated registries and module loaders for entity models, JSON Schemas, PyArrow schemas, transforms, and pre-processors."""

from collections.abc import Callable, Mapping, Sequence
from importlib import import_module
from types import MappingProxyType
from typing import Annotated, Any, Final, Literal, Self, overload

import pyarrow as pa
from dlt.common.storages.fsspec_filesystem import FileItemDict
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, InstanceOf, StringConstraints, model_validator

from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME
from cdm_data_loaders.schemas.jsonschema import ValidatedSchema

DEFAULT_TRANSFORMS_ATTR: Final[str] = "TABLE_TRANSFORMS"
DEFAULT_OUTPUT_SCHEMAS_ATTR: Final[str] = "OUTPUT_SCHEMAS"
DEFAULT_PREPROCESSORS_ATTR: Final[str] = "FILE_PREPROCESSORS"

RegistryKey = Annotated[str, StringConstraints(min_length=1, strip_whitespace=True, strict=True)]
type RecordTransformFn = Callable[[dict[str, Any]], Mapping[str, Sequence[dict[str, Any]]]]
type FilePreprocessFn = Callable[[FileItemDict], FileItemDict | None]


def _coerce_validated_schema(v: Any) -> Any:  # noqa: ANN401
    """Wrap raw schema inputs so ValidatedSchema validates them."""
    if isinstance(v, ValidatedSchema):
        return v
    return {"jsonschema": v}


def _coerce_preprocessor_seq(v: Any) -> Any:  # noqa: ANN401
    """Coerce a single callable into a one-element tuple, or convert sequences to a tuple."""
    if callable(v):
        return (v,)
    if isinstance(v, Sequence) and not isinstance(v, (str, bytes)):
        return tuple(v)
    return v


type PreprocessorTuple = Annotated[
    tuple[FilePreprocessFn, ...],
    BeforeValidator(_coerce_preprocessor_seq),
    Field(min_length=1),
]


class RejectedFreezerMixin(BaseModel):
    """Mixin to forbid the use of the reserved ``_rejected`` table name in registries."""

    @model_validator(mode="after")
    def _validate_and_freeze(self) -> Self:
        """Forbid ``_rejected`` in registry keys."""
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
    """A validated registry of per-table record transform functions."""

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    registry: Annotated[dict[RegistryKey, RecordTransformFn], Field(min_length=1)]


class PreprocessorRegistry(RejectedFreezerMixin, BaseModel):
    """A validated registry of pipeline-wide or per-table file pre-processor functions."""

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    default: tuple[FilePreprocessFn, ...] = ()
    registry: dict[RegistryKey, PreprocessorTuple] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_mode(self) -> Self:
        """Require either ``default`` or ``registry`` to be populated, not both."""
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

    :param module_path: dotted import path of the registry module
    :type module_path: str
    :param attribute_name: name of the dictionary attribute on the module
        (``ENTITY_MODELS``, ``ENTITY_SCHEMAS``, ``TABLE_TRANSFORMS``, or ``OUTPUT_SCHEMAS``)
    :type attribute_name: str
    :return: the validated registry instance
    :rtype: ModelRegistry | SchemaRegistry | TransformRegistry | ArrowSchemaRegistry
    :raises ValueError: if the attribute name is unsupported or uses the reserved ``_rejected`` table name
    """
    module = import_module(module_path)
    raw_registry = getattr(module, attribute_name, None)
    if not isinstance(raw_registry, dict):
        err_msg = f"{module_path}.{attribute_name} must be a dictionary, not {type(raw_registry).__name__}"
        raise TypeError(err_msg)

    cls_map = {
        "ENTITY_MODELS": ModelRegistry,
        "ENTITY_SCHEMAS": SchemaRegistry,
        "TABLE_TRANSFORMS": TransformRegistry,
        "OUTPUT_SCHEMAS": ArrowSchemaRegistry,
    }
    if attribute_name not in cls_map:
        err_msg = f"Unsupported registry attribute: {attribute_name}"
        raise ValueError(err_msg)

    return cls_map[attribute_name](registry=raw_registry)  # pyright: ignore[reportArgumentType]


def load_file_preprocessors(
    module_path: str,
    attribute_name: str = DEFAULT_PREPROCESSORS_ATTR,
) -> PreprocessorRegistry:
    """Import ``module_path`` and return its validated :class:`PreprocessorRegistry`.

    :param module_path: dotted import path of the pre-processors module
    :type module_path: str
    :param attribute_name: name of the attribute on the module (defaults to ``FILE_PREPROCESSORS``)
    :type attribute_name: str
    :return: the validated file pre-processor registry
    :rtype: PreprocessorRegistry
    :raises TypeError: if the attribute is neither a dictionary nor a sequence of callables
    :raises ValueError: if the registry is empty or uses the reserved ``_rejected`` table name
    """
    module = import_module(module_path)
    raw_preprocessors = getattr(module, attribute_name, None)
    if isinstance(raw_preprocessors, dict):
        return PreprocessorRegistry(registry=raw_preprocessors)
    if isinstance(raw_preprocessors, Sequence) and not isinstance(raw_preprocessors, (str, bytes)):
        return PreprocessorRegistry(default=tuple(raw_preprocessors))
    err_msg = (
        f"{module_path}.{attribute_name} must be a dictionary or a sequence of callables, "
        f"not {type(raw_preprocessors).__name__}"
    )
    raise TypeError(err_msg)
