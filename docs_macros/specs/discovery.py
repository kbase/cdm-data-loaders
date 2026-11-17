"""On-demand converter and pipeline specification auto-discovery from ``specs/`` manifests."""

import functools
import importlib
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.core.cli import SpecError
from docs_macros.constants import CONVERTER_SPECS_ROOT, DOCS_DIR, PIPELINE_SPECS_ROOT
from docs_macros.specs.models import (
    ConverterSpecConfig,
    PipelineSpecConfig,
    SourceFormatDefaults,
    TargetFormatDefaults,
)
from docs_macros.tables.registry import TABLE_TEMPLATE_REGISTRY
from specs._loader.yaml_io import load_yaml_dict

CONVERTER_SUITE_NAMES: Final[tuple[str, ...]] = ("spec", "examples")
CONVERTER_FACADES_PACKAGE: Final[str] = "cdm_data_loaders.converters.facades"
CONVERTER_FACADE_EXCEPTIONS_PACKAGE: Final[str] = "cdm_data_loaders.converters.exceptions.facades"

SOURCE_FORMAT_DEFAULTS: Final[MappingProxyType[str, SourceFormatDefaults]] = MappingProxyType(
    {
        "xsd": SourceFormatDefaults(schema_lang="xml", data_lang="xml", target_label="xml_tag"),
        "jsonschema": SourceFormatDefaults(schema_lang="json", data_lang="json"),
        "pydantic": SourceFormatDefaults(schema_lang="python", data_lang="json"),
        "pyarrow": SourceFormatDefaults(schema_lang="python", data_lang="python"),
        "pyiceberg": SourceFormatDefaults(schema_lang="python", data_lang="python"),
        "dlt": SourceFormatDefaults(schema_lang="json", data_lang="json"),
    }
)

TARGET_FORMAT_DEFAULTS: Final[MappingProxyType[str, TargetFormatDefaults]] = MappingProxyType(
    {
        "pyarrow": TargetFormatDefaults(
            expected_schema_lang="python",
            expected_schema_heading="PyArrow schema",
            run_output_heading="Converted rows",
        ),
        "jsonschema": TargetFormatDefaults(
            expected_schema_lang="json",
            expected_schema_heading="JSON Schema",
            run_output_heading="Serialized records",
        ),
        "linkml": TargetFormatDefaults(
            expected_schema_lang="yaml",
            expected_schema_heading="LinkML schema",
            run_output_heading="Serialized records",
        ),
        "dlt": TargetFormatDefaults(
            expected_schema_lang="json",
            expected_schema_heading="dlt schema",
            run_output_heading="Converted rows",
        ),
        "pyspark": TargetFormatDefaults(
            expected_schema_lang="python",
            expected_schema_heading="PySpark schema",
            run_output_heading="Converted rows",
        ),
    }
)

DEFAULT_PIPELINE_TABLES: Final[MappingProxyType[str, str]] = MappingProxyType(
    {
        "rejected_schema_table": "rejected_schema_table",
        "pydantic_scalar_table": "pydantic_scalar_table",
    }
)


def _resolve_spec_errors(
    exceptions_module_name: str,
    errors_attr: str,
    root_error_cls: type[Exception],
) -> tuple[type[Exception], tuple[type[Exception], ...]]:
    """Introspect ``exceptions_module_name`` to resolve ``(error_base, error_classes)``.

    :param exceptions_module_name: Dotted Python module path of the exceptions module.
    :type exceptions_module_name: str
    :param errors_attr: Tuple attribute name exporting the numbered error subclasses.
    :type errors_attr: str
    :param root_error_cls: Root exception base class (``ConversionError`` or ``SpecError``).
    :type root_error_cls: type[Exception]
    :return: Pair of ``(error_base_cls, error_classes_tuple)``.
    :rtype: tuple[type[Exception], tuple[type[Exception], ...]]
    """
    try:
        exceptions_mod = importlib.import_module(exceptions_module_name)
    except ModuleNotFoundError as exc:
        msg = f"Exceptions module {exceptions_module_name!r} does not define {errors_attr!r}"
        raise ValueError(msg) from exc
    if not hasattr(exceptions_mod, errors_attr):
        msg = f"Exceptions module {exceptions_module_name!r} does not define {errors_attr!r}"
        raise ValueError(msg)
    error_classes = tuple(getattr(exceptions_mod, errors_attr))
    candidates = [
        obj
        for obj in vars(exceptions_mod).values()
        if isinstance(obj, type)
        and issubclass(obj, Exception)
        and obj.__module__ == exceptions_mod.__name__
        and root_error_cls in obj.__bases__
        and all(issubclass(err_cls, obj) for err_cls in error_classes)
    ]
    if not candidates:
        msg = f"Cannot resolve direct {root_error_cls.__name__} base subclass in {exceptions_module_name!r}"
        raise ValueError(msg)
    return candidates[0], error_classes


def _resolve_generated_tables(
    name: str,
    source_fmt: str,
    raw_rules: Mapping[str, Any],
) -> MappingProxyType[str, str]:
    """Resolve ``generated:`` section table keys in ``rules.yaml`` to ``TABLE_TEMPLATE_REGISTRY`` IDs.

    :param name: Converter or pipeline identifier.
    :type name: str
    :param source_fmt: Source format prefix before ``_to_`` (empty for pipelines).
    :type source_fmt: str
    :param raw_rules: Parsed top-level ``rules.yaml`` mapping.
    :type raw_rules: Mapping[str, Any]
    :return: Read-only mapping from ``rules.yaml`` ``generated`` keys to ``TABLE_TEMPLATE_REGISTRY`` keys.
    :rtype: MappingProxyType[str, str]
    """
    resolved: dict[str, str] = {}
    for section in raw_rules.get("sections", ()) or ():
        if not isinstance(section, Mapping):
            continue
        gen_key = section.get("generated")
        if not isinstance(gen_key, str):
            continue
        if gen_key == "scalar_table":
            full_key = f"{name}_scalar_table"
            src_key = f"{source_fmt}_scalar_table"
            if full_key in TABLE_TEMPLATE_REGISTRY:
                resolved[gen_key] = full_key
            elif src_key in TABLE_TEMPLATE_REGISTRY:
                resolved[gen_key] = src_key
        elif gen_key in TABLE_TEMPLATE_REGISTRY:
            resolved[gen_key] = gen_key

    explicit = raw_rules.get("generated_tables")
    if isinstance(explicit, Mapping):
        resolved.update({str(k): str(v) for k, v in explicit.items()})
    return MappingProxyType(resolved)


def _build_converter_spec_config(
    data_dir: Path,
    docs_dir: Path,
    *,
    facade_package: str = CONVERTER_FACADES_PACKAGE,
    exceptions_package: str = CONVERTER_FACADE_EXCEPTIONS_PACKAGE,
) -> ConverterSpecConfig:
    """Construct a ``ConverterSpecConfig`` from a ``specs/schema_converters/<xxx_to_yyy>/`` directory.

    :param data_dir: Converter specification directory containing ``rules.yaml``.
    :type data_dir: Path
    :param docs_dir: Output ``docs/`` directory for ``<name>_specification.md``.
    :type docs_dir: Path
    :param facade_package: Base package containing converter facade modules.
    :type facade_package: str
    :param exceptions_package: Base package containing converter exception modules.
    :type exceptions_package: str
    :return: Populated ``ConverterSpecConfig`` instance.
    :rtype: ConverterSpecConfig
    """
    name = data_dir.name
    raw_rules = load_yaml_dict(data_dir / "rules.yaml")
    source_fmt, _, target_fmt = name.partition("_to_")
    src_defaults = SOURCE_FORMAT_DEFAULTS.get(source_fmt)
    tgt_defaults = TARGET_FORMAT_DEFAULTS.get(target_fmt)

    schema_lang = str(raw_rules.get("schema_lang", src_defaults.schema_lang if src_defaults else ""))
    data_lang = str(raw_rules.get("data_lang", src_defaults.data_lang if src_defaults else ""))
    if not schema_lang or not data_lang:
        msg = f"Cannot infer schema_lang/data_lang for converter {name!r}; set them in rules.yaml or SOURCE_FORMAT_DEFAULTS."
        raise ValueError(msg)

    target_label: str | None = raw_rules.get("target_label", src_defaults.target_label if src_defaults else None)
    expected_schema_lang = str(
        raw_rules.get("expected_schema_lang", tgt_defaults.expected_schema_lang if tgt_defaults else "")
    )
    expected_schema_heading = str(
        raw_rules.get("expected_schema_heading", tgt_defaults.expected_schema_heading if tgt_defaults else "")
    )
    run_output_heading = str(
        raw_rules.get("run_output_heading", tgt_defaults.run_output_heading if tgt_defaults else "")
    )
    if not expected_schema_lang or not expected_schema_heading or not run_output_heading:
        msg = f"Cannot infer target rendering defaults for converter {name!r}; set them in rules.yaml or TARGET_FORMAT_DEFAULTS."
        raise ValueError(msg)

    facade_module_name = str(raw_rules.get("facade_module", f"{facade_package}.{name}"))
    exceptions_module_name = str(raw_rules.get("exceptions_module", f"{exceptions_package}.{name}"))
    errors_attr = str(raw_rules.get("errors_attr", f"{name.upper()}_ERRORS"))
    error_base, error_classes = _resolve_spec_errors(exceptions_module_name, errors_attr, ConversionError)
    discovered_suites = tuple(suite for suite in CONVERTER_SUITE_NAMES if (data_dir / suite).is_dir())

    return ConverterSpecConfig(
        name=name,
        data_dir=data_dir,
        output=docs_dir / f"{name}_specification.md",
        suites=discovered_suites or ("spec",),
        error_manifest=data_dir / "spec" / "errors" / "manifest.yaml",
        error_base=error_base,
        error_classes=error_classes,
        schema_lang=schema_lang,
        data_lang=data_lang,
        generated_tables=_resolve_generated_tables(name, source_fmt, raw_rules),
        target_label=target_label,
        expected_schema_lang=expected_schema_lang,
        expected_schema_heading=expected_schema_heading,
        run_output_heading=run_output_heading,
        facade_module=facade_module_name,
    )


def _scan_converter_specs(
    specs_root: Path,
    docs_dir: Path,
    facade_package: str,
    exceptions_package: str,
) -> MappingProxyType[str, ConverterSpecConfig]:
    """Scan ``specs_root`` and construct ``ConverterSpecConfig`` mappings.

    :param specs_root: Root directory containing converter specification subdirectories.
    :type specs_root: Path
    :param docs_dir: Output ``docs/`` directory.
    :type docs_dir: Path
    :param facade_package: Base package containing converter facade modules.
    :type facade_package: str
    :param exceptions_package: Base package containing converter exception modules.
    :type exceptions_package: str
    :return: Read-only mapping from converter name to ``ConverterSpecConfig``.
    :rtype: MappingProxyType[str, ConverterSpecConfig]
    """
    if not specs_root.is_dir():
        return MappingProxyType({})
    configs: dict[str, ConverterSpecConfig] = {}
    for candidate in sorted(specs_root.iterdir()):
        if candidate.is_dir() and (candidate / "rules.yaml").is_file():
            configs[candidate.name] = _build_converter_spec_config(
                candidate,
                docs_dir,
                facade_package=facade_package,
                exceptions_package=exceptions_package,
            )
    return MappingProxyType(configs)


@functools.cache
def _cached_default_converter_specs() -> MappingProxyType[str, ConverterSpecConfig]:
    """Return cached converter specifications for the repository default roots.

    :return: Read-only mapping from converter name to ``ConverterSpecConfig``.
    :rtype: MappingProxyType[str, ConverterSpecConfig]
    """
    return _scan_converter_specs(
        CONVERTER_SPECS_ROOT,
        DOCS_DIR,
        CONVERTER_FACADES_PACKAGE,
        CONVERTER_FACADE_EXCEPTIONS_PACKAGE,
    )


def discover_converter_specs(
    specs_root: Path = CONVERTER_SPECS_ROOT,
    docs_dir: Path = DOCS_DIR,
    *,
    facade_package: str = CONVERTER_FACADES_PACKAGE,
    exceptions_package: str = CONVERTER_FACADE_EXCEPTIONS_PACKAGE,
) -> MappingProxyType[str, ConverterSpecConfig]:
    """Auto-discover all converter specification suites under ``specs_root``.

    :param specs_root: Root directory containing ``<source>_to_<target>/rules.yaml`` subdirectories.
    :type specs_root: Path
    :param docs_dir: Output ``docs/`` directory for ``<name>_specification.md`` files.
    :type docs_dir: Path
    :param facade_package: Base package containing ``<source>_to_<target>`` facade modules.
    :type facade_package: str
    :param exceptions_package: Base package containing ``<source>_to_<target>`` exception modules.
    :type exceptions_package: str
    :return: Read-only mapping from converter name to ``ConverterSpecConfig``.
    :rtype: MappingProxyType[str, ConverterSpecConfig]
    """
    if (
        specs_root == CONVERTER_SPECS_ROOT
        and docs_dir == DOCS_DIR
        and facade_package == CONVERTER_FACADES_PACKAGE
        and exceptions_package == CONVERTER_FACADE_EXCEPTIONS_PACKAGE
    ):
        return _cached_default_converter_specs()
    return _scan_converter_specs(specs_root, docs_dir, facade_package, exceptions_package)


def _build_pipeline_converter_config(
    name: str,
    data_dir: Path,
    docs_dir: Path,
    raw_rules: Mapping[str, Any],
) -> ConverterSpecConfig | None:
    """Build optional ``ConverterSpecConfig`` when a pipeline ``rules.yaml`` declares ``exceptions_module``.

    :param name: Pipeline family identifier.
    :type name: str
    :param data_dir: Pipeline specification directory.
    :type data_dir: Path
    :param docs_dir: Output ``docs/`` directory.
    :type docs_dir: Path
    :param raw_rules: Parsed top-level ``rules.yaml`` mapping.
    :type raw_rules: Mapping[str, Any]
    :return: ``ConverterSpecConfig`` for schema-backed pipelines, or ``None`` otherwise.
    :rtype: ConverterSpecConfig | None
    """
    exceptions_mod_name = raw_rules.get("exceptions_module")
    if not isinstance(exceptions_mod_name, str) or not exceptions_mod_name:
        return None
    errors_attr = str(raw_rules.get("errors_attr", f"{name.upper()}_ERRORS"))
    schema_lang = str(raw_rules.get("schema_lang", ""))
    data_lang = str(raw_rules.get("data_lang", ""))
    if not schema_lang or not data_lang:
        msg = f"Schema-backed pipeline {name!r} must declare schema_lang and data_lang in rules.yaml"
        raise ValueError(msg)
    error_base, error_classes = _resolve_spec_errors(exceptions_mod_name, errors_attr, SpecError)
    return ConverterSpecConfig(
        name=name,
        data_dir=data_dir,
        output=docs_dir / f"{name}_specification.md",
        suites=("spec",),
        error_manifest=data_dir / "spec" / "errors" / "manifest.yaml",
        error_base=error_base,
        error_classes=error_classes,
        schema_lang=schema_lang,
        data_lang=data_lang,
        generated_tables=_resolve_generated_tables(name, "", raw_rules),
        target_label=raw_rules.get("target_label"),
    )


def _build_pipeline_spec_config(data_dir: Path, docs_dir: Path) -> PipelineSpecConfig:
    """Construct a ``PipelineSpecConfig`` from ``specs/pipelines/<family>/`` and its ``rules.yaml``.

    :param data_dir: Pipeline specification directory containing ``rules.yaml``.
    :type data_dir: Path
    :param docs_dir: Output ``docs/`` directory for ``<name>_specification.md``.
    :type docs_dir: Path
    :return: Populated ``PipelineSpecConfig`` instance.
    :rtype: PipelineSpecConfig
    """
    name = data_dir.name
    raw_rules = load_yaml_dict(data_dir / "rules.yaml")
    explicit_presets = raw_rules.get("presets")
    if isinstance(explicit_presets, list | tuple):
        presets = tuple(str(item) for item in explicit_presets)
    else:
        presets_dir = data_dir / "presets"
        presets = (
            tuple(sorted(p.name for p in presets_dir.iterdir() if p.is_dir() and not p.name.startswith("_")))
            if presets_dir.is_dir()
            else ()
        )
    shared_files = tuple(sorted(p for p in data_dir.glob("*.py") if p.is_file() and p.name != "__init__.py"))
    converter_cfg = _build_pipeline_converter_config(name, data_dir, docs_dir, raw_rules)
    return PipelineSpecConfig(
        name=name,
        data_dir=data_dir,
        output_path=docs_dir / f"{name}_specification.md",
        presets=presets,
        shared_files=shared_files,
        converter_config=converter_cfg,
    )


def _scan_pipeline_specs(specs_root: Path, docs_dir: Path) -> MappingProxyType[str, PipelineSpecConfig]:
    """Scan ``specs_root`` and construct ``PipelineSpecConfig`` mappings.

    :param specs_root: Root directory containing pipeline specification subdirectories.
    :type specs_root: Path
    :param docs_dir: Output ``docs/`` directory.
    :type docs_dir: Path
    :return: Read-only mapping from pipeline family name to ``PipelineSpecConfig``.
    :rtype: MappingProxyType[str, PipelineSpecConfig]
    """
    if not specs_root.is_dir():
        return MappingProxyType({})
    configs: dict[str, PipelineSpecConfig] = {}
    for candidate in sorted(specs_root.iterdir()):
        if candidate.is_dir() and (candidate / "rules.yaml").is_file():
            configs[candidate.name] = _build_pipeline_spec_config(candidate, docs_dir)
    return MappingProxyType(configs)


@functools.cache
def _cached_default_pipeline_specs() -> MappingProxyType[str, PipelineSpecConfig]:
    """Return cached pipeline specifications for the repository default roots.

    :return: Read-only mapping from pipeline family name to ``PipelineSpecConfig``.
    :rtype: MappingProxyType[str, PipelineSpecConfig]
    """
    return _scan_pipeline_specs(PIPELINE_SPECS_ROOT, DOCS_DIR)


def discover_pipeline_specs(
    specs_root: Path = PIPELINE_SPECS_ROOT,
    docs_dir: Path = DOCS_DIR,
) -> MappingProxyType[str, PipelineSpecConfig]:
    """Auto-discover all pipeline specification suites under ``specs_root``.

    :param specs_root: Root directory containing ``<family>/rules.yaml`` subdirectories.
    :type specs_root: Path
    :param docs_dir: Output ``docs/`` directory for ``<name>_specification.md`` files.
    :type docs_dir: Path
    :return: Read-only mapping from pipeline family name to ``PipelineSpecConfig``.
    :rtype: MappingProxyType[str, PipelineSpecConfig]
    """
    if specs_root == PIPELINE_SPECS_ROOT and docs_dir == DOCS_DIR:
        return _cached_default_pipeline_specs()
    return _scan_pipeline_specs(specs_root, docs_dir)


def resolve_converter_config(
    name_or_config: str | ConverterSpecConfig,
    *,
    specs_root: Path = CONVERTER_SPECS_ROOT,
    docs_dir: Path = DOCS_DIR,
) -> ConverterSpecConfig:
    """Resolve a converter name or ``ConverterSpecConfig`` instance.

    :param name_or_config: Converter spec key or a ``ConverterSpecConfig``.
    :type name_or_config: str | ConverterSpecConfig
    :param specs_root: Root directory containing converter specification directories.
    :type specs_root: Path
    :param docs_dir: Output ``docs/`` directory for ``<name>_specification.md``.
    :type docs_dir: Path
    :return: Resolved ``ConverterSpecConfig``.
    :rtype: ConverterSpecConfig
    """
    if isinstance(name_or_config, ConverterSpecConfig):
        return name_or_config
    discovered = discover_converter_specs(specs_root=specs_root, docs_dir=docs_dir)
    if name_or_config not in discovered:
        msg = f"Unknown converter specification: {name_or_config!r}"
        raise KeyError(msg)
    return discovered[name_or_config]


def resolve_pipeline_config(
    name_or_config: str | PipelineSpecConfig,
    *,
    specs_root: Path = PIPELINE_SPECS_ROOT,
    docs_dir: Path = DOCS_DIR,
) -> PipelineSpecConfig:
    """Resolve a pipeline family name or ``PipelineSpecConfig`` instance.

    :param name_or_config: Pipeline spec key or a ``PipelineSpecConfig``.
    :type name_or_config: str | PipelineSpecConfig
    :param specs_root: Root directory containing pipeline specification directories.
    :type specs_root: Path
    :param docs_dir: Output ``docs/`` directory for ``<name>_specification.md``.
    :type docs_dir: Path
    :return: Resolved ``PipelineSpecConfig``.
    :rtype: PipelineSpecConfig
    """
    if isinstance(name_or_config, PipelineSpecConfig):
        return name_or_config
    discovered = discover_pipeline_specs(specs_root=specs_root, docs_dir=docs_dir)
    if name_or_config not in discovered:
        msg = f"Unknown pipeline specification: {name_or_config!r}"
        raise KeyError(msg)
    return discovered[name_or_config]
