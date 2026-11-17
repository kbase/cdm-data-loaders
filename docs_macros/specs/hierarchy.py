"""Preset settings subclass introspection for pipeline specification hierarchy tables."""

from typing import Any, Final

from cdm_data_loaders.core.pipeline_catalog import resolve_pipeline
from docs_macros.formatting import format_value_code
from docs_macros.specs.models import (
    PipelineSpecConfig,
    PresetDetailContext,
    PresetOverrideRowContext,
    SettingsHierarchyContext,
)

PRESET_EXTRA_ATTRS: Final[tuple[str, ...]] = (
    "PRECOMPUTED_SCHEMAS",
    "PRECOMPUTED_SCHEMA",
    "DEFAULT_FILE_PREPROCESSORS",
)


def introspect_preset_overrides(
    base_cls: type[Any],
    preset_cls: type[Any],
) -> tuple[PresetOverrideRowContext, ...]:
    """Compare ``preset_cls`` against ``base_cls`` to extract locked and overridden settings attributes.

    :param base_cls: Base pipeline settings class.
    :type base_cls: type[Any]
    :param preset_cls: Preset pipeline settings subclass.
    :type preset_cls: type[Any]
    :return: Tuple of ``PresetOverrideRowContext`` rows.
    :rtype: tuple[PresetOverrideRowContext, ...]
    """
    rows: list[PresetOverrideRowContext] = []
    base_fields = base_cls.model_fields
    preset_fields = preset_cls.model_fields

    for field_name, base_field in base_fields.items():
        if field_name not in preset_fields and hasattr(preset_cls, field_name):
            rows.append(
                PresetOverrideRowContext(
                    attr_name=field_name,
                    kind="Locked `ClassVar` (CLI flag removed)",
                    base_val=format_value_code(base_field.default),
                    preset_val=format_value_code(getattr(preset_cls, field_name)),
                )
            )
        elif field_name in preset_fields and preset_fields[field_name].default != base_field.default:
            rows.append(
                PresetOverrideRowContext(
                    attr_name=field_name,
                    kind="Overridden field default",
                    base_val=format_value_code(base_field.default),
                    preset_val=format_value_code(preset_fields[field_name].default),
                )
            )

    for field_name, preset_field in preset_fields.items():
        if field_name not in base_fields:
            rows.append(
                PresetOverrideRowContext(
                    attr_name=field_name,
                    kind="Added preset field",
                    base_val="—",
                    preset_val=format_value_code(preset_field.default),
                )
            )

    for extra_attr in PRESET_EXTRA_ATTRS:
        val = getattr(preset_cls, extra_attr, None)
        if val:
            base_val = getattr(base_cls, extra_attr, None)
            if val != base_val:
                rows.append(
                    PresetOverrideRowContext(
                        attr_name=extra_attr,
                        kind="Preset class binding",
                        base_val=format_value_code(base_val) if base_val else "`None`",
                        preset_val=format_value_code(val),
                    )
                )
    return tuple(rows)


def build_hierarchy_context(spec: PipelineSpecConfig) -> SettingsHierarchyContext | None:
    """Build ``SettingsHierarchyContext`` when ``spec`` defines preset subclasses.

    :param spec: Pipeline specification configuration.
    :type spec: PipelineSpecConfig
    :return: ``SettingsHierarchyContext`` or ``None`` when ``spec.presets`` is empty.
    :rtype: SettingsHierarchyContext | None
    """
    if not spec.presets:
        return None

    base_resolved = resolve_pipeline(spec.name)
    base_cls = base_resolved.settings_cls
    if base_cls is None:
        msg = f"Pipeline {spec.name!r} has no settings_cls"
        raise ValueError(msg)

    presets: list[PresetDetailContext] = []

    for preset_name in spec.presets:
        preset_resolved = resolve_pipeline(preset_name)
        preset_cls = preset_resolved.settings_cls
        if preset_cls is None or preset_resolved.pipeline_fn is None:
            msg = f"Preset pipeline {preset_name!r} has no settings_cls or pipeline_fn"
            raise ValueError(msg)

        shared_runner = f"{preset_resolved.pipeline_fn.__module__}.{preset_resolved.pipeline_fn.__name__}"
        presets.append(
            PresetDetailContext.model_validate(
                {
                    "cli_command": preset_resolved.cli_command,
                    "class_name": preset_cls.__name__,
                    "settings_api_url": preset_cls,
                    "base_class_name": base_cls.__name__,
                    "shared_runner": shared_runner,
                    "shared_runner_url": preset_resolved.pipeline_fn.__module__,
                    "overrides": introspect_preset_overrides(base_cls, preset_cls),
                }
            )
        )

    return SettingsHierarchyContext.model_validate(
        {
            "base_cli_command": base_resolved.cli_command,
            "base_class_name": base_cls.__name__,
            "base_settings_url": base_cls.__module__,
            "runner_name": base_resolved.pipeline_fn.__name__ if base_resolved.pipeline_fn else "",
            "runner_url": base_resolved.pipeline_fn.__module__ if base_resolved.pipeline_fn else None,
            "presets": tuple(presets),
        }
    )
