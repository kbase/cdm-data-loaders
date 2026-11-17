"""Live pipeline configuration, column-type, preset, dialect, and rejected-schema table builders."""

import json
from types import MappingProxyType
from typing import Any, Final

from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import convert_xsv_schema_to_pyarrow
from cdm_data_loaders.converters.sources.json_schema.strict_scalars import NON_EXTENDABLE_SCALAR_HINTS
from cdm_data_loaders.core.buffer import REJECTED_PYARROW_SCHEMA
from cdm_data_loaders.pipelines.presets.pubmed import PubMedXmlToPyArrowSettings
from cdm_data_loaders.pipelines.presets.uniprot_xml import (
    UniProtXmlToPyArrowSettings,
    UniRef50XmlToPyArrowSettings,
    UniRef90XmlToPyArrowSettings,
    UniRef100XmlToPyArrowSettings,
)
from cdm_data_loaders.pipelines.xmltopyarrow.settings import XmlToPyArrowSettings
from cdm_data_loaders.readers.xsv.duckdb_reader import PARSING_CONFIG_KEYS, resolve_duckdb_column_config
from cdm_data_loaders.readers.xsv.models import CleanerValidatorArgs
from cdm_data_loaders.validation.jsonschema.dereference import dereference_schema
from cdm_data_loaders.validation.jsonschema.metaschema import (
    SUPPORTED_JSONSCHEMA_DRAFT_URIS,
    get_xsv_config_schema,
    resolve_jsonschema_validator,
)
from cdm_data_loaders.validation.jsonschema.models import ValidatedXsvSchema
from docs_macros.constants import PIPELINE_SPECS_ROOT
from docs_macros.tables.models import (
    RejectedColumnRow,
    SupportedDraftRow,
    XmlPresetRow,
    XsvColumnTypeRow,
    XsvConfigRow,
)
from specs._loader.converter_spec import load_cases

XMLTOPYARROW_SETTINGS_CLASSES: Final[tuple[type[XmlToPyArrowSettings], ...]] = (
    XmlToPyArrowSettings,
    UniProtXmlToPyArrowSettings,
    UniRef50XmlToPyArrowSettings,
    UniRef90XmlToPyArrowSettings,
    UniRef100XmlToPyArrowSettings,
    PubMedXmlToPyArrowSettings,
)

REJECTED_COLUMN_DESCRIPTIONS: Final[MappingProxyType[str, str]] = MappingProxyType(
    {
        "table_name": "Target entity table name (`None` for level-mode file pre-processor errors)",
        "source_file": "Input file path relative to `input_dir`",
        "line_no": "1-based physical line number in `source_file` (`None` for file-level errors)",
        "raw_record": "Stripped raw input line (`None` for file-level errors)",
        "error_detail": "Parser error message, JSON-encoded list of schema errors, or pre-processor error",
    }
)


def build_xsv_config_rows() -> tuple[XsvConfigRow, ...]:
    """Extract ``x-xsv-config`` metaschema and ``CleanerValidatorArgs`` field mapping rows.

    :return: Tuple of ``XsvConfigRow`` records.
    :rtype: tuple[XsvConfigRow, ...]
    """
    props: dict[str, Any] = get_xsv_config_schema()["properties"]
    arg_fields = CleanerValidatorArgs.model_fields
    rows: list[XsvConfigRow] = []
    for raw_key, prop_schema in props.items():
        stripped = raw_key.removeprefix("x-").replace("-", "_")
        constraint_dict = {k: v for k, v in prop_schema.items() if k != "description"}
        if stripped == "has_header":
            rows.append(
                XsvConfigRow.model_validate(
                    {
                        "key": raw_key,
                        "constraint_json": constraint_dict,
                        "target_field": "missing_header",
                        "is_has_header": True,
                        "metadata_only": False,
                        "default_repr": "False",
                    }
                )
            )
        elif stripped in PARSING_CONFIG_KEYS:
            target_field = PARSING_CONFIG_KEYS[stripped]
            default_val = arg_fields[target_field].default
            rows.append(
                XsvConfigRow.model_validate(
                    {
                        "key": raw_key,
                        "constraint_json": constraint_dict,
                        "target_field": target_field,
                        "is_has_header": False,
                        "metadata_only": False,
                        "default_repr": repr(default_val),
                    }
                )
            )
        else:
            rows.append(
                XsvConfigRow.model_validate(
                    {
                        "key": raw_key,
                        "constraint_json": constraint_dict,
                        "target_field": None,
                        "is_has_header": False,
                        "metadata_only": True,
                        "default_repr": None,
                    }
                )
            )
    return tuple(rows)


def _build_xsv_column_probes() -> list[dict[str, Any] | None]:
    """Derive XSV column probes from scalar type hints and the XSV specification case schemas.

    :return: Ordered list of distinct column JSON Schema dicts (plus ``None`` for unlisted ``required`` columns).
    :rtype: list[dict[str, Any] | None]
    """
    seen: set[str] = set()
    probes: list[dict[str, Any] | None] = []

    def add_probe(probe: dict[str, Any]) -> None:
        """Record ``probe`` if its canonical JSON key has not been seen.

        :param probe: Column JSON Schema dictionary.
        :type probe: dict[str, Any]
        :return: None.
        :rtype: None
        """
        key = json.dumps(probe, sort_keys=True)
        if key not in seen:
            seen.add(key)
            probes.append(probe)

    for scalar_type in (*NON_EXTENDABLE_SCALAR_HINTS, "string"):
        add_probe({"type": scalar_type})
        add_probe({"type": [scalar_type, "null"]})

    has_unlisted_required = False
    for case in load_cases(PIPELINE_SPECS_ROOT / "xsv" / "spec"):
        raw_schema = json.loads(case.schema.read_text(encoding="utf-8"))
        deref = dereference_schema(raw_schema)
        props = deref.get("properties", {})
        if any(col not in props for col in deref.get("required", ())):
            has_unlisted_required = True
        for prop_schema in props.values():
            cleaned = {k: v for k, v in prop_schema.items() if k not in {"title", "description"}}
            add_probe(cleaned)

    if has_unlisted_required:
        probes.append(None)
    return probes


def build_xsv_column_type_rows() -> tuple[XsvColumnTypeRow, ...]:
    """Extract XSV column type and nullability rows from ``convert_xsv_schema_to_pyarrow`` and DuckDB config.

    :return: Tuple of ``XsvColumnTypeRow`` records.
    :rtype: tuple[XsvColumnTypeRow, ...]
    """
    rows: list[XsvColumnTypeRow] = []
    for probe in _build_xsv_column_probes():
        properties: dict[str, Any] = {"col": dict(probe)} if probe is not None else {}
        raw_schema: dict[str, Any] = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["col"],
            "properties": properties,
        }
        pa_schema = convert_xsv_schema_to_pyarrow(ValidatedXsvSchema(jsonschema=raw_schema))
        pa_field = pa_schema.field("col")
        duckdb_cfg = resolve_duckdb_column_config(pa_schema if probe is not None else None)
        duckdb_dtype = str(duckdb_cfg.get("dtype", {}).get("col", "VARCHAR (auto)"))
        force_not_null = "col" in duckdb_cfg.get("force_not_null", [])
        rows.append(
            XsvColumnTypeRow.model_validate(
                {
                    "json_schema": probe,
                    "duckdb_dtype": duckdb_dtype,
                    "force_not_null": force_not_null,
                    "arrow_type_expr": pa_field.type,
                    "nullable": pa_field.nullable,
                }
            )
        )
    return tuple(rows)


def build_xmltopyarrow_preset_rows() -> tuple[XmlPresetRow, ...]:
    """Extract XML-to-PyArrow base and preset settings rows from ``XMLTOPYARROW_SETTINGS_CLASSES``.

    :return: Tuple of ``XmlPresetRow`` records.
    :rtype: tuple[XmlPresetRow, ...]
    """
    rows: list[XmlPresetRow] = []
    for cls in XMLTOPYARROW_SETTINGS_CLASSES:
        cli_name = str(cls.model_config.get("cli_prog_name", ""))
        dataset_default = getattr(cls, "dataset_name", None)
        xml_level = getattr(cls, "xml_level", None)
        xml_tag = getattr(cls, "xml_tag", None)
        root_tag = getattr(cls, "root_tag", None)
        table_name = getattr(cls, "table_name", None)
        glob_default = cls.model_fields["file_glob"].default
        preprocessors = getattr(cls, "DEFAULT_FILE_PREPROCESSORS", ())
        rows.append(
            XmlPresetRow.model_validate(
                {
                    "cli_name": cli_name,
                    "settings_class_name": cls.__name__,
                    "dataset_repr": repr(dataset_default) if isinstance(dataset_default, str) else None,
                    "xml_level": xml_level if isinstance(xml_level, int) else None,
                    "root_tag_repr": repr(root_tag) if isinstance(xml_level, int) else None,
                    "xml_tag_repr": repr(xml_tag) if isinstance(xml_tag, str) else None,
                    "table_name_repr": repr(table_name) if isinstance(xml_tag, str) else None,
                    "file_glob_repr": repr(glob_default),
                    "preprocessors": preprocessors,
                }
            )
        )
    return tuple(rows)


def build_jsonlines_jsonschema_draft_rows() -> tuple[SupportedDraftRow, ...]:
    """Extract supported JSON Schema draft URI and validator class rows via ``resolve_jsonschema_validator``.

    :return: Tuple of ``SupportedDraftRow`` records.
    :rtype: tuple[SupportedDraftRow, ...]
    """
    rows: list[SupportedDraftRow] = []
    for uri in SUPPORTED_JSONSCHEMA_DRAFT_URIS:
        validator_cls = resolve_jsonschema_validator({"$schema": uri})
        rows.append(SupportedDraftRow(schema_uri=uri, validator_class_name=validator_cls.__name__))
    return tuple(rows)


def build_rejected_schema_rows() -> tuple[RejectedColumnRow, ...]:
    """Extract ``REJECTED_PYARROW_SCHEMA`` column rows for the rejected schema reference table.

    :return: Tuple of ``RejectedColumnRow`` records.
    :rtype: tuple[RejectedColumnRow, ...]
    """
    return tuple(
        RejectedColumnRow.model_validate(
            {
                "name": field.name,
                "arrow_type_expr": field.type,
                "nullable": field.nullable,
                "description": REJECTED_COLUMN_DESCRIPTIONS.get(field.name, "—"),
            }
        )
        for field in REJECTED_PYARROW_SCHEMA
    )
