"""JSON Schema -> dlt schema conversion.

Converts fully-dereferenced JSON Schema documents into dlt stored-schema dicts
(loadable via `dlt.Schema.from_dict`) or YAML for dlt's import-schema workflow.
"""

from cdm_data_loaders.converters.jsonschema_to_dlt.converter import (
    InvalidJSONSchemaError,
    JSONSchemaToDlt,
    JSONSchemaToDltError,
)

__all__ = [
    "InvalidJSONSchemaError",
    "JSONSchemaToDlt",
    "JSONSchemaToDltError",
]
