"""dlt schema -> JSON Schema draft 2020-12 conversion.

Converts dlt stored schemas (or plain tables dicts) into JSON Schema draft 2020-12
documents, folding nested child tables back into their parents.
"""

from cdm_data_loaders.converters.dlt_to_jsonschema.converter import (
    DltToJSONSchema,
    DltToJSONSchemaError,
)

__all__ = [
    "DltToJSONSchema",
    "DltToJSONSchemaError",
]
