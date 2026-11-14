# Schema Converters Guide

`cdm-data-loaders` provides CLI tools and Python facades under [`src/cdm_data_loaders/converters`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters) for translating table and document schemas across JSON Schema (Draft 2020-12), Pydantic v2, XML Schema (XSD), PyArrow, PyIceberg, LinkML, `dlt`, and PySpark.

For formal conversion rules, scalar type mappings, and error codes (`E1..En`), see:

- [JSON Schema to PyArrow Specification](jsonschema_to_pyarrow_specification.md)
- [Pydantic to PyArrow Specification](pydantic_to_pyarrow_specification.md)
- [XSD to PyArrow Specification](xsd_to_pyarrow_specification.md)
- [PyArrow to JSON Schema Specification](pyarrow_to_jsonschema_specification.md)
- [PyArrow to LinkML Specification](pyarrow_to_linkml_specification.md)

---

## Running Converter CLIs

Three schema converter entry points are registered in [`pyproject.toml`](https://github.com/kbase/cdm-data-loaders/blob/develop/pyproject.toml). See the [CLI Reference](cli_reference.md) for all flags and `CDL_*` environment variables.

### Generating Static PyArrow Modules from XSD (`xsd_to_pyarrow`)

[`xsd_to_pyarrow`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters/cli/xsd_to_pyarrow.py) reads an `.xsd` schema and writes a formatted Python module containing reusable `pa.struct` components and `pa.Schema` definitions ([CLI options](cli_reference.md#xsd_to_pyarrow)):

```sh
# generate a PyArrow schema module for a single repeating XML element (<entry>)
uv run xsd_to_pyarrow \
  --xsd-file src/cdm_data_loaders/schemas/uniprot/uniprot.xsd \
  --xml-tag entry \
  --output ./generated/uniprot_schema.py

# generate schemas for all XML elements at depth 1 beneath an explicit root tag
uv run xsd_to_pyarrow \
  --xsd-file src/cdm_data_loaders/schemas/uniref/uniref.xsd \
  --root-tag UniRef \
  --xml-level 1 \
  --max-recursion-depth 3 \
  --output ./generated/uniref_schema.py
```

### Exporting Iceberg Catalog Schemas (`pyiceberg_to_jsonschema` and `pyiceberg_to_linkml`)

[`pyiceberg_to_jsonschema`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters/cli/pyiceberg_to_jsonschema.py) ([CLI options](cli_reference.md#pyiceberg_to_jsonschema)) and [`pyiceberg_to_linkml`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters/cli/pyiceberg_to_linkml.py) ([CLI options](cli_reference.md#pyiceberg_to_linkml)) connect to a named PyIceberg catalog and export table schemas as Draft 2020-12 `.schema.json` or LinkML `.schema.yaml` files.

Configure the catalog in `~/.pyiceberg.yaml` (or via `PYICEBERG_CATALOG__*` environment variables):

```yaml
catalog:
  local_lakehouse:
    type: sql
    uri: sqlite:////path/to/warehouse/iceberg_catalog.db
    warehouse: file:///path/to/warehouse
```

Run either catalog dumper per table or grouped per namespace (`--group-by-namespace` / `-g`):

```sh
# write one <namespace>.<table>.schema.json file per Iceberg table
uv run pyiceberg_to_jsonschema \
  --catalog local_lakehouse \
  --output-dir ./exported_jsonschemas

# write one <namespace>.schema.yaml LinkML file per non-empty namespace
uv run pyiceberg_to_linkml \
  --catalog local_lakehouse \
  --output-dir ./exported_linkml \
  --group-by-namespace
```

---

## Converting Schemas to PyArrow in Python

Use [`JSONSchemaToPyArrow`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters/facades/jsonschema_to_pyarrow.py), [`PydanticToPyArrow`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters/facades/pydantic_to_pyarrow.py), and [`XsdToPyArrow`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters/facades/xsd_to_pyarrow.py) to build `pyarrow.Schema` objects, zero-row `pyarrow.Table` templates, or static Python schema modules:

```python
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
from pydantic import BaseModel

from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import JSONSchemaToPyArrow
from cdm_data_loaders.converters.facades.pydantic_to_pyarrow import PydanticToPyArrow
from cdm_data_loaders.converters.facades.xsd_to_pyarrow import XsdToPyArrow

GENOME_JSON_SCHEMA: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["assembly_id", "contig_count"],
    "properties": {
        "assembly_id": {"type": "string"},
        "contig_count": {"type": "integer"},
        "gc_percent": {"type": ["number", "null"]},
    },
}


class ContigFeature(BaseModel):
    """Example Pydantic record model."""

    contig_id: str
    start: int
    end: int
    strand: str | None = None


# convert Draft 2020-12 JSON Schema to pa.Schema
genome_arrow_schema: pa.Schema = JSONSchemaToPyArrow().convert(GENOME_JSON_SCHEMA)

# convert Pydantic BaseModel to pa.Schema or static Python code
feature_converter = PydanticToPyArrow()
feature_arrow_schema: pa.Schema = feature_converter.convert(ContigFeature)
feature_module_code: str = feature_converter.convert_to_python(ContigFeature)

# convert an XSD element to pa.Schema
uniprot_xsd = Path("src/cdm_data_loaders/schemas/uniprot/uniprot.xsd")
entry_arrow_schema: pa.Schema = XsdToPyArrow(xml_tag="entry").convert(uniprot_xsd)
```

---

## Exporting PyArrow and PyIceberg Schemas to JSON Schema and LinkML

Use [`PyArrowToJSONSchema`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters/facades/pyarrow_to_jsonschema.py) and [`PyArrowToLinkML`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters/facades/pyarrow_to_linkml.py) for `pyarrow.Schema` objects, or [`table_to_json_schema`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters/facades/pyiceberg_to_jsonschema.py) and [`table_to_linkml`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters/facades/pyiceberg_to_linkml.py) for loaded `pyiceberg.table.Table` instances:

```python
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import load_catalog

from cdm_data_loaders.converters.facades.pyarrow_to_jsonschema import PyArrowToJSONSchema
from cdm_data_loaders.converters.facades.pyarrow_to_linkml import PyArrowToLinkML
from cdm_data_loaders.converters.facades.pyiceberg_to_jsonschema import table_to_json_schema
from cdm_data_loaders.converters.facades.pyiceberg_to_linkml import table_to_linkml

arrow_schema = pa.schema(
    [
        pa.field("sample_id", pa.string(), nullable=False),
        pa.field("read_count", pa.int64()),
    ]
)

# export pa.Schema to Draft 2020-12 JSON Schema dict and LinkML YAML
json_schema_doc: dict[str, Any] = PyArrowToJSONSchema(schema_name="SampleStats").convert(arrow_schema)
linkml_yaml: str = PyArrowToLinkML(schema_name="sample_stats").convert_to_yaml(arrow_schema)

# export a loaded PyIceberg table directly without writing files
with load_catalog("local_lakehouse") as catalog:
    identifier = ("cdm", "sample_stats")
    iceberg_table = catalog.load_table(identifier)
    iceberg_json_schema = table_to_json_schema(iceberg_table, identifier)
    iceberg_linkml_model = table_to_linkml(iceberg_table, identifier)
```

---

## Translating Between JSON Schema, `dlt`, and PySpark

Use [`JSONSchemaToDlt`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters/facades/jsonschema_to_dlt.py), [`DltToJSONSchema`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters/facades/dlt_to_jsonschema.py), and [`JSONSchemaToPySpark`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters/facades/jsonschema_to_pyspark.py) when bridging JSON Schema documents with `dlt` table schemas or Spark `StructType` definitions:

```python
from typing import Any, Final

from pyspark.sql.types import StructType

from cdm_data_loaders.converters.facades.dlt_to_jsonschema import DltToJSONSchema
from cdm_data_loaders.converters.facades.jsonschema_to_dlt import JSONSchemaToDlt
from cdm_data_loaders.converters.facades.jsonschema_to_pyspark import JSONSchemaToPySpark

ASSEMBLY_SCHEMA: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "assembly",
    "type": "object",
    "required": ["accession"],
    "properties": {
        "accession": {"type": "string", "x-dlt": {"primary_key": True}},
        "genome_size": {"type": "integer"},
    },
}

# convert JSON Schema into a stored dlt schema dict or live dlt.Schema
dlt_converter = JSONSchemaToDlt(schema_name="ncbi")
stored_dlt_schema = dlt_converter.convert(ASSEMBLY_SCHEMA)
live_dlt_schema = dlt_converter.to_schema(ASSEMBLY_SCHEMA)

# round-trip a stored dlt schema mapping back into per-table JSON Schemas
table_json_schemas: dict[str, dict[str, Any]] = DltToJSONSchema().convert(dict(stored_dlt_schema))

# convert JSON Schema into a PySpark StructType
spark_struct: StructType = JSONSchemaToPySpark().convert(ASSEMBLY_SCHEMA)
```

---

## Composing Readers, Emitters, and Custom `x-` Extensions

All converters translate source schemas into the immutable intermediate representation in [`converters/core/ir.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters/core/ir.py) before emitting target schemas. Vendor `x-*` extension namespaces are validated against Draft 2020-12 contracts in [`converters/core/extensions.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/src/cdm_data_loaders/converters/core/extensions.py) (`DEFAULT_EXTENSIONS` registers `x-dlt`, `x-iceberg`, `x-pyarrow`, and `x-xsv-config`).

To accept and preserve a custom `x-*` extension namespace during conversion, register an `ExtensionSpec` on `DEFAULT_EXTENSIONS` (which returns a new immutable `ExtensionRegistry`) and pass `extension_registry` to the facade or reader:

```python
from typing import Any, Final

from cdm_data_loaders.converters.core.extensions import (
    DEFAULT_EXTENSIONS,
    ExtensionRegistry,
    ExtensionSpec,
)
from cdm_data_loaders.converters.emitters.json_schema import JsonSchemaEmitter
from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import JSONSchemaToPyArrow
from cdm_data_loaders.converters.readers.json_schema import JsonSchemaReader

KBASE_EXTENSION_SPEC: Final[ExtensionSpec] = ExtensionSpec(
    "x-kbase",
    {
        "type": "object",
        "properties": {
            "ontology_term": {"type": "string"},
            "indexed": {"type": "boolean"},
        },
        "additionalProperties": False,
    },
)

CUSTOM_REGISTRY: Final[ExtensionRegistry] = DEFAULT_EXTENSIONS.register(KBASE_EXTENSION_SPEC)

ANNOTATED_SCHEMA: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["gene_id"],
    "properties": {
        "gene_id": {
            "type": "string",
            "x-kbase": {"ontology_term": "SO:0000704", "indexed": True},
        },
    },
}

# pass the custom registry to a facade
arrow_schema = JSONSchemaToPyArrow(
    extension_registry=CUSTOM_REGISTRY,
    extra_metadata_keywords=frozenset({"x-kbase"}),
).convert(ANNOTATED_SCHEMA)

# or compose a reader and emitter directly over the IR SchemaDocument
ir_document = JsonSchemaReader(extension_registry=CUSTOM_REGISTRY).read(ANNOTATED_SCHEMA)
round_tripped_json_schema: dict[str, Any] = JsonSchemaEmitter().emit(ir_document)
```
