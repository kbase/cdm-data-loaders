<!-- GENERATED FILE - DO NOT EDIT. Source: `specs/pipelines/xmltodict/`. Regenerate with `uv run python -m scripts.gen_pipeline_spec` or `uv run python -m scripts.gen_converter_spec`. -->

# XML to Dict Pipeline Specification

This document specifies how the **XML to Dict ingestion pipeline**
(`cdm_data_loaders.pipelines.xmltodict.pipeline`, `cdm_data_loaders.pipelines.xmltodict.settings.XmlToDictSettings`,
and `cdm_data_loaders.readers.xml.process_xml_file_to_dict`) discovers, pre-processes, streams, parses, transforms,
and loads XML (`.xml` and `.xml.gz`) files without an XSD schema:

1. **Target selection, file discovery, `.gz` decompression, and file pre-processing** (`XmlToDictSettings`, `build_per_file_filesystem_resource`, `REJECTED_PYARROW_SCHEMA`).
2. **Outermost `<xml_tag>` streaming and `xmltodict` parsing** (`stream_xml_file`, `_parse_element_to_dict`).
3. **Table transforms, PyArrow output schema coercion, and pipeline reporting** (`route_and_coerce_record`, `DictBuffer`, `run_xmltodict_pipeline`).

| Property | Value |
| --- | --- |
| **Base Pipeline Family** | `xmltodict` |
| **Base CLI Entry Point** | `xmltodict` |
| **Base Settings Class** | `cdm_data_loaders.pipelines.xmltodict.settings.XmlToDictSettings` |
| **Pipeline Runner** | `cdm_data_loaders.pipelines.xmltodict.pipeline.run_xmltodict_pipeline` |
| **Preset Subclasses Covered** | — |
| **Specification Directory** | `specs/pipelines/xmltodict/` |

## Running Smoke Tests Across Environments

```bash
# Run all base and preset smoke tests for xmltodict across built-in environments
uv run pytest tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py --smoke-pipeline xmltodict

# Run against a custom environment directory (config.toml, secrets.toml, settings.toml)
uv run pytest tests/cdm_data_loaders/pipelines/test_pipeline_smoke_spec.py \
    --smoke-pipeline xmltodict \
    --dlt-config-dir /path/to/env_dlt_config \
    --settings-file /path/to/env_dlt_config/settings.toml
```

---

## 1. Conversion & Pipeline Rules

### 1.1 Target Selection, File Discovery, and Pre-processing

<a id="r-xmltodict-target-and-glob"></a>

- **R-XMLTODICT-TARGET-AND-GLOB**: `XmlToDictSettings` requires `dataset_name`, `table_name` (`-t`), and `xml_tag` (`-x`), and discovers
  matching files in `settings.input_dir` using `settings.file_glob` (default `"*.xml*"`), including nested
  subdirectories and `.gz`-compressed XML archives. Non-matching files are ignored.

  *Cases:* [`spec/01_outermost_elements_gzip_and_xmlns`](#case-spec-01-outermost-elements-gzip-and-xmlns)

<a id="r-xmltodict-preprocess-md5"></a>

- **R-XMLTODICT-PREPROCESS-MD5**: Before streaming an XML file, `build_per_file_filesystem_resource` runs
  `settings.get_file_preprocessors(settings.table_name)` (resolved from `preprocessors_module`, e.g.
  `validate_md5_sidecar`). When a pre-processor raises `FilePreprocessError`, the file is skipped, omitted
  from `ingested_files`, recorded in `report.txt` `load_errors` (`status: completed with errors`), and routed
  to `_rejected` (`REJECTED_PYARROW_SCHEMA`) with `table_name=settings.table_name`,
  `source_file=<rel_path>`, `line_no=None`, and `raw_record=None`.

  *Cases:* [`spec/02_md5_sidecar_preprocessing`](#case-spec-02-md5-sidecar-preprocessing)

| Column | PyArrow `DataType` | `Field.nullable` | Description |
| :--- | :--- | :--- | :--- |
| `table_name` | `pa.string()` | `True` | Target entity table name (`None` for level-mode file pre-processor errors) |
| `source_file` | `pa.string()` | `True` | Input file path relative to `input_dir` |
| `line_no` | `pa.int64()` | `True` | 1-based physical line number in `source_file` (`None` for file-level errors) |
| `raw_record` | `pa.string()` | `True` | Stripped raw input line (`None` for file-level errors) |
| `error_detail` | `pa.string()` | `True` | Parser error message, JSON-encoded list of schema errors, or pre-processor error |

### 1.2 Outermost Element Streaming and `xmltodict` Parsing

<a id="r-xmltodict-outermost-stream"></a>

- **R-XMLTODICT-OUTERMOST-STREAM**: `stream_xml_file` streams only the outermost XML elements matching `settings.xml_tag` using
  `lxml.etree.iterparse(..., events=("start", "end"), tag=xml_tag, remove_blank_text=True)` and clears
  processed subtrees from memory. Any `<xml_tag>` element nested inside an enclosing `<xml_tag>` is preserved
  in place as part of the outer element rather than emitted as a separate record.

  *Cases:* [`spec/01_outermost_elements_gzip_and_xmlns`](#case-spec-01-outermost-elements-gzip-and-xmlns), [`spec/03_table_transforms_and_output_schemas`](#case-spec-03-table-transforms-and-output-schemas)

<a id="r-xmltodict-parse"></a>

- **R-XMLTODICT-PARSE**: `_parse_element_to_dict` serializes each streamed element with `lxml.etree.tostring` and parses it via
  `xmltodict.parse(..., attr_prefix="_")`, producing `{"<xml_tag>": {...}}` with attributes prefixed by `_`,
  element text in `#text` when combined with attributes, and root-level `_xmlns*` namespace declarations
  stripped.

  *Cases:* [`spec/01_outermost_elements_gzip_and_xmlns`](#case-spec-01-outermost-elements-gzip-and-xmlns), [`spec/03_table_transforms_and_output_schemas`](#case-spec-03-table-transforms-and-output-schemas)

### 1.3 Table Transforms, PyArrow Output Schemas, and Reporting

<a id="r-xmltodict-transforms"></a>

- **R-XMLTODICT-TRANSFORMS**: Each parsed `{"<xml_tag>": {...}}` dictionary is routed through `route_and_coerce_record` with
  `default_table=settings.table_name`, `transform=settings.get_table_transform(settings.table_name)`
  (`transforms_module`), and `schema_map=settings.schema_map` (`output_schemas_module`). Transforms may
  unwrap the outer `<xml_tag>` key, filter records (`{}` / `None`), or fan out child elements across multiple
  destination tables coerced to `pa.Table` batches via `DictBuffer`.

  *Cases:* [`spec/03_table_transforms_and_output_schemas`](#case-spec-03-table-transforms-and-output-schemas)

<a id="r-xmltodict-report"></a>

- **R-XMLTODICT-REPORT**: `run_xmltodict_pipeline` executes the `xmltodict_reader` resource (`parallelized=False`) in a single
  `pipeline.run` call and writes `report.txt` (`status`, `ingested_files`, `load_errors`,
  `pipeline_load_info_errs`) and `_load_info`.

  *Cases:* [`spec/01_outermost_elements_gzip_and_xmlns`](#case-spec-01-outermost-elements-gzip-and-xmlns), [`spec/02_md5_sidecar_preprocessing`](#case-spec-02-md5-sidecar-preprocessing), [`spec/03_table_transforms_and_output_schemas`](#case-spec-03-table-transforms-and-output-schemas)

---

## 2. Case Coverage Matrix

| Suite | Target CLI | Settings Class | Case | Title | Rules | Expected Table Counts |
| --- | --- | --- | --- | --- | --- | --- |
| `spec` | `xmltodict` | `XmlToDictSettings` | [`01_outermost_elements_gzip_and_xmlns`](#case-spec-01-outermost-elements-gzip-and-xmlns) | Outermost `<xml_tag>` Streaming, Self-Nested Tags, Gzip Decompression, and `_xmlns` Stripping | [`R-XMLTODICT-TARGET-AND-GLOB`](#r-xmltodict-target-and-glob), [`R-XMLTODICT-OUTERMOST-STREAM`](#r-xmltodict-outermost-stream), [`R-XMLTODICT-PARSE`](#r-xmltodict-parse), [`R-XMLTODICT-REPORT`](#r-xmltodict-report) | `default`: `{'books': 2}` |
| `spec` | `xmltodict` | `XmlToDictSettings` | [`02_md5_sidecar_preprocessing`](#case-spec-02-md5-sidecar-preprocessing) | MD5 Sidecar Pre-processing and File-Level `_rejected` Routing | [`R-XMLTODICT-PREPROCESS-MD5`](#r-xmltodict-preprocess-md5), [`R-XMLTODICT-REPORT`](#r-xmltodict-report) | `partial_md5_failure`: `{'books': 1, '_rejected': 1}` |
| `spec` | `xmltodict` | `XmlToDictSettings` | [`03_table_transforms_and_output_schemas`](#case-spec-03-table-transforms-and-output-schemas) | Table Transforms, Unwrapping `<xml_tag>`, Multi-Table Fan-Out, and PyArrow Output Schemas | [`R-XMLTODICT-OUTERMOST-STREAM`](#r-xmltodict-outermost-stream), [`R-XMLTODICT-PARSE`](#r-xmltodict-parse), [`R-XMLTODICT-TRANSFORMS`](#r-xmltodict-transforms), [`R-XMLTODICT-REPORT`](#r-xmltodict-report) | `default`: `{'books': 2, 'book_authors': 3}` |

---

## 3. Base Pipeline Specification Cases (`xmltodict` / `spec/`)

**Shared Fixture Module (`specs/pipelines/xmltodict/transforms.py`)**

```python
"""File pre-processors, table transforms, and PyArrow output schemas for XML to Dict specification cases."""

from typing import Any, Final

import pyarrow as pa

from cdm_data_loaders.core.preprocessors import validate_md5_sidecar
from cdm_data_loaders.core.registry import FilePreprocessFn, RecordTransformFn

FILE_PREPROCESSORS: tuple[FilePreprocessFn, ...] = (validate_md5_sidecar,)

CATALOG_BOOKS_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("book_id", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
        pa.field("year", pa.int64(), nullable=False),
    ]
)

BOOK_AUTHORS_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("book_id", pa.string(), nullable=False),
        pa.field("author", pa.string(), nullable=False),
    ]
)


def transform_book(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Unwrap ``<book>`` dictionaries, drop withdrawn entries, and fan out authors to ``book_authors``.

    :param row: Parsed ``xmltodict`` dictionary of the form ``{"book": {...}}``.
    :type row: dict[str, Any]
    :return: Mapping of target table names to lists of output row dictionaries.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    book = row["book"]
    if book.get("_status") == "withdrawn":
        return {}
    book_id = str(book["_id"]).upper()
    title = str(book["title"])
    year = int(book["year"])
    routed: dict[str, list[dict[str, Any]]] = {
        "books": [
            {
                "book_id": book_id,
                "title": title,
                "year": year,
            }
        ]
    }
    raw_authors = book.get("author") or []
    authors = raw_authors if isinstance(raw_authors, list) else [raw_authors]
    if authors:
        routed["book_authors"] = [{"book_id": book_id, "author": str(a)} for a in authors]
    return routed


TABLE_TRANSFORMS: Final[dict[str, RecordTransformFn]] = {
    "books": transform_book,
}

OUTPUT_SCHEMAS: Final[dict[str, pa.Schema]] = {
    "books": CATALOG_BOOKS_SCHEMA,
    "book_authors": BOOK_AUTHORS_SCHEMA,
}
```

<a id="case-spec-01-outermost-elements-gzip-and-xmlns"></a>

### Outermost `<xml_tag>` Streaming, Self-Nested Tags, Gzip Decompression, and `_xmlns` Stripping (`spec/01_outermost_elements_gzip_and_xmlns`)

**Suite:** `spec` · **Target CLI:** `xmltodict` · **Settings Class:** `XmlToDictSettings`  
**Directory:** `specs/pipelines/xmltodict/spec/01_outermost_elements_gzip_and_xmlns/`  
**Rules:** [`R-XMLTODICT-TARGET-AND-GLOB`](#r-xmltodict-target-and-glob), [`R-XMLTODICT-OUTERMOST-STREAM`](#r-xmltodict-outermost-stream), [`R-XMLTODICT-PARSE`](#r-xmltodict-parse), [`R-XMLTODICT-REPORT`](#r-xmltodict-report)

- `file_glob = "**/*.xml*"` discovers `batch_a/part1.xml` and `batch_b/part2.xml.gz` across nested subdirectories while ignoring `batch_b/ignored.txt`.
- `batch_a/part1.xml` declares `xmlns:cat="http://example.org/catalog"` on `<library>` (stripped from the parsed `<book>` dict via `_xmlns` filtering) and contains a nested `<book id="b-1a">` inside `<related>`, which `stream_xml_file` preserves inside the outer `b-1` element rather than yielding as a separate row.

**Settings (`specs/pipelines/xmltodict/spec/01_outermost_elements_gzip_and_xmlns/settings.toml`)**

```toml
dataset_name = "xmltodict_spec"
table_name = "books"
xml_tag = "book"
file_glob = "**/*.xml*"
loader_file_format = "parquet"
use_output_dir_for_pipeline_metadata = true
```

**Input `batch_a/part1.xml` (`specs/pipelines/xmltodict/spec/01_outermost_elements_gzip_and_xmlns/input/batch_a/part1.xml`)**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<library xmlns:cat="http://example.org/catalog">
  <book id="b-1">
    <title>Dune</title>
    <related>
      <book id="b-1a">
        <title>Dune Messiah</title>
      </book>
    </related>
  </book>
</library>
```

**Input `batch_b/ignored.txt` (`specs/pipelines/xmltodict/spec/01_outermost_elements_gzip_and_xmlns/input/batch_b/ignored.txt`)**

```text
Not an XML file; ignored by file_glob.
```

**Input `batch_b/part2.xml.gz` (`specs/pipelines/xmltodict/spec/01_outermost_elements_gzip_and_xmlns/input/batch_b/part2.xml.gz`) (decompressed preview)**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<library>
  <book id="b-2">
    <title>Foundation</title>
  </book>
</library>
```

**Expected `report.txt` Summary & Row Counts (`{'books': 2}`)**

```yaml
status: completed
ingested_files:
- batch_a/part1.xml
- batch_b/part2.xml.gz
load_errors: []
pipeline_load_info_errs: null
```

**Expected PyArrow Tables (`specs/pipelines/xmltodict/spec/01_outermost_elements_gzip_and_xmlns/expected_tables.py` — `EXPECTED_TABLES`)**

```python
"""Expected PyArrow tables for xmltodict 01_outermost_elements_gzip_and_xmlns."""

import pyarrow as pa

BOOKS_SCHEMA = pa.schema(
    [
        pa.field("book___id", pa.string(), nullable=False),
        pa.field("book__title", pa.string(), nullable=False),
        pa.field("book__related__book___id", pa.string(), nullable=True),
        pa.field("book__related__book__title", pa.string(), nullable=True),
    ]
)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "books": pa.Table.from_pylist(
        [
            {
                "book___id": "b-1",
                "book__title": "Dune",
                "book__related__book___id": "b-1a",
                "book__related__book__title": "Dune Messiah",
            },
            {
                "book___id": "b-2",
                "book__title": "Foundation",
                "book__related__book___id": None,
                "book__related__book__title": None,
            },
        ],
        schema=BOOKS_SCHEMA,
    ),
}
```

**Expected Canonical Table Rows (`specs/pipelines/xmltodict/spec/01_outermost_elements_gzip_and_xmlns/expected.json`)**

```json
{
  "books": [
    {
      "book___id": "b-1",
      "book__related__book___id": "b-1a",
      "book__related__book__title": "Dune Messiah",
      "book__title": "Dune"
    },
    {
      "book___id": "b-2",
      "book__title": "Foundation"
    }
  ]
}
```

---

<a id="case-spec-02-md5-sidecar-preprocessing"></a>

### MD5 Sidecar Pre-processing and File-Level `_rejected` Routing (`spec/02_md5_sidecar_preprocessing`)

**Suite:** `spec` · **Target CLI:** `xmltodict` · **Settings Class:** `XmlToDictSettings`  
**Directory:** `specs/pipelines/xmltodict/spec/02_md5_sidecar_preprocessing/`  
**Rules:** [`R-XMLTODICT-PREPROCESS-MD5`](#r-xmltodict-preprocess-md5), [`R-XMLTODICT-REPORT`](#r-xmltodict-report)

- `preprocessors_module = "specs.pipelines.xmltodict.transforms"` registers `validate_md5_sidecar`.
- `good.xml` matches `good.xml.md5` and streams `<book id="b-10">` into `books`.
- `bad.xml` fails MD5 validation against `bad.xml.md5`; the file is skipped before XML parsing, omitted from `ingested_files`, recorded in `report.txt` `load_errors` (`status: completed with errors`), and emitted as a file-level row (`line_no=None`, `raw_record=None`) in `_rejected`.

**Settings (`specs/pipelines/xmltodict/spec/02_md5_sidecar_preprocessing/settings.toml`)**

```toml
dataset_name = "xmltodict_spec"
table_name = "books"
xml_tag = "book"
file_glob = "*.xml"
preprocessors_module = "specs.pipelines.xmltodict.transforms"
loader_file_format = "parquet"
use_output_dir_for_pipeline_metadata = true
```

**Input `bad.xml` (`specs/pipelines/xmltodict/spec/02_md5_sidecar_preprocessing/input/bad.xml`)**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<library>
  <book id="b-99">
    <title>Corrupted Book</title>
  </book>
</library>
```

**Input `bad.xml.md5` (`specs/pipelines/xmltodict/spec/02_md5_sidecar_preprocessing/input/bad.xml.md5`)**

```text
MD5(bad.xml)= 00000000000000000000000000000000
```

**Input `good.xml` (`specs/pipelines/xmltodict/spec/02_md5_sidecar_preprocessing/input/good.xml`)**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<library>
  <book id="b-10">
    <title>Hyperion</title>
  </book>
</library>
```

**Input `good.xml.md5` (`specs/pipelines/xmltodict/spec/02_md5_sidecar_preprocessing/input/good.xml.md5`)**

```text
MD5(good.xml)= 54f841749a9f9ca70941227f213a23e4
```

**Expected `report.txt` Summary & Row Counts (`{'books': 1, '_rejected': 1}`)**

```yaml
status: completed with errors
ingested_files:
- good.xml
load_errors:
- 'File pre-processing failed for bad.xml: MD5 mismatch for bad.xml'
pipeline_load_info_errs: null
```

**Expected PyArrow Tables (`specs/pipelines/xmltodict/spec/02_md5_sidecar_preprocessing/expected_tables.py` — `EXPECTED_TABLES`)**

```python
"""Expected PyArrow tables for xmltodict 02_md5_sidecar_preprocessing."""

import pyarrow as pa

from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA

BOOKS_SCHEMA = pa.schema(
    [
        pa.field("book___id", pa.string(), nullable=False),
        pa.field("book__title", pa.string(), nullable=False),
    ]
)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "books": pa.Table.from_pylist(
        [
            {"book___id": "b-10", "book__title": "Hyperion"},
        ],
        schema=BOOKS_SCHEMA,
    ),
    "_rejected": pa.Table.from_pylist(
        [
            {
                "table_name": "books",
                "source_file": "bad.xml",
                "line_no": None,
                "raw_record": None,
                "error_detail": (
                    "MD5 mismatch for bad.xml: "
                    "expected 00000000000000000000000000000000, got 5a46030ee1e4775d1dd0418280d8a0d7"
                ),
            }
        ],
        schema=REJECTED_PYARROW_SCHEMA,
    ),
}
```

**Expected Canonical Table Rows (`specs/pipelines/xmltodict/spec/02_md5_sidecar_preprocessing/expected.json`)**

```json
{
  "_rejected": [
    {
      "error_detail": "MD5 mismatch for bad.xml: expected 00000000000000000000000000000000, got 5a46030ee1e4775d1dd0418280d8a0d7",
      "source_file": "bad.xml",
      "table_name": "books"
    }
  ],
  "books": [
    {
      "book___id": "b-10",
      "book__title": "Hyperion"
    }
  ]
}
```

---

<a id="case-spec-03-table-transforms-and-output-schemas"></a>

### Table Transforms, Unwrapping `<xml_tag>`, Multi-Table Fan-Out, and PyArrow Output Schemas (`spec/03_table_transforms_and_output_schemas`)

**Suite:** `spec` · **Target CLI:** `xmltodict` · **Settings Class:** `XmlToDictSettings`  
**Directory:** `specs/pipelines/xmltodict/spec/03_table_transforms_and_output_schemas/`  
**Rules:** [`R-XMLTODICT-OUTERMOST-STREAM`](#r-xmltodict-outermost-stream), [`R-XMLTODICT-PARSE`](#r-xmltodict-parse), [`R-XMLTODICT-TRANSFORMS`](#r-xmltodict-transforms), [`R-XMLTODICT-REPORT`](#r-xmltodict-report)

- `transforms_module = "specs.pipelines.xmltodict.transforms"` and `output_schemas_module = "specs.pipelines.xmltodict.transforms"` register `TABLE_TRANSFORMS["books"]` and `OUTPUT_SCHEMAS` for `books` and `book_authors`.
- `transform_book` unwraps each `{"book": {...}}` dictionary, filters out `<book id="b-drop" status="withdrawn">`, normalizes `book_id` to uppercase, casts `year` to `int`, and fans out single or repeated `<author>` elements to `book_authors`.

**Settings (`specs/pipelines/xmltodict/spec/03_table_transforms_and_output_schemas/settings.toml`)**

```toml
dataset_name = "xmltodict_spec"
table_name = "books"
xml_tag = "book"
file_glob = "*.xml"
transforms_module = "specs.pipelines.xmltodict.transforms"
output_schemas_module = "specs.pipelines.xmltodict.transforms"
loader_file_format = "parquet"
use_output_dir_for_pipeline_metadata = true
```

**Input `catalog.xml` (`specs/pipelines/xmltodict/spec/03_table_transforms_and_output_schemas/input/catalog.xml`)**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<library>
  <book id="b-1" status="active">
    <title>The Left Hand of Darkness</title>
    <year>1969</year>
    <author>Ursula K. Le Guin</author>
  </book>
  <book id="b-drop" status="withdrawn">
    <title>Withdrawn Draft</title>
    <year>1970</year>
    <author>Ignored Author</author>
  </book>
  <book id="b-2" status="active">
    <title>Good Omens</title>
    <year>1990</year>
    <author>Terry Pratchett</author>
    <author>Neil Gaiman</author>
  </book>
</library>
```

**Expected `report.txt` Summary & Row Counts (`{'books': 2, 'book_authors': 3}`)**

```yaml
status: completed
ingested_files:
- catalog.xml
load_errors: []
pipeline_load_info_errs: null
```

**Expected PyArrow Tables (`specs/pipelines/xmltodict/spec/03_table_transforms_and_output_schemas/expected_tables.py` — `EXPECTED_TABLES`)**

```python
"""Expected PyArrow tables for xmltodict 03_table_transforms_and_output_schemas."""

import pyarrow as pa

from specs.pipelines.xmltodict.transforms import (
    BOOK_AUTHORS_SCHEMA,
    CATALOG_BOOKS_SCHEMA,
)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "books": pa.Table.from_pylist(
        [
            {"book_id": "B-1", "title": "The Left Hand of Darkness", "year": 1969},
            {"book_id": "B-2", "title": "Good Omens", "year": 1990},
        ],
        schema=CATALOG_BOOKS_SCHEMA,
    ),
    "book_authors": pa.Table.from_pylist(
        [
            {"book_id": "B-1", "author": "Ursula K. Le Guin"},
            {"book_id": "B-2", "author": "Terry Pratchett"},
            {"book_id": "B-2", "author": "Neil Gaiman"},
        ],
        schema=BOOK_AUTHORS_SCHEMA,
    ),
}
```

**Expected Canonical Table Rows (`specs/pipelines/xmltodict/spec/03_table_transforms_and_output_schemas/expected.json`)**

```json
{
  "book_authors": [
    {
      "author": "Ursula K. Le Guin",
      "book_id": "B-1"
    },
    {
      "author": "Neil Gaiman",
      "book_id": "B-2"
    },
    {
      "author": "Terry Pratchett",
      "book_id": "B-2"
    }
  ],
  "books": [
    {
      "book_id": "B-1",
      "title": "The Left Hand of Darkness",
      "year": 1969
    },
    {
      "book_id": "B-2",
      "title": "Good Omens",
      "year": 1990
    }
  ]
}
```

---

## 4. Fixture Index

Every case directory contains a `case.yaml` manifest; the test suites and this document are both driven by it.

| Case | Target CLI | Manifest | Runs |
| :--- | :--- | :--- | :--- |
| [`spec/01_outermost_elements_gzip_and_xmlns`](#case-spec-01-outermost-elements-gzip-and-xmlns) | `xmltodict` | [`case.yaml`](https://github.com/kbase/cdm-data-loaders/blob/main/specs/pipelines/xmltodict/spec/01_outermost_elements_gzip_and_xmlns/case.yaml) | default |
| [`spec/02_md5_sidecar_preprocessing`](#case-spec-02-md5-sidecar-preprocessing) | `xmltodict` | [`case.yaml`](https://github.com/kbase/cdm-data-loaders/blob/main/specs/pipelines/xmltodict/spec/02_md5_sidecar_preprocessing/case.yaml) | partial_md5_failure |
| [`spec/03_table_transforms_and_output_schemas`](#case-spec-03-table-transforms-and-output-schemas) | `xmltodict` | [`case.yaml`](https://github.com/kbase/cdm-data-loaders/blob/main/specs/pipelines/xmltodict/spec/03_table_transforms_and_output_schemas/case.yaml) | default |
