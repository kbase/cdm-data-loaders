<!-- GENERATED FILE - DO NOT EDIT. Source: `tests/data/xsd/rules.yaml`, `tests/data/xsd/spec`, `tests/data/xsd/examples`. Regenerate with `uv run python -m scripts.gen_converter_spec`. -->

# XSD to PyArrow Specification

This document specifies how an XML Schema (`.xsd`) and a target `xml_tag` determine:

1. The **PyArrow `pa.Schema`** of the table yielded by the pipeline (`generate_pyarrow_schema`).
2. The **Python / JSON row dictionaries** produced by `convert_xml_element` before
   `pa.Table.from_pylist(rows, schema=pa_schema)`.
3. The **errors** raised when an XSD or `xml_tag` uses unsupported or ambiguous constructs.

**Contents:** [1. Conversion Rules](#1-conversion-rules) · [2. Cases](#2-cases) · [3. Errors](#3-errors-unsupported--invalid-input) · [4. Fixture Index](#4-fixture-index)

## 1. Conversion Rules

### 1.1 Top-Level `xml_tag` Resolution and Unwrapping

<a id="r-resolve-depth"></a>

- **R-RESOLVE-DEPTH**: `xml_tag` is searched for among global elements (depth 0), then breadth-first through the content
  models of their types (depth 1, 2, ...). Matches at the shallowest depth are deduplicated by type
  identity; one type resolves, zero matches raise **E1**, and more than one distinct type raises **E2**.

  *Cases:* [07_multi_root_shared_type](#case-spec-07-multi-root-shared-type), [uniref](#case-examples-uniref)

<a id="r-resolve-clark"></a>

- **R-RESOLVE-CLARK**: `xml_tag` may be a local name (`entry`) or Clark notation (`{http://example.org/ns}entry`); both
  resolve to the same declaration.

  *Cases:* [02_namespaces_and_nillable](#case-spec-02-namespaces-and-nillable)

<a id="r-unwrap"></a>

- **R-UNWRAP**: Each `<xml_tag>` element becomes one row. Its type must map to a `pa.struct` (see **E3**), and the
  struct's fields become the top-level columns of the `pa.Schema`.

  *Cases:* [01_scalars_and_simple_types](#case-spec-01-scalars-and-simple-types)

### 1.2 Naming and Field Ordering

<a id="r-name-attr"></a>

- **R-NAME-ATTR**: Attributes become fields prefixed with `_` (`id` -> `_id`). Namespace declarations (`xmlns`,
  `xmlns:*`) and `xsi:*` attributes are stripped.

  *Cases:* [01_scalars_and_simple_types](#case-spec-01-scalars-and-simple-types), [02_namespaces_and_nillable](#case-spec-02-namespaces-and-nillable)

<a id="r-name-text"></a>

- **R-NAME-TEXT**: When a type has attributes and text (`simpleContent` with attributes, or `mixed="true"` with
  attributes), the text is stored in a field named `#text`.

  *Cases:* [02_namespaces_and_nillable](#case-spec-02-namespaces-and-nillable), [03_mixed_content_raw_text](#case-spec-03-mixed-content-raw-text), [10_combinations](#case-spec-10-combinations)

<a id="r-order"></a>

- **R-ORDER**: Struct fields are ordered: declared attributes in declaration order (base type before extension),
  then `#text`, then child elements in content-model traversal order. A child name appearing in
  several branches or positions is emitted once, at its first appearance.

  *Cases:* [01_scalars_and_simple_types](#case-spec-01-scalars-and-simple-types), [05_groups_and_compositors](#case-spec-05-groups-and-compositors), [10_combinations](#case-spec-10-combinations)

### 1.3 Element Classification (Shape)

| XSD element type | Single (`overall_max_occurs == 1`) | Repeated (`> 1` or `unbounded`) |
| :--- | :--- | :--- |
| atomic `simpleType` | scalar | `pa.list_(scalar)` |
| `xs:list` `simpleType` | `pa.list_(item)` | `pa.list_(pa.list_(item))` |
| `simpleContent`, no attributes | as underlying `simpleType` | as underlying `simpleType`, listed |
| `simpleContent` with attributes | `pa.struct([_attr..., #text])` | `pa.list_(pa.struct(...))` |
| `mixed="true"`, no attributes | `pa.string()` (raw inner XML) | `pa.list_(pa.string())` |
| `mixed="true"` with attributes | `pa.struct([_attr..., #text: string])` | `pa.list_(pa.struct(...))` |
| attribute-only / element-only `complexType` | `pa.struct([...])` | `pa.list_(pa.struct(...))` |
| empty `complexType` (no attrs, no children, not mixed) | `pa.string()` | `pa.list_(pa.string())` |

<a id="r-shape-cardinality"></a>

- **R-SHAPE-CARDINALITY**: A child's `overall_max_occurs` is computed across the whole content model: element `maxOccurs`
  multiplied through enclosing compositors, summed over `xs:sequence` / `xs:all` positions and
  maximised over `xs:choice` branches. A value greater than 1 (including a child of a repeated
  compositor that itself has `maxOccurs="1"`) makes the field a `pa.list_`. `maxOccurs="0"` children
  are omitted.

  *Cases:* [04_recursion_depth](#case-spec-04-recursion-depth), [05_groups_and_compositors](#case-spec-05-groups-and-compositors), [09_empty_marker_elements](#case-spec-09-empty-marker-elements), [10_combinations](#case-spec-10-combinations)

<a id="r-shape-groups"></a>

- **R-SHAPE-GROUPS**: `xs:group` and `xs:attributeGroup` references are expanded in place; `xs:sequence`, `xs:choice`
  and `xs:all` contribute their elements to the enclosing struct.

  *Cases:* [03_mixed_content_raw_text](#case-spec-03-mixed-content-raw-text), [05_groups_and_compositors](#case-spec-05-groups-and-compositors)

<a id="r-shape-extension"></a>

- **R-SHAPE-EXTENSION**: `complexContent` / `xs:extension` merges base-type attributes and children with the extension's,
  base first.

  *Cases:* [10_combinations](#case-spec-10-combinations)

<a id="r-shape-xslist"></a>

- **R-SHAPE-XSLIST**: `xs:list` simple types, and the built-in list types `xs:NMTOKENS`, `xs:IDREFS`, `xs:ENTITIES`, map to
  `pa.list_(item)` and are coerced by splitting on whitespace and coercing each token.

  *Cases:* [01_scalars_and_simple_types](#case-spec-01-scalars-and-simple-types), [uniprot](#case-examples-uniprot)

<a id="r-shape-simplecontent"></a>

- **R-SHAPE-SIMPLECONTENT**: `simpleContent` without attributes maps exactly like its base `simpleType`. With attributes it is a
  struct of the attribute fields followed by `#text`.

  *Cases:* [02_namespaces_and_nillable](#case-spec-02-namespaces-and-nillable), [06_context_dependent_paths](#case-spec-06-context-dependent-paths), [10_combinations](#case-spec-10-combinations)

<a id="r-shape-mixed"></a>

- **R-SHAPE-MIXED**: `mixed="true"` content is captured as the raw inner XML string; declared child elements are not
  emitted as fields. Inline tags (`<b>`, `<i>`, ...) are kept, text entities stay escaped (`&amp;`,
  `&lt;`, `&gt;`), and namespace declarations are dropped from inline tags.

  *Cases:* [03_mixed_content_raw_text](#case-spec-03-mixed-content-raw-text), [10_combinations](#case-spec-10-combinations), [pubmed_article](#case-examples-pubmed-article), [pubmed_book_article](#case-examples-pubmed-book-article)

<a id="r-shape-attr-only"></a>

- **R-SHAPE-ATTR-ONLY**: A `complexType` with attributes but no content maps to a struct of its attribute fields.

  *Cases:* [10_combinations](#case-spec-10-combinations)

<a id="r-shape-empty-marker"></a>

- **R-SHAPE-EMPTY-MARKER**: A `complexType` with no attributes, no children and no mixed content is an empty marker: it maps
  to `pa.string()`, with value `""` when present and `None` when absent.

  *Cases:* [09_empty_marker_elements](#case-spec-09-empty-marker-elements), [10_combinations](#case-spec-10-combinations), [pubmed_article](#case-examples-pubmed-article), [pubmed_book_article](#case-examples-pubmed-book-article)

<a id="r-shape-context-path"></a>

- **R-SHAPE-CONTEXT-PATH**: Types are resolved per ancestor path from `xml_tag`, not per `(parent, child)` name pair, so the
  same child name under differently-typed parents gets each parent's exact type.

  *Cases:* [06_context_dependent_paths](#case-spec-06-context-dependent-paths), [10_combinations](#case-spec-10-combinations)

<a id="r-recursion"></a>

- **R-RECURSION**: Each complex type may be expanded at most `max_recursion_depth` times (default `2`) along a path.
  A deeper occurrence becomes `pa.string()` (or `pa.list_(pa.string())` when repeated) and its
  subtree is serialized with `json.dumps`. Applies to type self-recursion, element-`ref`
  self-recursion and mutual recursion. Truncated `pa.string()` fields carry no `xsd_type`.

  *Cases:* [04_recursion_depth](#case-spec-04-recursion-depth), [10_combinations](#case-spec-10-combinations), [pubmed_article](#case-examples-pubmed-article), [pubmed_book_article](#case-examples-pubmed-book-article)

### 1.4 Field Nullability and Empty Tag Coercion

<a id="r-null-attr"></a>

- **R-NULL-ATTR**: Attribute fields are `nullable=False` when `use="required"` and the enclosing element is not
  `nillable="true"`; otherwise `nullable=True`.

  *Cases:* [01_scalars_and_simple_types](#case-spec-01-scalars-and-simple-types), [08_nullability_and_empty_tags](#case-spec-08-nullability-and-empty-tags)

<a id="r-null-child"></a>

- **R-NULL-CHILD**: A child field is `nullable=False` only when its overall `minOccurs` (multiplied through all
  enclosing compositors) is at least 1, it is not `nillable="true"`, and its parent element is not
  `nillable="true"`. Otherwise it is `nullable=True`.

  *Cases:* [05_groups_and_compositors](#case-spec-05-groups-and-compositors), [06_context_dependent_paths](#case-spec-06-context-dependent-paths), [08_nullability_and_empty_tags](#case-spec-08-nullability-and-empty-tags)

<a id="r-null-choice"></a>

- **R-NULL-CHOICE**: Elements that appear in only some branches of an `xs:choice` have overall `minOccurs == 0` and are
  therefore `nullable=True`.

  *Cases:* [05_groups_and_compositors](#case-spec-05-groups-and-compositors), [08_nullability_and_empty_tags](#case-spec-08-nullability-and-empty-tags), [pubmed_article](#case-examples-pubmed-article)

<a id="r-null-nillable-parent"></a>

- **R-NULL-NILLABLE-PARENT**: All attribute and child fields inside a `nillable="true"` complex element are `nullable=True`, since
  `<comment type="x" xsi:nil="true"/>` produces `{"_type": "x"}` with no children.

  *Cases:* [02_namespaces_and_nillable](#case-spec-02-namespaces-and-nillable), [08_nullability_and_empty_tags](#case-spec-08-nullability-and-empty-tags), [uniprot](#case-examples-uniprot)

<a id="r-null-text"></a>

- **R-NULL-TEXT**: `#text` fields are always `nullable=True` (`<tag attr="x"/>` has no text), and list `item` fields
  are always `nullable=True`.

  *Cases:* [02_namespaces_and_nillable](#case-spec-02-namespaces-and-nillable), [03_mixed_content_raw_text](#case-spec-03-mixed-content-raw-text)

<a id="r-nil"></a>

- **R-NIL**: `xsi:nil="true"` yields `None` for an element with no attributes, and a struct holding only its
  attributes otherwise.

  *Cases:* [02_namespaces_and_nillable](#case-spec-02-namespaces-and-nillable), [08_nullability_and_empty_tags](#case-spec-08-nullability-and-empty-tags), [uniprot](#case-examples-uniprot)

<a id="r-empty-coerce"></a>

- **R-EMPTY-COERCE**: A present empty element (`<tag/>`, no `xsi:nil`) that maps to a non-nullable `pa.string()` field is
  coerced to `""`, distinguishing it from an absent optional element (`None`). Empty markers are
  always `""` when present.

  *Cases:* [03_mixed_content_raw_text](#case-spec-03-mixed-content-raw-text), [08_nullability_and_empty_tags](#case-spec-08-nullability-and-empty-tags), [09_empty_marker_elements](#case-spec-09-empty-marker-elements)

### 1.5 Scalar Type Mapping and Field Metadata

<a id="r-scalar-builtin"></a>

- **R-SCALAR-BUILTIN**: Built-in XSD types map to Arrow types per the table below. Unlisted built-ins fall back to
  `pa.string()`. `xs:boolean` accepts `true`/`1` and `false`/`0`; `xs:date` is coerced with
  `datetime.date.fromisoformat`; `xs:hexBinary` with `bytes.fromhex`; `xs:base64Binary` with
  `base64.b64decode`.

  *Cases:* [01_scalars_and_simple_types](#case-spec-01-scalars-and-simple-types)

<a id="r-scalar-restriction"></a>

- **R-SCALAR-RESTRICTION**: `xs:restriction` types (including enumerations) inherit the mapping of their built-in base type.

  *Cases:* [01_scalars_and_simple_types](#case-spec-01-scalars-and-simple-types)

<a id="r-scalar-union"></a>

- **R-SCALAR-UNION**: An `xs:union` uses the shared Arrow type when every member maps to the same type; otherwise it
  falls back to `pa.string()`. Its `xsd_type` joins the member names with `" | "`.

  *Cases:* [01_scalars_and_simple_types](#case-spec-01-scalars-and-simple-types), [uniprot](#case-examples-uniprot)

<a id="r-meta-xsd-type"></a>

- **R-META-XSD-TYPE**: Every leaf scalar field (including `#text` and scalar list `item` fields) carries
  `metadata["xsd_type"]` with the built-in `xs:` name. Struct, outer list, empty-marker and
  recursion-truncated fields do not.

  *Cases:* [01_scalars_and_simple_types](#case-spec-01-scalars-and-simple-types)

<a id="r-meta-comment"></a>

- **R-META-COMMENT**: `<xs:annotation><xs:documentation>` on an element or attribute (or, failing that, on its `ref`
  target or type) is whitespace-normalized and stored in `metadata["comment"]`.

  *Cases:* [01_scalars_and_simple_types](#case-spec-01-scalars-and-simple-types), [uniprot](#case-examples-uniprot), [uniref](#case-examples-uniref)

| XSD built-in type(s) | Python type (coerced) | PyArrow `DataType` |
| :--- | :--- | :--- |
| `xs:string`, `xs:normalizedString`, `xs:token`, `xs:language`, `xs:Name`, `xs:NCName`, `xs:NMTOKEN`, `xs:ID`, `xs:IDREF`, `xs:ENTITY`, `xs:anyURI`, `xs:QName`, `xs:NOTATION`, `xs:dateTime`, `xs:time`, `xs:duration`, `xs:gYear`, `xs:gYearMonth`, `xs:gMonthDay`, `xs:gDay`, `xs:gMonth` | `str` | `pa.string()` |
| `xs:boolean` | `bool` | `pa.bool_()` |
| `xs:byte` | `int` | `pa.int8()` |
| `xs:short` | `int` | `pa.int16()` |
| `xs:int` | `int` | `pa.int32()` |
| `xs:long`, `xs:integer`, `xs:positiveInteger`, `xs:nonNegativeInteger`, `xs:negativeInteger`, `xs:nonPositiveInteger` | `int` | `pa.int64()` |
| `xs:unsignedByte` | `int` | `pa.uint8()` |
| `xs:unsignedShort` | `int` | `pa.uint16()` |
| `xs:unsignedInt` | `int` | `pa.uint32()` |
| `xs:unsignedLong` | `int` | `pa.uint64()` |
| `xs:float`, `xs:double`, `xs:decimal` | `float` | `pa.float64()` |
| `xs:date` | `datetime.date` | `pa.date32()` |
| `xs:base64Binary`, `xs:hexBinary` | `bytes` | `pa.binary()` |
| `xs:NMTOKENS` (items `xs:NMTOKEN`), `xs:IDREFS` (items `xs:IDREF`), `xs:ENTITIES` (items `xs:ENTITY`) | `list[str]` | `pa.list_(pa.string())` |

## 2. Cases

<a id="case-spec-01-scalars-and-simple-types"></a>

### Scalar Types, Restrictions, Lists and Unions (`spec/01_scalars_and_simple_types`)

**Rules:** [R-UNWRAP](#r-unwrap), [R-NAME-ATTR](#r-name-attr), [R-ORDER](#r-order), [R-SHAPE-XSLIST](#r-shape-xslist), [R-SCALAR-BUILTIN](#r-scalar-builtin), [R-SCALAR-RESTRICTION](#r-scalar-restriction), [R-SCALAR-UNION](#r-scalar-union), [R-META-XSD-TYPE](#r-meta-xsd-type), [R-META-COMMENT](#r-meta-comment), [R-NULL-ATTR](#r-null-attr)

**Target:** `xml_tag='record'`

- `code` is `xs:int | xs:short`: members map to different Arrow types (`int32` vs `int16`), so it falls back to `pa.string()`.
- `published` is `xs:date | xs:gYearMonth | xs:gYear` and also resolves to `pa.string()`.
- `scores` and `_evidence` use the `xs:list` type `intListType` and become `pa.list_(pa.int32())`.

#### Schema (`schema.xsd`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:simpleType name="intListType">
        <xs:list itemType="xs:int"/>
    </xs:simpleType>

    <xs:simpleType name="flexibleDateType">
        <xs:union memberTypes="xs:date xs:gYearMonth xs:gYear"/>
    </xs:simpleType>

    <xs:simpleType name="int64UnionType">
        <xs:union memberTypes="xs:long xs:integer"/>
    </xs:simpleType>

    <xs:simpleType name="statusEnum">
        <xs:annotation>
            <xs:documentation>Record lifecycle status.</xs:documentation>
        </xs:annotation>
        <xs:restriction base="xs:string">
            <xs:enumeration value="active"/>
            <xs:enumeration value="archived"/>
        </xs:restriction>
    </xs:simpleType>

    <xs:element name="dataset">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="record" maxOccurs="unbounded">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="flag" type="xs:boolean">
                                <xs:annotation>
                                    <xs:documentation>Primary boolean flag.</xs:documentation>
                                </xs:annotation>
                            </xs:element>
                            <xs:element name="smallInt" type="xs:byte" minOccurs="0"/>
                            <xs:element name="medInt" type="xs:short" minOccurs="0"/>
                            <xs:element name="stdInt" type="xs:int" minOccurs="0"/>
                            <xs:element name="bigInt" type="xs:integer" minOccurs="0"/>
                            <xs:element name="uByte" type="xs:unsignedByte" minOccurs="0"/>
                            <xs:element name="uShort" type="xs:unsignedShort" minOccurs="0"/>
                            <xs:element name="uInt" type="xs:unsignedInt" minOccurs="0"/>
                            <xs:element name="uLong" type="xs:unsignedLong" minOccurs="0"/>
                            <xs:element name="f32" type="xs:float" minOccurs="0"/>
                            <xs:element name="f64" type="xs:double" minOccurs="0"/>
                            <xs:element name="dec" type="xs:decimal" minOccurs="0"/>
                            <xs:element name="created" type="xs:date" minOccurs="0"/>
                            <xs:element name="updatedAt" type="xs:dateTime" minOccurs="0"/>
                            <xs:element name="published" type="flexibleDateType" minOccurs="0"/>
                            <xs:element name="homogeneousUnion" type="int64UnionType" minOccurs="0"/>
                            <xs:element name="scores" type="intListType" minOccurs="0"/>
                            <xs:element name="tokens" type="xs:NMTOKENS" minOccurs="0"/>
                            <xs:element name="rawHex" type="xs:hexBinary" minOccurs="0"/>
                            <xs:element name="rawB64" type="xs:base64Binary" minOccurs="0"/>
                        </xs:sequence>
                        <xs:attribute name="id" type="xs:ID" use="required">
                            <xs:annotation>
                                <xs:documentation>Unique record identifier.</xs:documentation>
                            </xs:annotation>
                        </xs:attribute>
                        <xs:attribute name="status" type="statusEnum" use="required"/>
                        <xs:attribute name="evidence" type="intListType" use="optional"/>
                        <xs:attribute name="version" type="xs:int" use="required"/>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
```

#### Input (`data.xml`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<dataset>
    <record id="rec1" status="active" evidence="1 4 9" version="2">
        <flag>true</flag>
        <smallInt>-5</smallInt>
        <medInt>1024</medInt>
        <stdInt>100000</stdInt>
        <bigInt>999999999999</bigInt>
        <uByte>255</uByte>
        <uShort>65535</uShort>
        <uInt>4000000000</uInt>
        <uLong>18446744073709551615</uLong>
        <f32>1.5</f32>
        <f64>2.718281828</f64>
        <dec>3.14</dec>
        <created>2025-06-15</created>
        <updatedAt>2025-06-15T12:30:00Z</updatedAt>
        <published>2025-06</published>
        <homogeneousUnion>42000000000</homogeneousUnion>
        <scores>10 20 30</scores>
        <tokens>alpha beta gamma</tokens>
        <rawHex>48656c6c6f</rawHex>
        <rawB64>SGVsbG8=</rawB64>
    </record>
    <record id="rec2" status="archived" version="1">
        <flag>0</flag>
    </record>
    <record id="rec3" status="active" evidence="  10   20  " version="-42">
        <flag>1</flag>
        <smallInt>-128</smallInt>
        <medInt>-32768</medInt>
        <stdInt>-2147483648</stdInt>
        <bigInt>-999999999999</bigInt>
        <uByte>0</uByte>
        <uShort>0</uShort>
        <uInt>0</uInt>
        <uLong>0</uLong>
        <f32>-0.5</f32>
        <f64>1.25e-4</f64>
        <dec>42</dec>
        <created>2025-06-15Z</created>
        <updatedAt>2025-06-15T12:30:00.123+02:00</updatedAt>
        <published>1999</published>
        <homogeneousUnion>-42</homogeneousUnion>
        <scores>  100   200  </scores>
        <tokens>  one   two  </tokens>
        <rawHex>48656C6C6F</rawHex>
        <rawB64>SGVsbG8=</rawB64>
    </record>
</dataset>
```

#### Converted rows: run `default`

```json
[
    {
        "_id": "rec1",
        "_status": "active",
        "_evidence": [
            1,
            4,
            9
        ],
        "_version": 2,
        "flag": true,
        "smallInt": -5,
        "medInt": 1024,
        "stdInt": 100000,
        "bigInt": 999999999999,
        "uByte": 255,
        "uShort": 65535,
        "uInt": 4000000000,
        "uLong": 18446744073709551615,
        "f32": 1.5,
        "f64": 2.718281828,
        "dec": 3.14,
        "created": "2025-06-15",
        "updatedAt": "2025-06-15T12:30:00Z",
        "published": "2025-06",
        "homogeneousUnion": 42000000000,
        "scores": [
            10,
            20,
            30
        ],
        "tokens": [
            "alpha",
            "beta",
            "gamma"
        ],
        "rawHex": "Hello",
        "rawB64": "Hello"
    },
    {
        "_id": "rec2",
        "_status": "archived",
        "_version": 1,
        "flag": false
    },
    {
        "_id": "rec3",
        "_status": "active",
        "_evidence": [
            10,
            20
        ],
        "_version": -42,
        "flag": true,
        "smallInt": -128,
        "medInt": -32768,
        "stdInt": -2147483648,
        "bigInt": -999999999999,
        "uByte": 0,
        "uShort": 0,
        "uInt": 0,
        "uLong": 0,
        "f32": -0.5,
        "f64": 0.000125,
        "dec": 42.0,
        "created": "2025-06-15",
        "updatedAt": "2025-06-15T12:30:00.123+02:00",
        "published": "1999",
        "homogeneousUnion": -42,
        "scores": [
            100,
            200
        ],
        "tokens": [
            "one",
            "two"
        ],
        "rawHex": "Hello",
        "rawB64": "Hello"
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/xsd/spec/01_scalars_and_simple_types/expected_schema.py))

```python
"""Expected PyArrow schema for 01_scalars_and_simple_types (xml_tag='record')."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "_id", pa.string(), nullable=False, metadata={"comment": "Unique record identifier.", "xsd_type": "xs:ID"}
        ),
        pa.field(
            "_status",
            pa.string(),
            nullable=False,
            metadata={"comment": "Record lifecycle status.", "xsd_type": "xs:string"},
        ),
        pa.field(
            "_evidence",
            pa.list_(
                pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"}),
            ),
            nullable=True,
        ),
        pa.field("_version", pa.int32(), nullable=False, metadata={"xsd_type": "xs:int"}),
        pa.field(
            "flag", pa.bool_(), nullable=False, metadata={"comment": "Primary boolean flag.", "xsd_type": "xs:boolean"}
        ),
        pa.field("smallInt", pa.int8(), nullable=True, metadata={"xsd_type": "xs:byte"}),
        pa.field("medInt", pa.int16(), nullable=True, metadata={"xsd_type": "xs:short"}),
        pa.field("stdInt", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"}),
        pa.field("bigInt", pa.int64(), nullable=True, metadata={"xsd_type": "xs:integer"}),
        pa.field("uByte", pa.uint8(), nullable=True, metadata={"xsd_type": "xs:unsignedByte"}),
        pa.field("uShort", pa.uint16(), nullable=True, metadata={"xsd_type": "xs:unsignedShort"}),
        pa.field("uInt", pa.uint32(), nullable=True, metadata={"xsd_type": "xs:unsignedInt"}),
        pa.field("uLong", pa.uint64(), nullable=True, metadata={"xsd_type": "xs:unsignedLong"}),
        pa.field("f32", pa.float64(), nullable=True, metadata={"xsd_type": "xs:float"}),
        pa.field("f64", pa.float64(), nullable=True, metadata={"xsd_type": "xs:double"}),
        pa.field("dec", pa.float64(), nullable=True, metadata={"xsd_type": "xs:decimal"}),
        pa.field("created", pa.date32(), nullable=True, metadata={"xsd_type": "xs:date"}),
        pa.field("updatedAt", pa.string(), nullable=True, metadata={"xsd_type": "xs:dateTime"}),
        pa.field("published", pa.string(), nullable=True, metadata={"xsd_type": "xs:date | xs:gYearMonth | xs:gYear"}),
        pa.field("homogeneousUnion", pa.int64(), nullable=True, metadata={"xsd_type": "xs:long | xs:integer"}),
        pa.field(
            "scores",
            pa.list_(
                pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"}),
            ),
            nullable=True,
        ),
        pa.field(
            "tokens",
            pa.list_(
                pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:NMTOKEN"}),
            ),
            nullable=True,
        ),
        pa.field("rawHex", pa.binary(), nullable=True, metadata={"xsd_type": "xs:hexBinary"}),
        pa.field("rawB64", pa.binary(), nullable=True, metadata={"xsd_type": "xs:base64Binary"}),
    ]
)
```

---

<a id="case-spec-02-namespaces-and-nillable"></a>

### Namespaces and `nillable="true"` (`spec/02_namespaces_and_nillable`)

**Rules:** [R-RESOLVE-CLARK](#r-resolve-clark), [R-NAME-ATTR](#r-name-attr), [R-NAME-TEXT](#r-name-text), [R-SHAPE-SIMPLECONTENT](#r-shape-simplecontent), [R-NIL](#r-nil), [R-NULL-NILLABLE-PARENT](#r-null-nillable-parent), [R-NULL-TEXT](#r-null-text)

**Target:** `xml_tag='entry'`

Covers `targetNamespace` with `elementFormDefault="qualified"` and the three `xsi:nil="true"` cases:

1. Nillable element with no attributes (`<title xsi:nil="true"/>`) -> `None`.
2. Nillable complex element with attributes (`<comment type="function" xsi:nil="true"/>`) -> `{"_type": "function"}`.
3. Nillable `simpleContent` element with attributes (`<label lang="en" xsi:nil="true"/>`) -> `{"_lang": "en"}`.

#### Schema (`schema.xsd`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           targetNamespace="http://example.org/ns"
           xmlns="http://example.org/ns"
           elementFormDefault="qualified">
    <xs:complexType name="commentType">
        <xs:sequence>
            <xs:element name="text" type="xs:string"/>
        </xs:sequence>
        <xs:attribute name="type" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="labelType">
        <xs:simpleContent>
            <xs:extension base="xs:string">
                <xs:attribute name="lang" type="xs:string"/>
            </xs:extension>
        </xs:simpleContent>
    </xs:complexType>

    <xs:element name="root">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="entry" maxOccurs="unbounded">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="title" type="xs:string" nillable="true"/>
                            <xs:element name="label" type="labelType" nillable="true" minOccurs="0"/>
                            <xs:element name="comment" type="commentType" nillable="true" minOccurs="0" maxOccurs="unbounded"/>
                        </xs:sequence>
                        <xs:attribute name="id" type="xs:string" use="required"/>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
```

#### Input (`data.xml`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<root xmlns="http://example.org/ns" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <entry id="e1">
        <title xsi:nil="true"/>
        <label lang="en" xsi:nil="true"/>
        <comment type="function" xsi:nil="true"/>
        <comment type="subunit">
            <text>Homodimer</text>
        </comment>
    </entry>
    <entry id="e2">
        <title>Normal Title</title>
        <label xsi:nil="true"/>
    </entry>
</root>
```

#### Converted rows: run `default`

```json
[
    {
        "_id": "e1",
        "label": {
            "_lang": "en"
        },
        "comment": [
            {
                "_type": "function"
            },
            {
                "_type": "subunit",
                "text": "Homodimer"
            }
        ]
    },
    {
        "_id": "e2",
        "title": "Normal Title"
    }
]
```

#### Converted rows: run `clark-notation` (`xml_tag='{http://example.org/ns}entry'`)

```json
[
    {
        "_id": "e1",
        "label": {
            "_lang": "en"
        },
        "comment": [
            {
                "_type": "function"
            },
            {
                "_type": "subunit",
                "text": "Homodimer"
            }
        ]
    },
    {
        "_id": "e2",
        "title": "Normal Title"
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/xsd/spec/02_namespaces_and_nillable/expected_schema.py))

```python
"""Expected PyArrow schema for 02_namespaces_and_nillable (xml_tag='entry')."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("_id", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("title", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "label",
            pa.struct(
                [
                    pa.field("_lang", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                    pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "comment",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field("_type", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                            pa.field("text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                        ]
                    ),
                    nullable=True,
                ),
            ),
            nullable=True,
        ),
    ]
)
```

---

<a id="case-spec-03-mixed-content-raw-text"></a>

### Mixed Content Kept as Raw Text (`spec/03_mixed_content_raw_text`)

**Rules:** [R-SHAPE-MIXED](#r-shape-mixed), [R-NAME-TEXT](#r-name-text), [R-SHAPE-GROUPS](#r-shape-groups), [R-EMPTY-COERCE](#r-empty-coerce), [R-NULL-TEXT](#r-null-text)

**Target:** `xml_tag='article'`

- `title` is `mixed="true"` without attributes -> `pa.string()`; the empty `<title/>` coerces to `""`.
- `abstractText` is `mixed="true"` with a `Label` attribute -> `pa.struct([_Label, #text])`.
- `&amp;` in text remains escaped so literal `<`/`>` are distinguishable from inline tags.

#### Schema (`schema.xsd`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:group name="inlineFormatting">
        <xs:choice>
            <xs:element name="b" type="formattedText"/>
            <xs:element name="i" type="formattedText"/>
            <xs:element name="sup" type="formattedText"/>
            <xs:element name="sub" type="formattedText"/>
        </xs:choice>
    </xs:group>

    <xs:complexType name="formattedText" mixed="true">
        <xs:group ref="inlineFormatting" minOccurs="0" maxOccurs="unbounded"/>
    </xs:complexType>

    <xs:complexType name="abstractTextType" mixed="true">
        <xs:group ref="inlineFormatting" minOccurs="0" maxOccurs="unbounded"/>
        <xs:attribute name="Label" type="xs:string"/>
    </xs:complexType>

    <xs:element name="articles">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="article" maxOccurs="unbounded">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="title" type="formattedText"/>
                            <xs:element name="abstractText" type="abstractTextType" minOccurs="0" maxOccurs="unbounded"/>
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
```

#### Input (`data.xml`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<articles>
    <article>
        <title>Ca<sup>2+</sup> signaling in <i>E. <b>coli</b></i> cells</title>
        <abstractText Label="BACKGROUND">Studied <b>bold</b> &amp; <i>italic</i> text where x &lt; y.</abstractText>
        <abstractText>Unlabelled paragraph with <sub>2</sub>O and empty <sup/> tag.</abstractText>
        <abstractText Label="EMPTY"/>
        <abstractText/>
    </article>
    <article>
        <title/>
    </article>
</articles>
```

#### Converted rows: run `default`

```json
[
    {
        "title": "Ca<sup>2+</sup> signaling in <i>E. <b>coli</b></i> cells",
        "abstractText": [
            {
                "_Label": "BACKGROUND",
                "#text": "Studied <b>bold</b> &amp; <i>italic</i> text where x &lt; y."
            },
            {
                "#text": "Unlabelled paragraph with <sub>2</sub>O and empty <sup/> tag."
            },
            {
                "_Label": "EMPTY"
            },
            null
        ]
    },
    {
        "title": ""
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/xsd/spec/03_mixed_content_raw_text/expected_schema.py))

```python
"""Expected PyArrow schema for 03_mixed_content_raw_text (xml_tag='article')."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("title", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "abstractText",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field("_Label", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                            pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                        ]
                    ),
                    nullable=True,
                ),
            ),
            nullable=True,
        ),
    ]
)
```

---

<a id="case-spec-04-recursion-depth"></a>

### Structural Recursion with Configurable `max_recursion_depth` (`spec/04_recursion_depth`)

**Rules:** [R-RECURSION](#r-recursion), [R-SHAPE-CARDINALITY](#r-shape-cardinality)

**Target:** `xml_tag='record'`

With `max_recursion_depth=2`, `tree` (depth 1) and its `branch` children (depth 2) are structs; grandchild
`branch` elements are serialized to JSON strings. With `max_recursion_depth=1`, `branch` is already truncated.

#### Schema (`schema.xsd`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:complexType name="TreeType">
        <xs:sequence>
            <xs:element name="branch" type="TreeType" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
        <xs:attribute name="label" type="xs:string"/>
        <xs:attribute name="weight" type="xs:int"/>
    </xs:complexType>

    <xs:element name="Section">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="SectionTitle" type="xs:string"/>
                <xs:element ref="Section" minOccurs="0" maxOccurs="unbounded"/>
            </xs:sequence>
        </xs:complexType>
    </xs:element>

    <xs:element name="doc">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="record" maxOccurs="unbounded">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="tree" type="TreeType" minOccurs="0"/>
                            <xs:element ref="Section" minOccurs="0"/>
                        </xs:sequence>
                        <xs:attribute name="id" type="xs:string" use="required"/>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
```

#### Input (`data.xml`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<doc>
    <record id="r1">
        <tree label="L1" weight="10">
            <branch label="L2">
                <branch label="L3" weight="30">
                    <branch label="L4"/>
                </branch>
                <branch label="L3_sibling"/>
            </branch>
        </tree>
        <Section>
            <SectionTitle>Ch 1</SectionTitle>
            <Section>
                <SectionTitle>Sec 1.1</SectionTitle>
                <Section>
                    <SectionTitle>Subsec 1.1.1</SectionTitle>
                </Section>
            </Section>
        </Section>
    </record>
</doc>
```

#### Converted rows: run `depth-2` (`max_recursion_depth=2`)

```json
[
    {
        "_id": "r1",
        "tree": {
            "_label": "L1",
            "_weight": 10,
            "branch": [
                {
                    "_label": "L2",
                    "branch": [
                        "{\"_label\": \"L3\", \"_weight\": 30, \"branch\": [{\"_label\": \"L4\"}]}",
                        "{\"_label\": \"L3_sibling\"}"
                    ]
                }
            ]
        },
        "Section": {
            "SectionTitle": "Ch 1",
            "Section": [
                {
                    "SectionTitle": "Sec 1.1",
                    "Section": [
                        "{\"SectionTitle\": \"Subsec 1.1.1\"}"
                    ]
                }
            ]
        }
    }
]
```

#### Converted rows: run `depth-1` (`max_recursion_depth=1`)

```json
[
    {
        "_id": "r1",
        "tree": {
            "_label": "L1",
            "_weight": 10,
            "branch": [
                "{\"_label\": \"L2\", \"branch\": [{\"_label\": \"L3\", \"_weight\": 30, \"branch\": [{\"_label\": \"L4\"}]}, {\"_label\": \"L3_sibling\"}]}"
            ]
        },
        "Section": {
            "SectionTitle": "Ch 1",
            "Section": [
                "{\"SectionTitle\": \"Sec 1.1\", \"Section\": [{\"SectionTitle\": \"Subsec 1.1.1\"}]}"
            ]
        }
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/xsd/spec/04_recursion_depth/expected_schema.py))

```python
"""Expected PyArrow schema for 04_recursion_depth (xml_tag='record')."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("_id", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "tree",
            pa.struct(
                [
                    pa.field("_label", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                    pa.field("_weight", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"}),
                    pa.field(
                        "branch",
                        pa.list_(
                            pa.field(
                                "item",
                                pa.struct(
                                    [
                                        pa.field(
                                            "_label", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}
                                        ),
                                        pa.field("_weight", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"}),
                                        pa.field(
                                            "branch",
                                            pa.list_(
                                                pa.field("item", pa.string(), nullable=True),
                                            ),
                                            nullable=True,
                                        ),
                                    ]
                                ),
                                nullable=True,
                            ),
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "Section",
            pa.struct(
                [
                    pa.field("SectionTitle", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
                    pa.field(
                        "Section",
                        pa.list_(
                            pa.field(
                                "item",
                                pa.struct(
                                    [
                                        pa.field(
                                            "SectionTitle",
                                            pa.string(),
                                            nullable=False,
                                            metadata={"xsd_type": "xs:string"},
                                        ),
                                        pa.field(
                                            "Section",
                                            pa.list_(
                                                pa.field("item", pa.string(), nullable=True),
                                            ),
                                            nullable=True,
                                        ),
                                    ]
                                ),
                                nullable=True,
                            ),
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
    ]
)

EXPECTED_SCHEMA_DEPTH_1 = pa.schema(
    [
        pa.field("_id", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "tree",
            pa.struct(
                [
                    pa.field("_label", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                    pa.field("_weight", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"}),
                    pa.field(
                        "branch",
                        pa.list_(
                            pa.field("item", pa.string(), nullable=True),
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "Section",
            pa.struct(
                [
                    pa.field("SectionTitle", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
                    pa.field(
                        "Section",
                        pa.list_(
                            pa.field("item", pa.string(), nullable=True),
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
    ]
)
```

---

<a id="case-spec-05-groups-and-compositors"></a>

### Model Groups, Attribute Groups, `xs:all` and Duplicate Element Names (`spec/05_groups_and_compositors`)

**Rules:** [R-SHAPE-GROUPS](#r-shape-groups), [R-SHAPE-CARDINALITY](#r-shape-cardinality), [R-ORDER](#r-order), [R-NULL-CHOICE](#r-null-choice), [R-NULL-CHILD](#r-null-child)

**Target:** `xml_tag='item'`

- `pagination` is `(StartPage, EndPage?, MedlinePgn?) | MedlinePgn`: `MedlinePgn` appears in both choice
  branches but has `overall_max_occurs == 1`, so it is a single field emitted once.
- `pairs` repeats `<xs:group ref="kvGroup" maxOccurs="unbounded"/>`, so `k` and `v` are lists.
- `meta` uses `<xs:all>`; `auditAttrs` is an `xs:attributeGroup`.

#### Schema (`schema.xsd`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:attributeGroup name="auditAttrs">
        <xs:attribute name="createdBy" type="xs:string"/>
        <xs:attribute name="version" type="xs:int"/>
    </xs:attributeGroup>

    <xs:group name="kvGroup">
        <xs:sequence>
            <xs:element name="k" type="xs:string"/>
            <xs:element name="v" type="xs:int"/>
        </xs:sequence>
    </xs:group>

    <xs:complexType name="paginationType">
        <xs:choice>
            <xs:sequence>
                <xs:element name="StartPage" type="xs:string"/>
                <xs:element name="EndPage" type="xs:string" minOccurs="0"/>
                <xs:element name="MedlinePgn" type="xs:string" minOccurs="0"/>
            </xs:sequence>
            <xs:element name="MedlinePgn" type="xs:string"/>
        </xs:choice>
    </xs:complexType>

    <xs:complexType name="interactionType">
        <xs:sequence>
            <xs:element name="interactant" type="xs:string"/>
            <xs:element name="interactant" type="xs:string"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="metadataAllType">
        <xs:all>
            <xs:element name="author" type="xs:string"/>
            <xs:element name="year" type="xs:int" minOccurs="0"/>
        </xs:all>
    </xs:complexType>

    <xs:element name="dataset">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="item" maxOccurs="unbounded">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="pagination" type="paginationType" minOccurs="0"/>
                            <xs:element name="interaction" type="interactionType" minOccurs="0"/>
                            <xs:element name="meta" type="metadataAllType" minOccurs="0"/>
                            <xs:element name="pairs" minOccurs="0">
                                <xs:complexType>
                                    <xs:group ref="kvGroup" maxOccurs="unbounded"/>
                                </xs:complexType>
                            </xs:element>
                        </xs:sequence>
                        <xs:attributeGroup ref="auditAttrs"/>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
```

#### Input (`data.xml`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<dataset>
    <item createdBy="alice" version="1">
        <pagination>
            <StartPage>10</StartPage>
            <EndPage>20</EndPage>
            <MedlinePgn>10-20</MedlinePgn>
        </pagination>
        <interaction>
            <interactant>P12345</interactant>
            <interactant>Q67890</interactant>
        </interaction>
        <meta>
            <year>2025</year>
            <author>Bob</author>
        </meta>
        <pairs>
            <k>a</k>
            <v>1</v>
            <k>b</k>
            <v>2</v>
        </pairs>
    </item>
    <item>
        <pagination>
            <MedlinePgn>e12345</MedlinePgn>
        </pagination>
    </item>
</dataset>
```

#### Converted rows: run `default`

```json
[
    {
        "_createdBy": "alice",
        "_version": 1,
        "pagination": {
            "StartPage": "10",
            "EndPage": "20",
            "MedlinePgn": "10-20"
        },
        "interaction": {
            "interactant": [
                "P12345",
                "Q67890"
            ]
        },
        "meta": {
            "author": "Bob",
            "year": 2025
        },
        "pairs": {
            "k": [
                "a",
                "b"
            ],
            "v": [
                1,
                2
            ]
        }
    },
    {
        "pagination": {
            "MedlinePgn": "e12345"
        }
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/xsd/spec/05_groups_and_compositors/expected_schema.py))

```python
"""Expected PyArrow schema for 05_groups_and_compositors (xml_tag='item')."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("_createdBy", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_version", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"}),
        pa.field(
            "pagination",
            pa.struct(
                [
                    pa.field("StartPage", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                    pa.field("EndPage", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                    pa.field("MedlinePgn", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "interaction",
            pa.struct(
                [
                    pa.field(
                        "interactant",
                        pa.list_(
                            pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                        ),
                        nullable=False,
                    ),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "meta",
            pa.struct(
                [
                    pa.field("author", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
                    pa.field("year", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"}),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "pairs",
            pa.struct(
                [
                    pa.field(
                        "k",
                        pa.list_(
                            pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                        ),
                        nullable=False,
                    ),
                    pa.field(
                        "v",
                        pa.list_(
                            pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"}),
                        ),
                        nullable=False,
                    ),
                ]
            ),
            nullable=True,
        ),
    ]
)
```

---

<a id="case-spec-06-context-dependent-paths"></a>

### Context-Dependent Same `(parent, child)` Names with Different Types (`spec/06_context_dependent_paths`)

**Rules:** [R-SHAPE-CONTEXT-PATH](#r-shape-context-path), [R-SHAPE-SIMPLECONTENT](#r-shape-simplecontent), [R-NULL-CHILD](#r-null-child)

**Target:** `xml_tag='root'`

`left/node/leaf` is a single `xs:string`; `right/node/leaf` is an unbounded `simpleContent` type with a `lang`
attribute. The ancestor paths differ, so each `leaf` gets its own type.

#### Schema (`schema.xsd`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="root">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="left">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="node">
                                <xs:complexType>
                                    <xs:sequence>
                                        <xs:element name="leaf" type="xs:string"/>
                                    </xs:sequence>
                                </xs:complexType>
                            </xs:element>
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>
                <xs:element name="right">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="node">
                                <xs:complexType>
                                    <xs:sequence>
                                        <xs:element name="leaf" maxOccurs="unbounded">
                                            <xs:complexType>
                                                <xs:simpleContent>
                                                    <xs:extension base="xs:string">
                                                        <xs:attribute name="lang" type="xs:string"/>
                                                    </xs:extension>
                                                </xs:simpleContent>
                                            </xs:complexType>
                                        </xs:element>
                                    </xs:sequence>
                                </xs:complexType>
                            </xs:element>
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
```

#### Input (`data.xml`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<root>
    <left>
        <node>
            <leaf>l1</leaf>
        </node>
    </left>
    <right>
        <node>
            <leaf>r1</leaf>
            <leaf lang="en">r2</leaf>
        </node>
    </right>
</root>
```

#### Converted rows: run `default`

```json
[
    {
        "left": {
            "node": {
                "leaf": "l1"
            }
        },
        "right": {
            "node": {
                "leaf": [
                    {
                        "#text": "r1"
                    },
                    {
                        "_lang": "en",
                        "#text": "r2"
                    }
                ]
            }
        }
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/xsd/spec/06_context_dependent_paths/expected_schema.py))

```python
"""Expected PyArrow schema for 06_context_dependent_paths (xml_tag='root')."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "left",
            pa.struct(
                [
                    pa.field(
                        "node",
                        pa.struct(
                            [
                                pa.field("leaf", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
                            ]
                        ),
                        nullable=False,
                    ),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "right",
            pa.struct(
                [
                    pa.field(
                        "node",
                        pa.struct(
                            [
                                pa.field(
                                    "leaf",
                                    pa.list_(
                                        pa.field(
                                            "item",
                                            pa.struct(
                                                [
                                                    pa.field(
                                                        "_lang",
                                                        pa.string(),
                                                        nullable=True,
                                                        metadata={"xsd_type": "xs:string"},
                                                    ),
                                                    pa.field(
                                                        "#text",
                                                        pa.string(),
                                                        nullable=True,
                                                        metadata={"xsd_type": "xs:string"},
                                                    ),
                                                ]
                                            ),
                                            nullable=True,
                                        ),
                                    ),
                                    nullable=False,
                                ),
                            ]
                        ),
                        nullable=False,
                    ),
                ]
            ),
            nullable=False,
        ),
    ]
)
```

---

<a id="case-spec-07-multi-root-shared-type"></a>

### Multiple Root Elements Sharing the Same Child `xml_tag` (`spec/07_multi_root_shared_type`)

**Rules:** [R-RESOLVE-DEPTH](#r-resolve-depth)

**Target:** `xml_tag='entry'`

Several root elements each declare a child `entry` of the same named type (the `uniref.xsd` pattern), so
resolving `xml_tag="entry"` is unambiguous.

#### Schema (`schema.xsd`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:complexType name="entryType">
        <xs:sequence>
            <xs:element name="name" type="xs:string"/>
        </xs:sequence>
        <xs:attribute name="id" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="clusterSetType">
        <xs:sequence>
            <xs:element name="entry" type="entryType" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:element name="Cluster100" type="clusterSetType"/>
    <xs:element name="Cluster90" type="clusterSetType"/>
    <xs:element name="Cluster50" type="clusterSetType"/>
</xs:schema>
```

#### Input (`data.xml`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Cluster50>
    <entry id="C1">
        <name>First Cluster</name>
    </entry>
    <entry id="C2">
        <name>Second Cluster</name>
    </entry>
</Cluster50>
```

#### Converted rows: run `default`

```json
[
    {
        "_id": "C1",
        "name": "First Cluster"
    },
    {
        "_id": "C2",
        "name": "Second Cluster"
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/xsd/spec/07_multi_root_shared_type/expected_schema.py))

```python
"""Expected PyArrow schema for 07_multi_root_shared_type (xml_tag='entry')."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("_id", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("name", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
    ]
)
```

---

<a id="case-spec-08-nullability-and-empty-tags"></a>

### Field Nullability and Empty String Tag Coercion (`spec/08_nullability_and_empty_tags`)

**Rules:** [R-NULL-ATTR](#r-null-attr), [R-NULL-CHILD](#r-null-child), [R-NULL-CHOICE](#r-null-choice), [R-NULL-NILLABLE-PARENT](#r-null-nillable-parent), [R-EMPTY-COERCE](#r-empty-coerce), [R-NIL](#r-nil)

**Target:** `xml_tag='entry'`

- `_id` (`use="required"`) is non-nullable; `_source` is nullable.
- `summary` is required and non-nillable, so the present `<summary/>` coerces to `""`.
- `geneId` / `proteinId` sit in an `xs:choice` and are nullable.
- `comment` is `nillable="true"`, so its required child `text` is nullable.

#### Schema (`schema.xsd`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:complexType name="commentType">
        <xs:sequence>
            <xs:element name="text" type="xs:string"/>
        </xs:sequence>
        <xs:attribute name="type" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:element name="root">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="entry" maxOccurs="unbounded">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="name" type="xs:string"/>
                            <xs:element name="summary" type="xs:string"/>
                            <xs:element name="optionalNote" type="xs:string" minOccurs="0"/>
                            <xs:choice>
                                <xs:element name="geneId" type="xs:int"/>
                                <xs:element name="proteinId" type="xs:string"/>
                            </xs:choice>
                            <xs:element name="comment" type="commentType" nillable="true"/>
                        </xs:sequence>
                        <xs:attribute name="id" type="xs:string" use="required"/>
                        <xs:attribute name="source" type="xs:string" use="optional"/>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
```

#### Input (`data.xml`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<root xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <entry id="e1">
        <name>BRCA1</name>
        <summary/>
        <geneId>672</geneId>
        <comment type="function" xsi:nil="true"/>
    </entry>
</root>
```

#### Converted rows: run `default`

```json
[
    {
        "_id": "e1",
        "name": "BRCA1",
        "summary": "",
        "geneId": 672,
        "comment": {
            "_type": "function"
        }
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/xsd/spec/08_nullability_and_empty_tags/expected_schema.py))

```python
"""Expected PyArrow schema for 08_nullability_and_empty_tags (xml_tag='entry')."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("_id", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("_source", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("name", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("summary", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("optionalNote", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("geneId", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"}),
        pa.field("proteinId", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "comment",
            pa.struct(
                [
                    pa.field("_type", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                    pa.field("text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                ]
            ),
            nullable=True,
        ),
    ]
)
```

---

<a id="case-spec-09-empty-marker-elements"></a>

### Purely Empty `complexType` Marker Elements (`spec/09_empty_marker_elements`)

**Rules:** [R-SHAPE-EMPTY-MARKER](#r-shape-empty-marker), [R-EMPTY-COERCE](#r-empty-coerce), [R-SHAPE-CARDINALITY](#r-shape-cardinality)

**Target:** `xml_tag='book'`

DTD-derived markers such as PubMed's `<BookInProcess/>` map to `pa.string()` (`""` when present) and repeated
markers to `pa.list_(pa.string())`.

#### Schema (`schema.xsd`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:complexType name="EmptyType"/>

    <xs:element name="books">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="book" maxOccurs="unbounded">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="title" type="xs:string"/>
                            <xs:element name="BookInProcess" type="EmptyType" minOccurs="0"/>
                            <xs:element name="flag" type="EmptyType" minOccurs="0" maxOccurs="unbounded"/>
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
```

#### Input (`data.xml`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<books>
    <book>
        <title>In-Process Book</title>
        <BookInProcess/>
        <flag/>
        <flag/>
    </book>
    <book>
        <title>Completed Book</title>
    </book>
</books>
```

#### Converted rows: run `default`

```json
[
    {
        "title": "In-Process Book",
        "BookInProcess": "",
        "flag": [
            "",
            ""
        ]
    },
    {
        "title": "Completed Book"
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/xsd/spec/09_empty_marker_elements/expected_schema.py))

```python
"""Expected PyArrow schema for 09_empty_marker_elements (xml_tag='book')."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("title", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("BookInProcess", pa.string(), nullable=True),
        pa.field(
            "flag",
            pa.list_(
                pa.field("item", pa.string(), nullable=True),
            ),
            nullable=True,
        ),
    ]
)
```

---

<a id="case-spec-10-combinations"></a>

### Combinations (`spec/10_combinations`)

**Rules:** [R-SHAPE-CARDINALITY](#r-shape-cardinality), [R-SHAPE-EXTENSION](#r-shape-extension), [R-SHAPE-ATTR-ONLY](#r-shape-attr-only), [R-SHAPE-SIMPLECONTENT](#r-shape-simplecontent), [R-SHAPE-MIXED](#r-shape-mixed), [R-SHAPE-EMPTY-MARKER](#r-shape-empty-marker), [R-SHAPE-CONTEXT-PATH](#r-shape-context-path), [R-RECURSION](#r-recursion), [R-ORDER](#r-order), [R-NAME-TEXT](#r-name-text)

**Target:** `xml_tag='record'`

Stress test combining the constructs above in one type: bounded and unbounded repeats, `maxOccurs="0"`
children, children of repeated `xs:choice` / `xs:sequence` compositors, `simpleContent` with string and
decimal bases, attribute-only types, mixed types with and without attributes, element `ref`, a
self-recursive type and `complexContent` extension.

#### Schema (`schema.xsd`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!--
  Schema for testing the force_list and force_cdata callables built from an XSD.
  The comment before each declaration gives the expected classification of the
  (parent, child) pair: list, single, and/or cdata.
-->
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <!-- simple content with attributes: cdata -->
    <xs:complexType name="LabelType">
        <xs:simpleContent>
            <xs:extension base="xs:string">
                <xs:attribute name="lang" type="xs:string"/>
                <!-- xmltodict does not apply defaults: an absent attribute stays absent -->
                <xs:attribute name="source" type="xs:string" default="manual"/>
            </xs:extension>
        </xs:simpleContent>
    </xs:complexType>
    <!-- simple content without attributes: always a string or None, not cdata -->
    <xs:complexType name="TextNoAttrType">
        <xs:simpleContent>
            <xs:extension base="xs:string"/>
        </xs:simpleContent>
    </xs:complexType>
    <!-- non-string simple content with attributes: cdata; the value stays a string -->
    <xs:complexType name="MeasureType">
        <xs:simpleContent>
            <xs:extension base="xs:decimal">
                <xs:attribute name="unit" type="xs:string"/>
            </xs:extension>
        </xs:simpleContent>
    </xs:complexType>
    <!-- attributes only, no content: a dict or None, not cdata -->
    <xs:complexType name="FlagType">
        <xs:attribute name="on" type="xs:boolean"/>
    </xs:complexType>
    <!-- no attributes, no content: always None -->
    <xs:complexType name="EmptyType"/>
    <xs:complexType name="PropType">
        <xs:attribute name="key" type="xs:string" use="required"/>
        <xs:attribute name="value" type="xs:string"/>
    </xs:complexType>
    <!-- element-only content with attributes; attribute "name" and child "name" share a name -->
    <xs:complexType name="EntityType">
        <xs:sequence>
            <!-- (entity, name): single -->
            <xs:element name="name" type="xs:string" minOccurs="0"/>
            <!-- (entity, prop): list -->
            <xs:element name="prop" type="PropType" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
        <xs:attribute name="type" type="xs:string"/>
        <xs:attribute name="name" type="xs:string"/>
    </xs:complexType>
    <!-- mixed content with child elements and an attribute: cdata -->
    <xs:complexType name="ParaType" mixed="true">
        <!-- (para, b), (para, i): list, via the repeated choice -->
        <xs:choice minOccurs="0" maxOccurs="unbounded">
            <xs:element name="b" type="xs:string"/>
            <xs:element name="i" type="xs:string"/>
        </xs:choice>
        <xs:attribute name="style" type="xs:string"/>
    </xs:complexType>
    <!-- mixed content with an attribute and no child elements: cdata -->
    <xs:complexType name="NoteType" mixed="true">
        <xs:attribute name="kind" type="xs:string"/>
    </xs:complexType>
    <!-- mixed content with no attributes and no child elements: always a string or None, not cdata -->
    <xs:complexType name="RemarkType" mixed="true"/>
    <!-- self-referential type -->
    <xs:complexType name="TreeType">
        <xs:sequence>
            <!-- (tree, branch) and (branch, branch): list -->
            <xs:element name="branch" type="TreeType" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
        <xs:attribute name="label" type="xs:string"/>
    </xs:complexType>
    <xs:complexType name="BaseType">
        <xs:sequence>
            <!-- (derived, baseField): single -->
            <xs:element name="baseField" type="xs:string" minOccurs="0"/>
        </xs:sequence>
        <xs:attribute name="baseAttr" type="xs:string"/>
    </xs:complexType>
    <!-- derivation by extension: base and extension children both belong to "derived" -->
    <xs:complexType name="DerivedType">
        <xs:complexContent>
            <xs:extension base="BaseType">
                <xs:sequence>
                    <!-- (derived, extraField): list -->
                    <xs:element name="extraField" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
                </xs:sequence>
                <xs:attribute name="extraAttr" type="xs:string"/>
            </xs:extension>
        </xs:complexContent>
    </xs:complexType>
    <xs:complexType name="RecordType">
        <xs:sequence>
            <!-- (record, plain): single -->
            <xs:element name="plain" type="xs:string" minOccurs="0"/>
            <!-- (record, plainList): list -->
            <xs:element name="plainList" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <!-- (record, bounded): list -->
            <xs:element name="bounded" type="xs:string" minOccurs="0" maxOccurs="2"/>
            <!-- (record, never): in no set -->
            <xs:element name="never" type="xs:string" minOccurs="0" maxOccurs="0"/>
            <!-- (record, label): single, cdata -->
            <xs:element name="label" type="LabelType" minOccurs="0"/>
            <!-- (record, labelList): list, cdata -->
            <xs:element name="labelList" type="LabelType" minOccurs="0" maxOccurs="unbounded"/>
            <!-- (record, textNoAttr): single -->
            <xs:element name="textNoAttr" type="TextNoAttrType" minOccurs="0"/>
            <!-- (record, measure): single, cdata -->
            <xs:element name="measure" type="MeasureType" minOccurs="0"/>
            <!-- (record, flag): single -->
            <xs:element name="flag" type="FlagType" minOccurs="0"/>
            <!-- (record, flagList): list -->
            <xs:element name="flagList" type="FlagType" minOccurs="0" maxOccurs="unbounded"/>
            <!-- (record, marker): list -->
            <xs:element name="marker" type="EmptyType" minOccurs="0" maxOccurs="unbounded"/>
            <!-- (record, group): single; element-only content, no attributes -->
            <xs:element name="group" minOccurs="0">
                <xs:complexType>
                    <xs:sequence>
                        <!-- (group, name): single -->
                        <xs:element name="name" type="xs:string"/>
                        <!-- (group, alias): list -->
                        <xs:element name="alias" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <!-- (record, box): single; element-only content where every child is optional -->
            <xs:element name="box" minOccurs="0">
                <xs:complexType>
                    <xs:sequence>
                        <!-- (box, item): list -->
                        <xs:element name="item" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <!-- (record, entity): list -->
            <xs:element name="entity" type="EntityType" minOccurs="0" maxOccurs="unbounded"/>
            <!-- (record, para): list, cdata -->
            <xs:element name="para" type="ParaType" minOccurs="0" maxOccurs="unbounded"/>
            <!-- (record, note): single, cdata -->
            <xs:element name="note" type="NoteType" minOccurs="0"/>
            <!-- (record, remark): single -->
            <xs:element name="remark" type="RemarkType" minOccurs="0"/>
            <!-- children of a repeated choice are lists even though each has maxOccurs=1 -->
            <xs:choice minOccurs="0" maxOccurs="unbounded">
                <!-- (record, optA): list -->
                <xs:element name="optA" type="xs:string"/>
                <!-- (record, optB): list, cdata -->
                <xs:element name="optB" type="LabelType"/>
            </xs:choice>
            <!-- (record, pairs): single -->
            <xs:element name="pairs" minOccurs="0">
                <xs:complexType>
                    <!-- children of a repeated sequence are lists even though each has maxOccurs=1 -->
                    <xs:sequence maxOccurs="2">
                        <!-- (pairs, k), (pairs, v): list -->
                        <xs:element name="k" type="xs:string"/>
                        <xs:element name="v" type="xs:string"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <!-- the child name "title" is classified differently under different parents -->
            <!-- (record, book): single -->
            <xs:element name="book" minOccurs="0">
                <xs:complexType>
                    <xs:sequence>
                        <!-- (book, title): single -->
                        <xs:element name="title" type="xs:string"/>
                        <!-- (book, chapter): list -->
                        <xs:element name="chapter" minOccurs="0" maxOccurs="unbounded">
                            <xs:complexType>
                                <xs:sequence>
                                    <!-- (chapter, title): list, cdata -->
                                    <xs:element name="title" type="LabelType" maxOccurs="unbounded"/>
                                </xs:sequence>
                            </xs:complexType>
                        </xs:element>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <!-- the pair (node, leaf) is declared twice with different cardinality and content type -->
            <!-- (record, left): single -->
            <xs:element name="left" minOccurs="0">
                <xs:complexType>
                    <xs:sequence>
                        <!-- (left, node): single -->
                        <xs:element name="node">
                            <xs:complexType>
                                <xs:sequence>
                                    <!-- declared single, no attributes; merged result is list, cdata -->
                                    <xs:element name="leaf" type="xs:string"/>
                                </xs:sequence>
                            </xs:complexType>
                        </xs:element>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <!-- (record, right): single -->
            <xs:element name="right" minOccurs="0">
                <xs:complexType>
                    <xs:sequence>
                        <!-- (right, node): single -->
                        <xs:element name="node">
                            <xs:complexType>
                                <xs:sequence>
                                    <!-- declared list, with attributes; merged result is list, cdata -->
                                    <xs:element name="leaf" type="LabelType" maxOccurs="unbounded"/>
                                </xs:sequence>
                            </xs:complexType>
                        </xs:element>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <!-- (record, tree): single -->
            <xs:element name="tree" type="TreeType" minOccurs="0"/>
            <!-- (record, shared): list; declared by reference to a global element -->
            <xs:element ref="shared" minOccurs="0" maxOccurs="unbounded"/>
            <!-- (record, derived): single -->
            <xs:element name="derived" type="DerivedType" minOccurs="0"/>
        </xs:sequence>
        <xs:attribute name="id" type="xs:string" use="required"/>
    </xs:complexType>
    <xs:element name="shared" type="xs:string"/>
    <xs:element name="dataset">
        <xs:complexType>
            <xs:sequence>
                <!-- (dataset, record): list -->
                <xs:element name="record" type="RecordType" minOccurs="0" maxOccurs="unbounded"/>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
```

#### Input (`data.xml`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!--
  Instance document for xmltodict_combinations.xsd.
  Each record is parsed on its own (xml_tag="record"); combinations.json holds the expected output.
-->
<dataset>
    <!-- r01: record with an attribute and no children -->
    <record id="r01"/>
    <!-- r02: one occurrence each of a single, an unbounded and a bounded string element -->
    <record id="r02">
        <plain>a</plain>
        <plainList>x</plainList>
        <bounded>only</bounded>
    </record>
    <!-- r03: empty and whitespace-only elements become None; several occurrences of list elements -->
    <record id="r03">
        <plain/>
        <plainList>x</plainList>
        <plainList/>
        <plainList>   </plainList>
        <plainList>y</plainList>
        <bounded>b1</bounded>
        <bounded>b2</bounded>
    </record>
    <!-- r04: CDATA section, entity reference, leading and trailing whitespace -->
    <record id="r04">
        <plain>
            <![CDATA[a < b & c]]>
        </plain>
        <plainList>  padded  </plainList>
        <plainList>Tom &amp; Jerry</plainList>
    </record>
    <!-- r05: simple content with attributes: attributes and text, text only, attribute only, empty -->
    <record id="r05">
        <label lang="en">Hello</label>
        <labelList lang="en" source="curated">one</labelList>
        <labelList>two</labelList>
        <labelList lang="fr"/>
        <labelList/>
    </record>
    <!-- r06: simple content with attributes, no attribute in the instance -->
    <record id="r06">
        <label>Hello</label>
        <labelList>solo</labelList>
    </record>
    <!-- r07: attribute only; simple content without attributes; wildcard attributes; decimal content -->
    <record id="r07">
        <label lang="de"/>
        <textNoAttr>t</textNoAttr>
        <measure unit="kg">3.50</measure>
    </record>
    <!-- r08: the same elements as r07, empty or without attributes -->
    <record id="r08">
        <label/>
        <textNoAttr/>
        <measure>7</measure>
    </record>
    <!-- r09: attribute-only and empty types, several occurrences -->
    <record id="r09">
        <flag on="true"/>
        <flagList on="false"/>
        <flagList/>
        <marker/>
        <marker/>
    </record>
    <!-- r10: attribute-only and empty types, one occurrence, no attributes on the single element -->
    <record id="r10">
        <flag/>
        <flagList on="1"/>
        <marker/>
    </record>
    <!-- r11: element-only content; attribute and child with the same name; empty element-only element -->
    <record id="r11">
        <group>
            <name>g1</name>
            <alias>a1</alias>
            <alias>a2</alias>
        </group>
        <box/>
        <entity type="person" name="attr-name">
            <name>child-name</name>
            <prop key="k1" value="v1"/>
            <prop key="k2"/>
        </entity>
        <entity type="org"/>
    </record>
    <!-- r12: optional list children absent; one occurrence of list children -->
    <record id="r12">
        <group>
            <name>g2</name>
        </group>
        <box>
            <item>1</item>
        </box>
        <entity>
            <prop key="only"/>
        </entity>
    </record>
    <!-- r13: mixed content: text only, text and children, children only, empty, interleaved, attribute and children -->
    <record id="r13">
        <para>plain text</para>
        <para>Hello
            <b>world</b> again
        </para>
        <para>
            <b>x</b>
        </para>
        <para/>
        <para>A
            <b>1</b>B
            <i>2</i>C
            <b>3</b>
        </para>
        <para style="em">Intro
            <b>bold</b>
        </para>
        <note kind="aside">side text</note>
        <remark>just text</remark>
    </record>
    <!-- r14: mixed content: one child only, attribute only; mixed element without attributes or children -->
    <record id="r14">
        <para>
            <i>only italic</i>
        </para>
        <para style="x"/>
        <note>bare note</note>
        <remark/>
    </record>
    <!-- r15: mixed content with attribute and no text; repeated choice, interleaved -->
    <record id="r15">
        <note kind="empty"/>
        <optA>a1</optA>
        <optB lang="en">b1</optB>
        <optA>a2</optA>
        <optB>b2</optB>
    </record>
    <!-- r16: repeated choice with one occurrence; repeated sequence with one occurrence -->
    <record id="r16">
        <optA>solo</optA>
        <pairs>
            <k>a</k>
            <v>1</v>
        </pairs>
    </record>
    <!-- r17: empty choice member with simple content and attributes; repeated sequence twice -->
    <record id="r17">
        <optB/>
        <pairs>
            <k>a</k>
            <v>1</v>
            <k>b</k>
            <v>2</v>
        </pairs>
    </record>
    <!-- r18: "title" is single under book and list plus cdata under chapter -->
    <record id="r18">
        <book>
            <title>Book title</title>
            <chapter>
                <title>Ch1</title>
            </chapter>
            <chapter>
                <title lang="en">Ch2</title>
                <title>Ch2 alt</title>
            </chapter>
        </book>
    </record>
    <!-- r19: optional list child absent -->
    <record id="r19">
        <book>
            <title>Lonely</title>
        </book>
    </record>
    <!-- r20: (node, leaf) declared twice; the merged classification applies under both parents -->
    <record id="r20">
        <left>
            <node>
                <leaf>l1</leaf>
            </node>
        </left>
        <right>
            <node>
                <leaf>r1</leaf>
                <leaf lang="en">r2</leaf>
            </node>
        </right>
    </record>
    <!-- r21: (node, leaf) empty and single occurrence -->
    <record id="r21">
        <left>
            <node>
                <leaf/>
            </node>
        </left>
        <right>
            <node>
                <leaf>r</leaf>
            </node>
        </right>
    </record>
    <!-- r22: self-referential type, three levels, with an empty branch -->
    <record id="r22">
        <tree label="root">
            <branch label="a">
                <branch label="a1"/>
                <branch label="a2">
                    <branch label="a2x"/>
        </branch>
            </branch>
            <branch/>
        </tree>
    </record>
    <!-- r23: self-referential type, one branch inside one branch -->
    <record id="r23">
        <tree label="t">
            <branch>
                <branch label="deep"/>
      </branch>
        </tree>
    </record>
    <!-- r24: empty tree; comment inside a record; element declared by ref; derived type -->
    <record id="r24">
        <tree/>
        <!-- comments are dropped by xmltodict unless process_comments=True -->
        <shared>s1</shared>
        <derived baseAttr="b" extraAttr="e">
            <baseField>base</baseField>
            <extraField>x1</extraField>
        </derived>
    </record>
    <!-- r25: element declared by ref, several occurrences; derived type with extension children only -->
    <record id="r25">
        <shared>s1</shared>
        <shared>s2</shared>
        <derived>
            <extraField>x1</extraField>
            <extraField>x2</extraField>
        </derived>
    </record>
</dataset>
```

#### Converted rows: run `default`

```json
[
    {
        "_id": "r01"
    },
    {
        "_id": "r02",
        "plain": "a",
        "plainList": [
            "x"
        ],
        "bounded": [
            "only"
        ]
    },
    {
        "_id": "r03",
        "plainList": [
            "x",
            null,
            null,
            "y"
        ],
        "bounded": [
            "b1",
            "b2"
        ]
    },
    {
        "_id": "r04",
        "plain": "a < b & c",
        "plainList": [
            "padded",
            "Tom & Jerry"
        ]
    },
    {
        "_id": "r05",
        "label": {
            "_lang": "en",
            "#text": "Hello"
        },
        "labelList": [
            {
                "_lang": "en",
                "_source": "curated",
                "#text": "one"
            },
            {
                "#text": "two"
            },
            {
                "_lang": "fr"
            },
            null
        ]
    },
    {
        "_id": "r06",
        "label": {
            "#text": "Hello"
        },
        "labelList": [
            {
                "#text": "solo"
            }
        ]
    },
    {
        "_id": "r07",
        "label": {
            "_lang": "de"
        },
        "textNoAttr": "t",
        "measure": {
            "_unit": "kg",
            "#text": 3.5
        }
    },
    {
        "_id": "r08",
        "measure": {
            "#text": 7.0
        }
    },
    {
        "_id": "r09",
        "flag": {
            "_on": true
        },
        "flagList": [
            {
                "_on": false
            },
            null
        ],
        "marker": [
            "",
            ""
        ]
    },
    {
        "_id": "r10",
        "flagList": [
            {
                "_on": true
            }
        ],
        "marker": [
            ""
        ]
    },
    {
        "_id": "r11",
        "group": {
            "name": "g1",
            "alias": [
                "a1",
                "a2"
            ]
        },
        "entity": [
            {
                "_type": "person",
                "_name": "attr-name",
                "name": "child-name",
                "prop": [
                    {
                        "_key": "k1",
                        "_value": "v1"
                    },
                    {
                        "_key": "k2"
                    }
                ]
            },
            {
                "_type": "org"
            }
        ]
    },
    {
        "_id": "r12",
        "group": {
            "name": "g2"
        },
        "box": {
            "item": [
                "1"
            ]
        },
        "entity": [
            {
                "prop": [
                    {
                        "_key": "only"
                    }
                ]
            }
        ]
    },
    {
        "_id": "r13",
        "para": [
            {
                "#text": "plain text"
            },
            {
                "#text": "Hello\n            <b>world</b> again"
            },
            {
                "#text": "<b>x</b>"
            },
            null,
            {
                "#text": "A\n            <b>1</b>B\n            <i>2</i>C\n            <b>3</b>"
            },
            {
                "_style": "em",
                "#text": "Intro\n            <b>bold</b>"
            }
        ],
        "note": {
            "_kind": "aside",
            "#text": "side text"
        },
        "remark": "just text"
    },
    {
        "_id": "r14",
        "para": [
            {
                "#text": "<i>only italic</i>"
            },
            {
                "_style": "x"
            }
        ],
        "note": {
            "#text": "bare note"
        }
    },
    {
        "_id": "r15",
        "note": {
            "_kind": "empty"
        },
        "optA": [
            "a1",
            "a2"
        ],
        "optB": [
            {
                "_lang": "en",
                "#text": "b1"
            },
            {
                "#text": "b2"
            }
        ]
    },
    {
        "_id": "r16",
        "optA": [
            "solo"
        ],
        "pairs": {
            "k": [
                "a"
            ],
            "v": [
                "1"
            ]
        }
    },
    {
        "_id": "r17",
        "optB": [
            null
        ],
        "pairs": {
            "k": [
                "a",
                "b"
            ],
            "v": [
                "1",
                "2"
            ]
        }
    },
    {
        "_id": "r18",
        "book": {
            "title": "Book title",
            "chapter": [
                {
                    "title": [
                        {
                            "#text": "Ch1"
                        }
                    ]
                },
                {
                    "title": [
                        {
                            "_lang": "en",
                            "#text": "Ch2"
                        },
                        {
                            "#text": "Ch2 alt"
                        }
                    ]
                }
            ]
        }
    },
    {
        "_id": "r19",
        "book": {
            "title": "Lonely"
        }
    },
    {
        "_id": "r20",
        "left": {
            "node": {
                "leaf": "l1"
            }
        },
        "right": {
            "node": {
                "leaf": [
                    {
                        "#text": "r1"
                    },
                    {
                        "_lang": "en",
                        "#text": "r2"
                    }
                ]
            }
        }
    },
    {
        "_id": "r21",
        "left": {
            "node": {
                "leaf": ""
            }
        },
        "right": {
            "node": {
                "leaf": [
                    {
                        "#text": "r"
                    }
                ]
            }
        }
    },
    {
        "_id": "r22",
        "tree": {
            "_label": "root",
            "branch": [
                {
                    "_label": "a",
                    "branch": [
                        "{\"_label\": \"a1\"}",
                        "{\"_label\": \"a2\", \"branch\": [{\"_label\": \"a2x\"}]}"
                    ]
                },
                null
            ]
        }
    },
    {
        "_id": "r23",
        "tree": {
            "_label": "t",
            "branch": [
                {
                    "branch": [
                        "{\"_label\": \"deep\"}"
                    ]
                }
            ]
        }
    },
    {
        "_id": "r24",
        "shared": [
            "s1"
        ],
        "derived": {
            "_baseAttr": "b",
            "_extraAttr": "e",
            "baseField": "base",
            "extraField": [
                "x1"
            ]
        }
    },
    {
        "_id": "r25",
        "shared": [
            "s1",
            "s2"
        ],
        "derived": {
            "extraField": [
                "x1",
                "x2"
            ]
        }
    }
]
```

#### PyArrow schema ([`expected_schema.py`](../tests/data/xsd/spec/10_combinations/expected_schema.py))

```python
"""Expected PyArrow schema for 10_combinations (xml_tag='record')."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("_id", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("plain", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "plainList",
            pa.list_(
                pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
            ),
            nullable=True,
        ),
        pa.field(
            "bounded",
            pa.list_(
                pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
            ),
            nullable=True,
        ),
        pa.field(
            "label",
            pa.struct(
                [
                    pa.field("_lang", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                    pa.field("_source", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                    pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "labelList",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field("_lang", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                            pa.field("_source", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                            pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                        ]
                    ),
                    nullable=True,
                ),
            ),
            nullable=True,
        ),
        pa.field("textNoAttr", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "measure",
            pa.struct(
                [
                    pa.field("_unit", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                    pa.field("#text", pa.float64(), nullable=True, metadata={"xsd_type": "xs:decimal"}),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "flag",
            pa.struct(
                [
                    pa.field("_on", pa.bool_(), nullable=True, metadata={"xsd_type": "xs:boolean"}),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "flagList",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field("_on", pa.bool_(), nullable=True, metadata={"xsd_type": "xs:boolean"}),
                        ]
                    ),
                    nullable=True,
                ),
            ),
            nullable=True,
        ),
        pa.field(
            "marker",
            pa.list_(
                pa.field("item", pa.string(), nullable=True),
            ),
            nullable=True,
        ),
        pa.field(
            "group",
            pa.struct(
                [
                    pa.field("name", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
                    pa.field(
                        "alias",
                        pa.list_(
                            pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "box",
            pa.struct(
                [
                    pa.field(
                        "item",
                        pa.list_(
                            pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "entity",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field("_type", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                            pa.field("_name", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                            pa.field("name", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                            pa.field(
                                "prop",
                                pa.list_(
                                    pa.field(
                                        "item",
                                        pa.struct(
                                            [
                                                pa.field(
                                                    "_key",
                                                    pa.string(),
                                                    nullable=False,
                                                    metadata={"xsd_type": "xs:string"},
                                                ),
                                                pa.field(
                                                    "_value",
                                                    pa.string(),
                                                    nullable=True,
                                                    metadata={"xsd_type": "xs:string"},
                                                ),
                                            ]
                                        ),
                                        nullable=True,
                                    ),
                                ),
                                nullable=True,
                            ),
                        ]
                    ),
                    nullable=True,
                ),
            ),
            nullable=True,
        ),
        pa.field(
            "para",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field("_style", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                            pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                        ]
                    ),
                    nullable=True,
                ),
            ),
            nullable=True,
        ),
        pa.field(
            "note",
            pa.struct(
                [
                    pa.field("_kind", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                    pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                ]
            ),
            nullable=True,
        ),
        pa.field("remark", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "optA",
            pa.list_(
                pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
            ),
            nullable=True,
        ),
        pa.field(
            "optB",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field("_lang", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                            pa.field("_source", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                            pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                        ]
                    ),
                    nullable=True,
                ),
            ),
            nullable=True,
        ),
        pa.field(
            "pairs",
            pa.struct(
                [
                    pa.field(
                        "k",
                        pa.list_(
                            pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                        ),
                        nullable=False,
                    ),
                    pa.field(
                        "v",
                        pa.list_(
                            pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                        ),
                        nullable=False,
                    ),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "book",
            pa.struct(
                [
                    pa.field("title", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
                    pa.field(
                        "chapter",
                        pa.list_(
                            pa.field(
                                "item",
                                pa.struct(
                                    [
                                        pa.field(
                                            "title",
                                            pa.list_(
                                                pa.field(
                                                    "item",
                                                    pa.struct(
                                                        [
                                                            pa.field(
                                                                "_lang",
                                                                pa.string(),
                                                                nullable=True,
                                                                metadata={"xsd_type": "xs:string"},
                                                            ),
                                                            pa.field(
                                                                "_source",
                                                                pa.string(),
                                                                nullable=True,
                                                                metadata={"xsd_type": "xs:string"},
                                                            ),
                                                            pa.field(
                                                                "#text",
                                                                pa.string(),
                                                                nullable=True,
                                                                metadata={"xsd_type": "xs:string"},
                                                            ),
                                                        ]
                                                    ),
                                                    nullable=True,
                                                ),
                                            ),
                                            nullable=False,
                                        ),
                                    ]
                                ),
                                nullable=True,
                            ),
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "left",
            pa.struct(
                [
                    pa.field(
                        "node",
                        pa.struct(
                            [
                                pa.field("leaf", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
                            ]
                        ),
                        nullable=False,
                    ),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "right",
            pa.struct(
                [
                    pa.field(
                        "node",
                        pa.struct(
                            [
                                pa.field(
                                    "leaf",
                                    pa.list_(
                                        pa.field(
                                            "item",
                                            pa.struct(
                                                [
                                                    pa.field(
                                                        "_lang",
                                                        pa.string(),
                                                        nullable=True,
                                                        metadata={"xsd_type": "xs:string"},
                                                    ),
                                                    pa.field(
                                                        "_source",
                                                        pa.string(),
                                                        nullable=True,
                                                        metadata={"xsd_type": "xs:string"},
                                                    ),
                                                    pa.field(
                                                        "#text",
                                                        pa.string(),
                                                        nullable=True,
                                                        metadata={"xsd_type": "xs:string"},
                                                    ),
                                                ]
                                            ),
                                            nullable=True,
                                        ),
                                    ),
                                    nullable=False,
                                ),
                            ]
                        ),
                        nullable=False,
                    ),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "tree",
            pa.struct(
                [
                    pa.field("_label", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                    pa.field(
                        "branch",
                        pa.list_(
                            pa.field(
                                "item",
                                pa.struct(
                                    [
                                        pa.field(
                                            "_label", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}
                                        ),
                                        pa.field(
                                            "branch",
                                            pa.list_(
                                                pa.field("item", pa.string(), nullable=True),
                                            ),
                                            nullable=True,
                                        ),
                                    ]
                                ),
                                nullable=True,
                            ),
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "shared",
            pa.list_(
                pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
            ),
            nullable=True,
        ),
        pa.field(
            "derived",
            pa.struct(
                [
                    pa.field("_baseAttr", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                    pa.field("_extraAttr", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                    pa.field("baseField", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                    pa.field(
                        "extraField",
                        pa.list_(
                            pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
    ]
)
```

---

<a id="case-examples-pubmed-article"></a>

### PubMed (`PubmedArticle`) (`examples/pubmed_article`)

**Rules:** [R-SHAPE-MIXED](#r-shape-mixed), [R-RECURSION](#r-recursion), [R-SHAPE-EMPTY-MARKER](#r-shape-empty-marker), [R-NULL-CHOICE](#r-null-choice)

**Target:** `xml_tag='PubmedArticle'`

DTD-derived schema with mixed-content titles and abstracts, the `(StartPage, EndPage?, MedlinePgn?) | MedlinePgn`
pagination choice and element-`ref` self-recursion.

**Schema:** [`tests/data/pubmed/pubmed.xsd`](../tests/data/pubmed/pubmed.xsd)

- Run `minimal`: [`minimal.xml`](../tests/data/xsd/examples/pubmed_article/minimal.xml), [`minimal_expected.json`](../tests/data/xsd/examples/pubmed_article/minimal_expected.json), [`minimal_expected_table.py`](../tests/data/xsd/examples/pubmed_article/minimal_expected_table.py)
- Run `maximal`: [`maximal.xml`](../tests/data/xsd/examples/pubmed_article/maximal.xml), [`maximal_expected.json`](../tests/data/xsd/examples/pubmed_article/maximal_expected.json), [`maximal_expected_table.py`](../tests/data/xsd/examples/pubmed_article/maximal_expected_table.py)

**Expected PyArrow schema:** [`expected_schema.py`](../tests/data/xsd/examples/pubmed_article/expected_schema.py)

---

<a id="case-examples-pubmed-book-article"></a>

### PubMed (`PubmedBookArticle`) (`examples/pubmed_book_article`)

**Rules:** [R-SHAPE-MIXED](#r-shape-mixed), [R-RECURSION](#r-recursion), [R-SHAPE-EMPTY-MARKER](#r-shape-empty-marker)

**Target:** `xml_tag='PubmedBookArticle'`

Book article variant of the PubMed schema, including recursive `Section` elements and the empty
`BookInProcess` marker.

**Schema:** [`tests/data/pubmed/pubmed.xsd`](../tests/data/pubmed/pubmed.xsd)

- Run `minimal`: [`minimal.xml`](../tests/data/xsd/examples/pubmed_book_article/minimal.xml), [`minimal_expected.json`](../tests/data/xsd/examples/pubmed_book_article/minimal_expected.json), [`minimal_expected_table.py`](../tests/data/xsd/examples/pubmed_book_article/minimal_expected_table.py)
- Run `maximal`: [`maximal.xml`](../tests/data/xsd/examples/pubmed_book_article/maximal.xml), [`maximal_expected.json`](../tests/data/xsd/examples/pubmed_book_article/maximal_expected.json), [`maximal_expected_table.py`](../tests/data/xsd/examples/pubmed_book_article/maximal_expected_table.py)

**Expected PyArrow schema:** [`expected_schema.py`](../tests/data/xsd/examples/pubmed_book_article/expected_schema.py)

---

<a id="case-examples-uniprot"></a>

### UniProtKB (`examples/uniprot`)

**Rules:** [R-SHAPE-XSLIST](#r-shape-xslist), [R-SCALAR-UNION](#r-scalar-union), [R-NIL](#r-nil), [R-NULL-NILLABLE-PARENT](#r-null-nillable-parent), [R-META-COMMENT](#r-meta-comment)

**Target:** `xml_tag='entry'`

Exercises `intListType` (`xs:list`), the `date | gYearMonth | gYear` union and nillable `comment` elements
with attributes.

**Schema:** [`tests/data/uniprot/uniprot_kb/uniprot.xsd`](../tests/data/uniprot/uniprot_kb/uniprot.xsd)

- Run `minimal`: [`minimal.xml`](../tests/data/xsd/examples/uniprot/minimal.xml), [`minimal_expected.json`](../tests/data/xsd/examples/uniprot/minimal_expected.json), [`minimal_expected_table.py`](../tests/data/xsd/examples/uniprot/minimal_expected_table.py)
- Run `maximal`: [`maximal.xml`](../tests/data/xsd/examples/uniprot/maximal.xml), [`maximal_expected.json`](../tests/data/xsd/examples/uniprot/maximal_expected.json), [`maximal_expected_table.py`](../tests/data/xsd/examples/uniprot/maximal_expected_table.py)

**Expected PyArrow schema:** [`expected_schema.py`](../tests/data/xsd/examples/uniprot/expected_schema.py)

---

<a id="case-examples-uniref"></a>

### UniRef (`examples/uniref`)

**Rules:** [R-RESOLVE-DEPTH](#r-resolve-depth), [R-META-COMMENT](#r-meta-comment)

**Target:** `xml_tag='entry'`

Four root elements (`UniRef100`, `UniRef90`, `UniRef50`, `UniRef`) declare `entry` with the shared
`entryType`, so resolution succeeds.

**Schema:** [`tests/data/uniprot/uniref/uniref.xsd`](../tests/data/uniprot/uniref/uniref.xsd)

- Run `minimal`: [`minimal.xml`](../tests/data/xsd/examples/uniref/minimal.xml), [`minimal_expected.json`](../tests/data/xsd/examples/uniref/minimal_expected.json), [`minimal_expected_table.py`](../tests/data/xsd/examples/uniref/minimal_expected_table.py)
- Run `maximal`: [`maximal.xml`](../tests/data/xsd/examples/uniref/maximal.xml), [`maximal_expected.json`](../tests/data/xsd/examples/uniref/maximal_expected.json), [`maximal_expected_table.py`](../tests/data/xsd/examples/uniref/maximal_expected_table.py)

**Expected PyArrow schema:** [`expected_schema.py`](../tests/data/xsd/examples/uniref/expected_schema.py)

---

## 3. Errors (Unsupported / Invalid Input)

Schema generation raises a subclass of `XsdToPyArrowError` (itself a `ValueError`).

| Code | Exception | Condition | Reason | Fixtures |
| :--- | :--- | :--- | :--- | :--- |
| **E1** | `XmlTagNotFoundError` | `xml_tag` not found | The target element is not declared anywhere in the XSD. | [`empty_schema.xsd`](../tests/data/xsd/empty_schema.xsd): Schema declares no elements.<br>[`uniref.xsd`](../tests/data/uniprot/uniref/uniref.xsd): `xml_tag` is not declared in an otherwise valid schema. |
| **E2** | `AmbiguousXmlTagError` | Ambiguous `xml_tag` type | Declarations of `xml_tag` at the shallowest matching depth have different types, so no single `pa.Schema` can be chosen. | [`ambiguous_xml_tag.xsd`](../tests/data/xsd/spec/errors/ambiguous_xml_tag.xsd): Root `A` declares `<entry type="Type1"/>` and root `B` declares `<entry type="Type2"/>`. |
| **E3** | `NonStructXmlTagError` | Non-struct top-level `xml_tag` | A `pa.Table` schema needs top-level columns: the type of `xml_tag` must yield at least one attribute, `#text`, or child element field. | [`scalar_top_level.xsd`](../tests/data/xsd/spec/errors/scalar_top_level.xsd): `xml_tag` has a simple type (`xs:string`).<br>[`empty_marker_top_level.xsd`](../tests/data/xsd/spec/errors/empty_marker_top_level.xsd): `xml_tag` has an empty `complexType` with no attributes or children.<br>[`no_children.xsd`](../tests/data/xsd/no_children.xsd): `xml_tag` is a scalar root element. |
| **E4** | `WildcardElementError` | Element wildcard (`xs:any`) | Arbitrary undeclared child elements cannot be represented as fixed `pa.struct` fields. | [`wildcard_any_element.xsd`](../tests/data/xsd/spec/errors/wildcard_any_element.xsd): `<xs:sequence><xs:any processContents="lax"/></xs:sequence>` |
| **E5** | `WildcardAttributeError` | Attribute wildcard (`xs:anyAttribute`) | Arbitrary undeclared attributes cannot be represented as fixed `pa.struct` fields. | [`wildcard_any_attribute.xsd`](../tests/data/xsd/spec/errors/wildcard_any_attribute.xsd): `<xs:anyAttribute processContents="skip"/>`<br>[`cdata_variety.xsd`](../tests/data/xsd/cdata_variety.xsd): Legacy fixture containing `xs:anyAttribute`.<br>[`combinations.xsd`](../tests/data/xsd/combinations.xsd): Legacy fixture containing `xs:anyAttribute`. |
| **E6** | `AbstractComponentError` | Abstract element or type (`abstract="true"`) | Concrete instances of an abstract element or type require `xsi:type` or substitution polymorphism, which has no single schema. | [`abstract_element.xsd`](../tests/data/xsd/spec/errors/abstract_element.xsd): Child element declared with `abstract="true"`.<br>[`abstract_type.xsd`](../tests/data/xsd/spec/errors/abstract_type.xsd): Child element typed by a `complexType` with `abstract="true"`. |
| **E7** | `SubstitutionGroupError` | Substitution group | Substituted elements introduce element names and types that are not declared in the parent content model. | [`substitution_group.xsd`](../tests/data/xsd/spec/errors/substitution_group.xsd): `<xs:element name="sub" substitutionGroup="head" type="OtherType"/>` |
| **E8** | `FieldNameCollisionError` | Attribute / child field name collision | An attribute (prefixed `_`) and a child element map to the same field name in one `pa.struct`. | [`attr_child_name_collision.xsd`](../tests/data/xsd/spec/errors/attr_child_name_collision.xsd): Attribute `id` (field `_id`) and child element `_id` in the same type. |
| **E9** | `RecursiveNonJsonTypeError` | Non-JSON-serializable scalar under recursion | Subtrees truncated beyond `max_recursion_depth` are serialized with `json.dumps`, which cannot encode `datetime.date` or `bytes` without lossy conversion. | [`nested_date_under_recursion.xsd`](../tests/data/xsd/spec/errors/nested_date_under_recursion.xsd): Recursive `NodeType` has an `xs:date` child element.<br>[`nested_binary_under_recursion.xsd`](../tests/data/xsd/spec/errors/nested_binary_under_recursion.xsd): Recursive `NodeType` has an `xs:hexBinary` attribute. |

## 4. Fixture Index

Every case directory contains a `case.yaml` manifest; the test suite and this document are both driven by it.

| Case | `xml_tag` | Schema | Runs |
| :--- | :--- | :--- | :--- |
| [`spec/01_scalars_and_simple_types`](../tests/data/xsd/spec/01_scalars_and_simple_types/case.yaml) | `record` | [`schema.xsd`](../tests/data/xsd/spec/01_scalars_and_simple_types/schema.xsd) | default |
| [`spec/02_namespaces_and_nillable`](../tests/data/xsd/spec/02_namespaces_and_nillable/case.yaml) | `entry` | [`schema.xsd`](../tests/data/xsd/spec/02_namespaces_and_nillable/schema.xsd) | default, clark-notation |
| [`spec/03_mixed_content_raw_text`](../tests/data/xsd/spec/03_mixed_content_raw_text/case.yaml) | `article` | [`schema.xsd`](../tests/data/xsd/spec/03_mixed_content_raw_text/schema.xsd) | default |
| [`spec/04_recursion_depth`](../tests/data/xsd/spec/04_recursion_depth/case.yaml) | `record` | [`schema.xsd`](../tests/data/xsd/spec/04_recursion_depth/schema.xsd) | depth-2, depth-1 |
| [`spec/05_groups_and_compositors`](../tests/data/xsd/spec/05_groups_and_compositors/case.yaml) | `item` | [`schema.xsd`](../tests/data/xsd/spec/05_groups_and_compositors/schema.xsd) | default |
| [`spec/06_context_dependent_paths`](../tests/data/xsd/spec/06_context_dependent_paths/case.yaml) | `root` | [`schema.xsd`](../tests/data/xsd/spec/06_context_dependent_paths/schema.xsd) | default |
| [`spec/07_multi_root_shared_type`](../tests/data/xsd/spec/07_multi_root_shared_type/case.yaml) | `entry` | [`schema.xsd`](../tests/data/xsd/spec/07_multi_root_shared_type/schema.xsd) | default |
| [`spec/08_nullability_and_empty_tags`](../tests/data/xsd/spec/08_nullability_and_empty_tags/case.yaml) | `entry` | [`schema.xsd`](../tests/data/xsd/spec/08_nullability_and_empty_tags/schema.xsd) | default |
| [`spec/09_empty_marker_elements`](../tests/data/xsd/spec/09_empty_marker_elements/case.yaml) | `book` | [`schema.xsd`](../tests/data/xsd/spec/09_empty_marker_elements/schema.xsd) | default |
| [`spec/10_combinations`](../tests/data/xsd/spec/10_combinations/case.yaml) | `record` | [`schema.xsd`](../tests/data/xsd/spec/10_combinations/schema.xsd) | default |
| [`examples/pubmed_article`](../tests/data/xsd/examples/pubmed_article/case.yaml) | `PubmedArticle` | [`pubmed.xsd`](../tests/data/pubmed/pubmed.xsd) | minimal, maximal |
| [`examples/pubmed_book_article`](../tests/data/xsd/examples/pubmed_book_article/case.yaml) | `PubmedBookArticle` | [`pubmed.xsd`](../tests/data/pubmed/pubmed.xsd) | minimal, maximal |
| [`examples/uniprot`](../tests/data/xsd/examples/uniprot/case.yaml) | `entry` | [`uniprot.xsd`](../tests/data/uniprot/uniprot_kb/uniprot.xsd) | minimal, maximal |
| [`examples/uniref`](../tests/data/xsd/examples/uniref/case.yaml) | `entry` | [`uniref.xsd`](../tests/data/uniprot/uniref/uniref.xsd) | minimal, maximal |
