"""Expected PyArrow table for 03_mixed_content_raw_text (xml_tag='article')."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "title": "Ca<sup>2+</sup> signaling in <i>E. <b>coli</b></i> cells",
            "abstractText": [
                {"_Label": "BACKGROUND", "#text": "Studied <b>bold</b> &amp; <i>italic</i> text where x &lt; y."},
                {"#text": "Unlabelled paragraph with <sub>2</sub>O and empty <sup/> tag."},
                {"_Label": "EMPTY"},
                None,
            ],
        },
        {"title": ""},
    ],
    schema=EXPECTED_SCHEMA,
)
