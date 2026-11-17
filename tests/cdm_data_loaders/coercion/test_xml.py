"""Unit tests for XML-to-Arrow record coercion helpers."""

from pathlib import Path
from typing import Final

import pyarrow as pa
from lxml.etree import fromstring

from cdm_data_loaders.coercion.xml import make_xml_level_converters
from cdm_data_loaders.schemas.uniprot.schema import SCHEMAS as UNIPROT_SCHEMAS
from cdm_data_loaders.validation.xsd.traversal import load_schema
from tests._support.paths_and_constants import BASE_DIR

XSD_DATA_DIR: Final[Path] = (BASE_DIR / "specs" / "schema_converters" / "xsd_to_pyarrow").resolve()


def test_make_xml_level_converters_pass_struct_and_leaf_elements() -> None:
    """make_xml_level_converters converts both complex struct elements and scalar leaf elements at a level."""
    uniprot_xsd = load_schema(XSD_DATA_DIR / "examples" / "uniprot" / "uniprot.xsd")
    converters = make_xml_level_converters(uniprot_xsd, 1)
    assert set(converters.keys()) == {"entry", "copyright"}

    copyright_elem = fromstring(
        b'<copyright xmlns="https://uniprot.org/uniprot">Copyrighted by the UniProt Consortium</copyright>'
    )
    assert converters["copyright"](copyright_elem) == {"#text": "Copyrighted by the UniProt Consortium"}
    table = pa.Table.from_pylist([converters["copyright"](copyright_elem)], schema=UNIPROT_SCHEMAS["copyright"])
    assert table.to_pylist() == [{"#text": "Copyrighted by the UniProt Consortium"}]
