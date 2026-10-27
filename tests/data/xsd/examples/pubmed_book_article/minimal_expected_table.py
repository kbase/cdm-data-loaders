"""Expected PyArrow table for pubmed_book_article minimal.xml (xml_tag='PubmedBookArticle')."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "BookDocument": {
                "PMID": {"_Version": "1", "#text": "38123456"},
                "ArticleIdList": {"ArticleId": [{"#text": "38123456"}]},
                "Book": {
                    "Publisher": {"PublisherName": "Cold Spring Harbor Laboratory Press"},
                    "BookTitle": {"#text": "Molecular Biology of the Cell, <i>7th Edition</i>"},
                    "PubDate": {"MedlineDate": "Sample_MedlineDate_1"},
                },
            }
        },
    ],
    schema=EXPECTED_SCHEMA,
)
