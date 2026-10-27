"""Expected PyArrow table for pubmed_article minimal.xml (xml_tag='PubmedArticle')."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "MedlineCitation": {
                "_Status": "MEDLINE",
                "PMID": {"_Version": "1", "#text": "38123456"},
                "Article": {
                    "_PubModel": "Print-Electronic",
                    "Journal": {
                        "JournalIssue": {"_CitedMedium": "Internet", "PubDate": {"MedlineDate": "Sample_MedlineDate_1"}}
                    },
                    "ArticleTitle": {
                        "#text": "Structural basis of <i>TP53</i> tetramerization and Ca<sup>2+</sup> signaling"
                    },
                    "ELocationID": [{"_EIdType": "doi", "#text": "10.1038/s41586-024-07123-x"}],
                    "Language": ["eng"],
                    "PublicationTypeList": {"PublicationType": [{"_UI": "D016428", "#text": "Journal Article"}]},
                },
                "MedlineJournalInfo": {"MedlineTA": "Nature"},
            }
        },
    ],
    schema=EXPECTED_SCHEMA,
)
