"""Expected PyArrow tables for pubmed 01_multi_table_ingest."""

import pyarrow as pa

from cdm_data_loaders.schemas.pubmed.schema import (
    DELETE_CITATION_SCHEMA,
    PUBMED_ARTICLE_SCHEMA,
    PUBMED_BOOK_ARTICLE_SCHEMA,
)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "pubmed_article": pa.Table.from_pylist(
        [
            {
                "MedlineCitation": {
                    "_Status": "MEDLINE",
                    "PMID": {"_Version": "1", "#text": "38123456"},
                    "Article": {
                        "_PubModel": "Print-Electronic",
                        "Journal": {
                            "JournalIssue": {
                                "_CitedMedium": "Internet",
                                "PubDate": {"MedlineDate": "Sample_MedlineDate_1"},
                            }
                        },
                        "ArticleTitle": {
                            "#text": "Structural basis of <i>TP53</i> tetramerization and Ca<sup>2+</sup> signaling"
                        },
                        "ELocationID": [{"_EIdType": "doi", "#text": "10.1038/s41586-024-07123-x"}],
                        "Language": ["eng"],
                        "PublicationTypeList": {
                            "PublicationType": [{"_UI": "D016428", "#text": "Journal Article"}]
                        },
                    },
                    "MedlineJournalInfo": {"MedlineTA": "Nature"},
                }
            }
        ],
        schema=PUBMED_ARTICLE_SCHEMA,
    ),
    "pubmed_book_article": pa.Table.from_pylist(
        [
            {
                "BookDocument": {
                    "PMID": {"_Version": "1", "#text": "38123457"},
                    "ArticleIdList": {"ArticleId": [{"#text": "38123457"}]},
                    "Book": {
                        "Publisher": {"PublisherName": "Cold Spring Harbor Laboratory Press"},
                        "BookTitle": {"#text": "Molecular Biology of the Cell, <i>7th Edition</i>"},
                        "PubDate": {"MedlineDate": "Sample_MedlineDate_1"},
                    },
                }
            }
        ],
        schema=PUBMED_BOOK_ARTICLE_SCHEMA,
    ),
    "delete_citation": pa.Table.from_pylist(
        [
            {
                "PMID": [{"_Version": "1", "#text": "99999999"}],
            }
        ],
        schema=DELETE_CITATION_SCHEMA,
    ),
}
