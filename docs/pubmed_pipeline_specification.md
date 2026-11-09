<!-- AUTO-GENERATED from specs/pipelines/pubmed by scripts/gen_pipeline_spec.py; do not edit by hand. Run `uv run python -m scripts.gen_pipeline_spec` to regenerate. -->

---

# PubMed Pipeline Specification

This document specifies the end-to-end contract for the `pubmed` pipeline
(`cdm_data_loaders.pipelines.xmltopyarrow.pipeline:pubmed_cli`, configured via
`PubMedXmlToPyArrowSettings`):

1. **Input discovery and MD5 sidecar validation** (`*.xml.gz` and `<file>.xml.gz.md5` via `validate_md5_sidecar`).
2. **Level-1 XML element routing** under `<PubmedArticleSet>` into `pubmed_article`,
   `pubmed_book_article`, and `delete_citation` using precomputed PyArrow schemas (`PUBMED_SCHEMAS`).
3. **Error routing and pipeline report persistence** (`_rejected`, `_load_info`, and `report.txt`).

---

## Input Discovery and MD5 Sidecar Pre-Processing

- **`R-PUBMED-GLOB`** — The pipeline scans `input_dir` for files matching `file_glob` (default `*.xml.gz`).
Accompanying `.md5` sidecar files are not matched as standalone XML inputs. *(tested in [`01_multi_table_ingest`](#case-pubmed-01-multi-table-ingest), [`02_invalid_md5_sidecar`](#case-pubmed-02-invalid-md5-sidecar))*
- **`R-PUBMED-MD5-PASS`** — Before opening an `.xml.gz` file for XML parsing, `validate_md5_sidecar` reads `<file>.md5`,
computes the MD5 digest of the compressed `.xml.gz` bytes, and verifies they match. Validated
files proceed to XML extraction and are listed under `ingested_files` in `report.txt`. *(tested in [`01_multi_table_ingest`](#case-pubmed-01-multi-table-ingest))*
- **`R-PUBMED-MD5-FAIL`** — When `<file>.md5` is missing, malformed, or does not match the `.xml.gz` MD5 digest,
`validate_md5_sidecar` raises `FilePreprocessError`. The pipeline skips XML parsing for that file,
emits one provenance row to `_rejected` (`table_name=None`, `source_file=<relative_path>`,
`line_no=None`, `raw_record=None`, `error_detail=<message>`), and records the error under
`load_errors` in `report.txt` with `status: completed with errors`. *(tested in [`02_invalid_md5_sidecar`](#case-pubmed-02-invalid-md5-sidecar))*

---

## Level-1 XML Element Routing and Precomputed Schemas

- **`R-PUBMED-LEVEL1`** — With `root_tag="PubmedArticleSet"` and `xml_level=1`, the streaming parser extracts direct child
elements of `<PubmedArticleSet>` (`<PubmedArticle>`, `<PubmedBookArticle>`, and `<DeleteCitation>`)
and routes each element tag to its normalized destination table (`pubmed_article`,
`pubmed_book_article`, and `delete_citation`). *(tested in [`01_multi_table_ingest`](#case-pubmed-01-multi-table-ingest))*
- **`R-PUBMED-SCHEMA`** — Extracted element rows are coerced against the precomputed PyArrow schemas in
`cdm_data_loaders.schemas.pubmed.schema.SCHEMAS` (`PUBMED_ARTICLE_SCHEMA`,
`PUBMED_BOOK_ARTICLE_SCHEMA`, and `DELETE_CITATION_SCHEMA`), preserving nested structs, lists,
and raw inner XML strings on mixed-content fields (`ArticleTitle`, `BookTitle`, `AbstractText`). *(tested in [`01_multi_table_ingest`](#case-pubmed-01-multi-table-ingest))*

---

## Pipeline Metadata and Load Report

- **`R-PUBMED-REPORT`** — Every completed run persists dlt load metadata to `_load_info` in the destination dataset and writes
`report.txt` containing `pipeline: pubmed`, `dataset: pubmed`, `started`, `finished`, `status`,
`load_ids`, and `pipeline_load_info_errs`. *(tested in [`01_multi_table_ingest`](#case-pubmed-01-multi-table-ingest), [`02_invalid_md5_sidecar`](#case-pubmed-02-invalid-md5-sidecar))*

---

## Smoke Test Cases

<a id="case-pubmed-01-multi-table-ingest"></a>
### `01_multi_table_ingest` — Multi-Table PubMed Ingest (`PubmedArticle`, `PubmedBookArticle`, `DeleteCitation`)

**Exercises:** `R-PUBMED-GLOB`, `R-PUBMED-MD5-PASS`, `R-PUBMED-LEVEL1`, `R-PUBMED-SCHEMA`, `R-PUBMED-REPORT`

A single gzip-compressed `<PubmedArticleSet>` XML file accompanied by a matching `.xml.gz.md5`
sidecar file containing one `<PubmedArticle>`, one `<PubmedBookArticle>`, and one `<DeleteCitation>`.
Validates MD5 pre-processing, level-1 multi-table routing, mixed-content preservation, and report output.

**Settings (`settings.toml`)**

```toml
loader_file_format = "parquet"
use_output_dir_for_pipeline_metadata = true
```

**Input (`input/pubmed_sample.xml.gz` (decompressed))**

```xml
<?xml version="1.0" encoding="utf-8"?>
<PubmedArticleSet xmlns="http://www.w3.org/namespace/">
  <PubmedArticle>
    <MedlineCitation Status="MEDLINE">
      <PMID Version="1">38123456</PMID>
      <Article PubModel="Print-Electronic">
        <Journal>
          <JournalIssue CitedMedium="Internet">
            <PubDate>
              <MedlineDate>Sample_MedlineDate_1</MedlineDate>
            </PubDate>
          </JournalIssue>
        </Journal>
        <ArticleTitle>Structural basis of <i>TP53</i> tetramerization and Ca<sup>2+</sup> signaling</ArticleTitle>
        <ELocationID EIdType="doi">10.1038/s41586-024-07123-x</ELocationID>
        <Language>eng</Language>
        <PublicationTypeList>
          <PublicationType UI="D016428">Journal Article</PublicationType>
        </PublicationTypeList>
      </Article>
      <MedlineJournalInfo>
        <MedlineTA>Nature</MedlineTA>
      </MedlineJournalInfo>
    </MedlineCitation>
  </PubmedArticle>
  <PubmedBookArticle>
    <BookDocument>
      <PMID Version="1">38123457</PMID>
      <ArticleIdList>
        <ArticleId>38123457</ArticleId>
      </ArticleIdList>
      <Book>
        <Publisher>
          <PublisherName>Cold Spring Harbor Laboratory Press</PublisherName>
        </Publisher>
        <BookTitle>Molecular Biology of the Cell, <i>7th Edition</i></BookTitle>
        <PubDate>
          <MedlineDate>Sample_MedlineDate_1</MedlineDate>
        </PubDate>
      </Book>
    </BookDocument>
  </PubmedBookArticle>
  <DeleteCitation>
    <PMID Version="1">99999999</PMID>
  </DeleteCitation>
</PubmedArticleSet>
```

**Input (`input/pubmed_sample.xml.gz.md5`)**

```text
MD5(pubmed_sample.xml.gz)= 0db850c2bce102b7757053a174fed330
```

**Expected Report (`report.txt` summary)**

```yaml
status: completed
pipeline_load_info_errs: null
ingested_files:
- pubmed_sample.xml.gz
```

**Expected PyArrow Tables (`expected_tables.py`)**

```python
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
```

**Expected JSON (`expected.json`, non-null fields)**

```json
{
  "delete_citation": [
    {
      "pmid": [
        {
          "#text": "99999999",
          "_Version": "1"
        }
      ]
    }
  ],
  "pubmed_article": [
    {
      "medline_citation": {
        "Article": {
          "ArticleTitle": {
            "#text": "Structural basis of <i>TP53</i> tetramerization and Ca<sup>2+</sup> signaling"
          },
          "ELocationID": [
            {
              "#text": "10.1038/s41586-024-07123-x",
              "_EIdType": "doi"
            }
          ],
          "Journal": {
            "JournalIssue": {
              "PubDate": {
                "MedlineDate": "Sample_MedlineDate_1"
              },
              "_CitedMedium": "Internet"
            }
          },
          "Language": [
            "eng"
          ],
          "PublicationTypeList": {
            "PublicationType": [
              {
                "#text": "Journal Article",
                "_UI": "D016428"
              }
            ]
          },
          "_PubModel": "Print-Electronic"
        },
        "MedlineJournalInfo": {
          "MedlineTA": "Nature"
        },
        "PMID": {
          "#text": "38123456",
          "_Version": "1"
        },
        "_Status": "MEDLINE"
      }
    }
  ],
  "pubmed_book_article": [
    {
      "book_document": {
        "ArticleIdList": {
          "ArticleId": [
            {
              "#text": "38123457"
            }
          ]
        },
        "Book": {
          "BookTitle": {
            "#text": "Molecular Biology of the Cell, <i>7th Edition</i>"
          },
          "PubDate": {
            "MedlineDate": "Sample_MedlineDate_1"
          },
          "Publisher": {
            "PublisherName": "Cold Spring Harbor Laboratory Press"
          }
        },
        "PMID": {
          "#text": "38123457",
          "_Version": "1"
        }
      }
    }
  ]
}
```

---

<a id="case-pubmed-02-invalid-md5-sidecar"></a>
### `02_invalid_md5_sidecar` — Invalid MD5 Sidecar Rejection (`_rejected` and `load_errors`)

**Exercises:** `R-PUBMED-GLOB`, `R-PUBMED-MD5-FAIL`, `R-PUBMED-REPORT`

An `.xml.gz` file whose accompanying `.xml.gz.md5` sidecar contains a mismatched digest.
`validate_md5_sidecar` rejects the file before XML parsing, writes a file-level provenance
record to `_rejected`, and marks `report.txt` as `completed with errors`.

**Settings (`settings.toml`)**

```toml
loader_file_format = "parquet"
use_output_dir_for_pipeline_metadata = true
```

**Input (`input/corrupt_sample.xml.gz` (decompressed))**

```xml
<?xml version="1.0" encoding="utf-8"?>
<PubmedArticleSet xmlns="http://www.w3.org/namespace/">
  <PubmedArticle>
    <MedlineCitation Status="MEDLINE">
      <PMID Version="1">38123456</PMID>
      <Article PubModel="Print-Electronic">
        <Journal>
          <JournalIssue CitedMedium="Internet">
            <PubDate>
              <MedlineDate>Sample_MedlineDate_1</MedlineDate>
            </PubDate>
          </JournalIssue>
        </Journal>
        <ArticleTitle>Structural basis of <i>TP53</i> tetramerization and Ca<sup>2+</sup> signaling</ArticleTitle>
        <ELocationID EIdType="doi">10.1038/s41586-024-07123-x</ELocationID>
        <Language>eng</Language>
        <PublicationTypeList>
          <PublicationType UI="D016428">Journal Article</PublicationType>
        </PublicationTypeList>
      </Article>
      <MedlineJournalInfo>
        <MedlineTA>Nature</MedlineTA>
      </MedlineJournalInfo>
    </MedlineCitation>
  </PubmedArticle>
  <PubmedBookArticle>
    <BookDocument>
      <PMID Version="1">38123457</PMID>
      <ArticleIdList>
        <ArticleId>38123457</ArticleId>
      </ArticleIdList>
      <Book>
        <Publisher>
          <PublisherName>Cold Spring Harbor Laboratory Press</PublisherName>
        </Publisher>
        <BookTitle>Molecular Biology of the Cell, <i>7th Edition</i></BookTitle>
        <PubDate>
          <MedlineDate>Sample_MedlineDate_1</MedlineDate>
        </PubDate>
      </Book>
    </BookDocument>
  </PubmedBookArticle>
  <DeleteCitation>
    <PMID Version="1">99999999</PMID>
  </DeleteCitation>
</PubmedArticleSet>
```

**Input (`input/corrupt_sample.xml.gz.md5`)**

```text
MD5(corrupt_sample.xml.gz)= 00000000000000000000000000000000
```

**Expected Report (`report.txt` summary)**

```yaml
status: completed with errors
pipeline_load_info_errs: null
load_errors:
- 'File pre-processing failed for corrupt_sample.xml.gz: MD5 mismatch for corrupt_sample.xml.gz'
```

**Expected PyArrow Tables (`expected_tables.py`)**

```python
"""Expected PyArrow tables for pubmed 02_invalid_md5_sidecar."""

import pyarrow as pa

from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA

EXPECTED_TABLES: dict[str, pa.Table] = {
    "_rejected": pa.Table.from_pylist(
        [
            {
                "table_name": None,
                "source_file": "corrupt_sample.xml.gz",
                "line_no": None,
                "raw_record": None,
                "error_detail": (
                    "MD5 mismatch for corrupt_sample.xml.gz: "
                    "expected 00000000000000000000000000000000, got 0db850c2bce102b7757053a174fed330"
                ),
            }
        ],
        schema=REJECTED_PYARROW_SCHEMA,
    ),
}
```

**Expected JSON (`expected.json`, non-null fields)**

```json
{
  "_rejected": [
    {
      "error_detail": "MD5 mismatch for corrupt_sample.xml.gz: expected 00000000000000000000000000000000, got 0db850c2bce102b7757053a174fed330",
      "source_file": "corrupt_sample.xml.gz"
    }
  ]
}
```
