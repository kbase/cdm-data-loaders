"""PyArrow schemas and reusable struct components for PubMed XML."""

from typing import Final

import pyarrow as pa

from cdm_data_loaders.converters.emitters.pyarrow import assemble_pyarrow_schema

PMID_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_Version", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

DATE_COMPLETED_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("Year", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Month", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Day", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
    ]
)

DATE_REVISED_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("Year", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Month", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Day", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
    ]
)

ISSN_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_IssnType", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

PUB_DATE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("Year", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Month", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Day", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Season", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("MedlineDate", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

JOURNAL_ISSUE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_CitedMedium", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Volume", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Issue", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("PubDate", PUB_DATE_STRUCT, nullable=False),
    ]
)

JOURNAL_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("ISSN", ISSN_STRUCT, nullable=True),
        pa.field("JournalIssue", JOURNAL_ISSUE_STRUCT, nullable=False),
        pa.field("Title", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("ISOAbbreviation", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

ARTICLE_TITLE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_book", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_part", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_sec", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

PAGINATION_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("StartPage", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("EndPage", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("MedlinePgn", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

E_LOCATION_ID_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_EIdType", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("_ValidYN", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

ABSTRACT_TEXT_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_Label", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_NlmCategory", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

ABSTRACT_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("AbstractText", pa.list_(pa.field("item", ABSTRACT_TEXT_STRUCT, nullable=True)), nullable=False),
        pa.field("CopyrightInformation", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

COLLECTIVE_NAME_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_Investigators", pa.string(), nullable=True, metadata={"xsd_type": "xs:IDREF"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

IDENTIFIER_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_Source", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

AFFILIATION_INFO_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("Affiliation", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Identifier", pa.list_(pa.field("item", IDENTIFIER_STRUCT, nullable=True)), nullable=True),
    ]
)

AUTHOR_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_ValidYN", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_EqualContrib", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("LastName", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("ForeName", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Initials", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Suffix", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("CollectiveName", COLLECTIVE_NAME_STRUCT, nullable=True),
        pa.field("Identifier", pa.list_(pa.field("item", IDENTIFIER_STRUCT, nullable=True)), nullable=True),
        pa.field("AffiliationInfo", pa.list_(pa.field("item", AFFILIATION_INFO_STRUCT, nullable=True)), nullable=True),
    ]
)

AUTHOR_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_CompleteYN", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_Type", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Author", pa.list_(pa.field("item", AUTHOR_STRUCT, nullable=True)), nullable=False),
    ]
)

ACCESSION_NUMBER_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "AccessionNumber",
            pa.list_(pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"})),
            nullable=False,
        )
    ]
)

DATA_BANK_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("DataBankName", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("AccessionNumberList", ACCESSION_NUMBER_LIST_STRUCT, nullable=True),
    ]
)

DATA_BANK_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_CompleteYN", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("DataBank", pa.list_(pa.field("item", DATA_BANK_STRUCT, nullable=True)), nullable=False),
    ]
)

GRANT_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("GrantID", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Acronym", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Agency", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Country", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

GRANT_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_CompleteYN", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Grant", pa.list_(pa.field("item", GRANT_STRUCT, nullable=True)), nullable=False),
    ]
)

PUBLICATION_TYPE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_UI", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

PUBLICATION_TYPE_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("PublicationType", pa.list_(pa.field("item", PUBLICATION_TYPE_STRUCT, nullable=True)), nullable=False)]
)

ARTICLE_DATE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_DateType", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Year", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Month", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Day", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
    ]
)

ARTICLE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_PubModel", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Journal", JOURNAL_STRUCT, nullable=False),
        pa.field("ArticleTitle", ARTICLE_TITLE_STRUCT, nullable=False),
        pa.field("Pagination", PAGINATION_STRUCT, nullable=True),
        pa.field("ELocationID", pa.list_(pa.field("item", E_LOCATION_ID_STRUCT, nullable=True)), nullable=True),
        pa.field("Abstract", ABSTRACT_STRUCT, nullable=True),
        pa.field("AuthorList", AUTHOR_LIST_STRUCT, nullable=True),
        pa.field(
            "Language",
            pa.list_(pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"})),
            nullable=False,
        ),
        pa.field("DataBankList", DATA_BANK_LIST_STRUCT, nullable=True),
        pa.field("GrantList", GRANT_LIST_STRUCT, nullable=True),
        pa.field("PublicationTypeList", PUBLICATION_TYPE_LIST_STRUCT, nullable=False),
        pa.field("VernacularTitle", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("ArticleDate", pa.list_(pa.field("item", ARTICLE_DATE_STRUCT, nullable=True)), nullable=True),
    ]
)

MEDLINE_JOURNAL_INFO_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("Country", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("MedlineTA", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("NlmUniqueID", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("ISSNLinking", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

NAME_OF_SUBSTANCE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_UI", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

CHEMICAL_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("RegistryNumber", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("NameOfSubstance", NAME_OF_SUBSTANCE_STRUCT, nullable=False),
    ]
)

CHEMICAL_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("Chemical", pa.list_(pa.field("item", CHEMICAL_STRUCT, nullable=True)), nullable=False)]
)

SUPPL_MESH_NAME_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_Type", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("_UI", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

SUPPL_MESH_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("SupplMeshName", pa.list_(pa.field("item", SUPPL_MESH_NAME_STRUCT, nullable=True)), nullable=False)]
)

COMMENTS_CORRECTIONS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_RefType", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("RefSource", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("PMID", PMID_STRUCT, nullable=True),
        pa.field("Note", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

COMMENTS_CORRECTIONS_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "CommentsCorrections",
            pa.list_(pa.field("item", COMMENTS_CORRECTIONS_STRUCT, nullable=True)),
            nullable=False,
        )
    ]
)

GENE_SYMBOL_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "GeneSymbol",
            pa.list_(pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"})),
            nullable=False,
        )
    ]
)

DESCRIPTOR_NAME_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_MajorTopicYN", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_AutoHM", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_Type", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_UI", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

QUALIFIER_NAME_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_AutoHM", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_MajorTopicYN", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_UI", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

MESH_HEADING_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("DescriptorName", DESCRIPTOR_NAME_STRUCT, nullable=False),
        pa.field("QualifierName", pa.list_(pa.field("item", QUALIFIER_NAME_STRUCT, nullable=True)), nullable=True),
    ]
)

MESH_HEADING_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("MeshHeading", pa.list_(pa.field("item", MESH_HEADING_STRUCT, nullable=True)), nullable=False)]
)

PERSONAL_NAME_SUBJECT_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("LastName", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("ForeName", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Initials", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Suffix", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

PERSONAL_NAME_SUBJECT_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "PersonalNameSubject",
            pa.list_(pa.field("item", PERSONAL_NAME_SUBJECT_STRUCT, nullable=True)),
            nullable=False,
        )
    ]
)

OTHER_ID_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_Source", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

OTHER_ABSTRACT_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_Type", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("_Language", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("AbstractText", pa.list_(pa.field("item", ABSTRACT_TEXT_STRUCT, nullable=True)), nullable=False),
        pa.field("CopyrightInformation", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

KEYWORD_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_MajorTopicYN", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

KEYWORD_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_Owner", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Keyword", pa.list_(pa.field("item", KEYWORD_STRUCT, nullable=True)), nullable=False),
    ]
)

INVESTIGATOR_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_ValidYN", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("LastName", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("ForeName", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Initials", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Suffix", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Identifier", pa.list_(pa.field("item", IDENTIFIER_STRUCT, nullable=True)), nullable=True),
        pa.field("AffiliationInfo", pa.list_(pa.field("item", AFFILIATION_INFO_STRUCT, nullable=True)), nullable=True),
    ]
)

INVESTIGATOR_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_ID", pa.string(), nullable=True, metadata={"xsd_type": "xs:ID"}),
        pa.field("Investigator", pa.list_(pa.field("item", INVESTIGATOR_STRUCT, nullable=True)), nullable=False),
    ]
)

GENERAL_NOTE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_Owner", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

MEDLINE_CITATION_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_Owner", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_Status", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("_VersionID", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_VersionDate", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_IndexingMethod", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("PMID", PMID_STRUCT, nullable=False),
        pa.field("DateCompleted", DATE_COMPLETED_STRUCT, nullable=True),
        pa.field("DateRevised", DATE_REVISED_STRUCT, nullable=True),
        pa.field("Article", ARTICLE_STRUCT, nullable=False),
        pa.field("MedlineJournalInfo", MEDLINE_JOURNAL_INFO_STRUCT, nullable=False),
        pa.field("ChemicalList", CHEMICAL_LIST_STRUCT, nullable=True),
        pa.field("SupplMeshList", SUPPL_MESH_LIST_STRUCT, nullable=True),
        pa.field(
            "CitationSubset",
            pa.list_(pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"})),
            nullable=True,
        ),
        pa.field("CommentsCorrectionsList", COMMENTS_CORRECTIONS_LIST_STRUCT, nullable=True),
        pa.field("GeneSymbolList", GENE_SYMBOL_LIST_STRUCT, nullable=True),
        pa.field("MeshHeadingList", MESH_HEADING_LIST_STRUCT, nullable=True),
        pa.field("NumberOfReferences", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("PersonalNameSubjectList", PERSONAL_NAME_SUBJECT_LIST_STRUCT, nullable=True),
        pa.field("OtherID", pa.list_(pa.field("item", OTHER_ID_STRUCT, nullable=True)), nullable=True),
        pa.field("OtherAbstract", pa.list_(pa.field("item", OTHER_ABSTRACT_STRUCT, nullable=True)), nullable=True),
        pa.field("KeywordList", pa.list_(pa.field("item", KEYWORD_LIST_STRUCT, nullable=True)), nullable=True),
        pa.field("CoiStatement", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "SpaceFlightMission",
            pa.list_(pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"})),
            nullable=True,
        ),
        pa.field(
            "InvestigatorList", pa.list_(pa.field("item", INVESTIGATOR_LIST_STRUCT, nullable=True)), nullable=True
        ),
        pa.field("GeneralNote", pa.list_(pa.field("item", GENERAL_NOTE_STRUCT, nullable=True)), nullable=True),
    ]
)

PUB_MED_PUB_DATE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_PubStatus", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Year", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Month", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Day", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Hour", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Minute", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Second", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

HISTORY_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("PubMedPubDate", pa.list_(pa.field("item", PUB_MED_PUB_DATE_STRUCT, nullable=True)), nullable=False)]
)

ARTICLE_ID_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_IdType", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

ARTICLE_ID_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("ArticleId", pa.list_(pa.field("item", ARTICLE_ID_STRUCT, nullable=True)), nullable=False)]
)

PARAM_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_Name", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

OBJECT_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_Type", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Param", pa.list_(pa.field("item", PARAM_STRUCT, nullable=True)), nullable=True),
    ]
)

OBJECT_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("Object", pa.list_(pa.field("item", OBJECT_STRUCT, nullable=True)), nullable=False)]
)

REFERENCE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("Citation", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("ArticleIdList", ARTICLE_ID_LIST_STRUCT, nullable=True),
    ]
)

REFERENCE_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("Title", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Reference", pa.list_(pa.field("item", REFERENCE_STRUCT, nullable=True)), nullable=True),
        pa.field(
            "ReferenceList",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field("Title", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
                            pa.field(
                                "Reference", pa.list_(pa.field("item", REFERENCE_STRUCT, nullable=True)), nullable=True
                            ),
                            pa.field(
                                "ReferenceList", pa.list_(pa.field("item", pa.string(), nullable=True)), nullable=True
                            ),
                        ]
                    ),
                    nullable=True,
                )
            ),
            nullable=True,
        ),
    ]
)

PUBMED_DATA_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("History", HISTORY_STRUCT, nullable=True),
        pa.field("PublicationStatus", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("ArticleIdList", ARTICLE_ID_LIST_STRUCT, nullable=False),
        pa.field("ObjectList", OBJECT_LIST_STRUCT, nullable=True),
        pa.field("ReferenceList", pa.list_(pa.field("item", REFERENCE_LIST_STRUCT, nullable=True)), nullable=True),
    ]
)

PUBMED_ARTICLE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("MedlineCitation", MEDLINE_CITATION_STRUCT, nullable=False),
        pa.field("PubmedData", PUBMED_DATA_STRUCT, nullable=True),
    ]
)

PUBLISHER_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("PublisherName", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("PublisherLocation", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

BOOK_TITLE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_book", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_part", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_sec", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

BEGINNING_DATE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("Year", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Month", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Day", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Season", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

ENDING_DATE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("Year", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Month", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Day", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Season", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

COLLECTION_TITLE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_book", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_part", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_sec", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

BOOK_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("Publisher", PUBLISHER_STRUCT, nullable=False),
        pa.field("BookTitle", BOOK_TITLE_STRUCT, nullable=False),
        pa.field("PubDate", PUB_DATE_STRUCT, nullable=False),
        pa.field("BeginningDate", BEGINNING_DATE_STRUCT, nullable=True),
        pa.field("EndingDate", ENDING_DATE_STRUCT, nullable=True),
        pa.field("AuthorList", pa.list_(pa.field("item", AUTHOR_LIST_STRUCT, nullable=True)), nullable=True),
        pa.field("InvestigatorList", INVESTIGATOR_LIST_STRUCT, nullable=True),
        pa.field("Volume", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("VolumeTitle", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Edition", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("CollectionTitle", COLLECTION_TITLE_STRUCT, nullable=True),
        pa.field(
            "Isbn",
            pa.list_(pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"})),
            nullable=True,
        ),
        pa.field("ELocationID", pa.list_(pa.field("item", E_LOCATION_ID_STRUCT, nullable=True)), nullable=True),
        pa.field("Medium", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("ReportNumber", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

LOCATION_LABEL_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_Type", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

SECTION_TITLE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_book", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_part", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_sec", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

SECTION_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("LocationLabel", LOCATION_LABEL_STRUCT, nullable=True),
        pa.field("SectionTitle", SECTION_TITLE_STRUCT, nullable=False),
        pa.field(
            "Section",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field("LocationLabel", LOCATION_LABEL_STRUCT, nullable=True),
                            pa.field("SectionTitle", SECTION_TITLE_STRUCT, nullable=False),
                            pa.field("Section", pa.list_(pa.field("item", pa.string(), nullable=True)), nullable=True),
                        ]
                    ),
                    nullable=True,
                )
            ),
            nullable=True,
        ),
    ]
)

SECTIONS_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("Section", pa.list_(pa.field("item", SECTION_STRUCT, nullable=True)), nullable=False)]
)

CONTRIBUTION_DATE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("Year", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("Month", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Day", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Season", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

ITEM_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_ListType", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "Item",
            pa.list_(pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"})),
            nullable=False,
        ),
    ]
)

BOOK_DOCUMENT_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("PMID", PMID_STRUCT, nullable=False),
        pa.field("ArticleIdList", ARTICLE_ID_LIST_STRUCT, nullable=False),
        pa.field("Book", BOOK_STRUCT, nullable=False),
        pa.field("LocationLabel", pa.list_(pa.field("item", LOCATION_LABEL_STRUCT, nullable=True)), nullable=True),
        pa.field("ArticleTitle", ARTICLE_TITLE_STRUCT, nullable=True),
        pa.field("VernacularTitle", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("Pagination", PAGINATION_STRUCT, nullable=True),
        pa.field(
            "Language",
            pa.list_(pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"})),
            nullable=True,
        ),
        pa.field("AuthorList", pa.list_(pa.field("item", AUTHOR_LIST_STRUCT, nullable=True)), nullable=True),
        pa.field("InvestigatorList", INVESTIGATOR_LIST_STRUCT, nullable=True),
        pa.field("PublicationType", pa.list_(pa.field("item", PUBLICATION_TYPE_STRUCT, nullable=True)), nullable=True),
        pa.field("Abstract", ABSTRACT_STRUCT, nullable=True),
        pa.field("Sections", SECTIONS_STRUCT, nullable=True),
        pa.field("KeywordList", pa.list_(pa.field("item", KEYWORD_LIST_STRUCT, nullable=True)), nullable=True),
        pa.field("ContributionDate", CONTRIBUTION_DATE_STRUCT, nullable=True),
        pa.field("DateRevised", DATE_REVISED_STRUCT, nullable=True),
        pa.field("GrantList", GRANT_LIST_STRUCT, nullable=True),
        pa.field("ItemList", pa.list_(pa.field("item", ITEM_LIST_STRUCT, nullable=True)), nullable=True),
        pa.field("ReferenceList", pa.list_(pa.field("item", REFERENCE_LIST_STRUCT, nullable=True)), nullable=True),
    ]
)

PUBMED_BOOK_DATA_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("History", HISTORY_STRUCT, nullable=True),
        pa.field("PublicationStatus", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("ArticleIdList", ARTICLE_ID_LIST_STRUCT, nullable=False),
        pa.field("ObjectList", OBJECT_LIST_STRUCT, nullable=True),
    ]
)

PUBMED_BOOK_ARTICLE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("BookDocument", BOOK_DOCUMENT_STRUCT, nullable=False),
        pa.field("PubmedBookData", PUBMED_BOOK_DATA_STRUCT, nullable=True),
    ]
)

DELETE_CITATION_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("PMID", pa.list_(pa.field("item", PMID_STRUCT, nullable=True)), nullable=False)]
)

PUBMED_ARTICLE_SCHEMA: Final[pa.Schema] = assemble_pyarrow_schema(PUBMED_ARTICLE_STRUCT)
PUBMED_BOOK_ARTICLE_SCHEMA: Final[pa.Schema] = assemble_pyarrow_schema(PUBMED_BOOK_ARTICLE_STRUCT)
DELETE_CITATION_SCHEMA: Final[pa.Schema] = assemble_pyarrow_schema(DELETE_CITATION_STRUCT)
SCHEMAS: Final[dict[str, pa.Schema]] = {
    "PubmedArticle": PUBMED_ARTICLE_SCHEMA,
    "PubmedBookArticle": PUBMED_BOOK_ARTICLE_SCHEMA,
    "DeleteCitation": DELETE_CITATION_SCHEMA,
}
