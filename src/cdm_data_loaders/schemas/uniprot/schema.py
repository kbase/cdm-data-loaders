"""PyArrow schemas and reusable struct components for UniProt XML."""

from typing import Final

import pyarrow as pa

from cdm_data_loaders.converters.emitters.pyarrow import assemble_pyarrow_schema

EVIDENCED_STRING_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

RECOMMENDED_NAME_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("fullName", EVIDENCED_STRING_STRUCT, nullable=False),
        pa.field("shortName", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
        pa.field("ecNumber", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
    ]
)

ALTERNATIVE_NAME_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("fullName", EVIDENCED_STRING_STRUCT, nullable=True),
        pa.field("shortName", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
        pa.field("ecNumber", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
    ]
)

SUBMITTED_NAME_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("fullName", EVIDENCED_STRING_STRUCT, nullable=False),
        pa.field("ecNumber", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
    ]
)

DOMAIN_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("recommendedName", RECOMMENDED_NAME_STRUCT, nullable=True),
        pa.field("alternativeName", pa.list_(pa.field("item", ALTERNATIVE_NAME_STRUCT, nullable=True)), nullable=True),
        pa.field("submittedName", pa.list_(pa.field("item", SUBMITTED_NAME_STRUCT, nullable=True)), nullable=True),
        pa.field("allergenName", EVIDENCED_STRING_STRUCT, nullable=True),
        pa.field("biotechName", EVIDENCED_STRING_STRUCT, nullable=True),
        pa.field("cdAntigenName", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
        pa.field("innName", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
    ]
)

COMPONENT_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("recommendedName", RECOMMENDED_NAME_STRUCT, nullable=True),
        pa.field("alternativeName", pa.list_(pa.field("item", ALTERNATIVE_NAME_STRUCT, nullable=True)), nullable=True),
        pa.field("submittedName", pa.list_(pa.field("item", SUBMITTED_NAME_STRUCT, nullable=True)), nullable=True),
        pa.field("allergenName", EVIDENCED_STRING_STRUCT, nullable=True),
        pa.field("biotechName", EVIDENCED_STRING_STRUCT, nullable=True),
        pa.field("cdAntigenName", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
        pa.field("innName", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
    ]
)

PROTEIN_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("recommendedName", RECOMMENDED_NAME_STRUCT, nullable=True),
        pa.field("alternativeName", pa.list_(pa.field("item", ALTERNATIVE_NAME_STRUCT, nullable=True)), nullable=True),
        pa.field("submittedName", pa.list_(pa.field("item", SUBMITTED_NAME_STRUCT, nullable=True)), nullable=True),
        pa.field("allergenName", EVIDENCED_STRING_STRUCT, nullable=True),
        pa.field("biotechName", EVIDENCED_STRING_STRUCT, nullable=True),
        pa.field("cdAntigenName", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
        pa.field("innName", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
        pa.field(
            "domain",
            pa.list_(pa.field("item", DOMAIN_STRUCT, nullable=True)),
            nullable=True,
            metadata={
                "comment": 'Describes names of "domains". Equivalent to the flat file DE-line Includes: section.'
            },
        ),
        pa.field(
            "component",
            pa.list_(pa.field("item", COMPONENT_STRUCT, nullable=True)),
            nullable=True,
            metadata={
                "comment": "Describes names of processed products. Equivalent to the flat file DE-line Contains: section."
            },
        ),
    ]
)

GENE_NAME_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
        pa.field("_type", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

GENE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "name",
            pa.list_(pa.field("item", GENE_NAME_STRUCT, nullable=True)),
            nullable=False,
            metadata={
                "comment": "Describes different types of gene designations. Equivalent to the flat file GN-line."
            },
        )
    ]
)

ORGANISM_NAME_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_type", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

MOLECULE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_id", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

PROPERTY_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_type", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("_value", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
    ]
)

DB_REFERENCE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_type",
            pa.string(),
            nullable=False,
            metadata={"comment": "Describes the name of the database.", "xsd_type": "xs:string"},
        ),
        pa.field(
            "_id",
            pa.string(),
            nullable=False,
            metadata={"comment": "Describes a unique database identifier.", "xsd_type": "xs:string"},
        ),
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
        pa.field(
            "molecule",
            MOLECULE_STRUCT,
            nullable=True,
            metadata={"comment": "Describes a molecule by name or unique identifier."},
        ),
        pa.field("property", pa.list_(pa.field("item", PROPERTY_STRUCT, nullable=True)), nullable=True),
    ]
)

LINEAGE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "taxon",
            pa.list_(pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"})),
            nullable=False,
        )
    ]
)

ORGANISM_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
        pa.field(
            "name",
            pa.list_(pa.field("item", ORGANISM_NAME_STRUCT, nullable=True)),
            nullable=False,
            metadata={"comment": "Describes the names of the source organism. Equivalent to the flat file OS-line."},
        ),
        pa.field(
            "dbReference",
            pa.list_(pa.field("item", DB_REFERENCE_STRUCT, nullable=True)),
            nullable=False,
            metadata={
                "comment": "Describes a cross-reference to the NCBI taxonomy database. Equivalent to the flat file OX-line."
            },
        ),
        pa.field(
            "lineage",
            LINEAGE_STRUCT,
            nullable=True,
            metadata={"comment": "Describes the lineage of the source organism. Equivalent to the flat file OC-line."},
        ),
    ]
)

STATUS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_status", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

GENE_LOCATION_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_type", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
        pa.field(
            "name",
            pa.list_(pa.field("item", STATUS_STRUCT, nullable=True)),
            nullable=True,
            metadata={"comment": "Indicates whether the name of a plasmid is known or unknown."},
        ),
    ]
)

CONSORTIUM_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("_name", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"})]
)

PERSON_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("_name", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"})]
)

NAME_LIST_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "consortium",
            pa.list_(pa.field("item", CONSORTIUM_STRUCT, nullable=True)),
            nullable=True,
            metadata={
                "comment": "Describes the authors of a citation when these are represented by a consortium. Equivalent to the flat file RG-line."
            },
        ),
        pa.field("person", pa.list_(pa.field("item", PERSON_STRUCT, nullable=True)), nullable=True),
    ]
)

CITATION_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_type",
            pa.string(),
            nullable=False,
            metadata={"comment": "Describes the type of a citation.", "xsd_type": "xs:string"},
        ),
        pa.field("_date", pa.string(), nullable=True, metadata={"xsd_type": "xs:date | xs:gYearMonth | xs:gYear"}),
        pa.field(
            "_name",
            pa.string(),
            nullable=True,
            metadata={"comment": "Describes the name of an (online) journal or book.", "xsd_type": "xs:string"},
        ),
        pa.field(
            "_volume",
            pa.string(),
            nullable=True,
            metadata={"comment": "Describes the volume of a journal or book.", "xsd_type": "xs:string"},
        ),
        pa.field(
            "_first",
            pa.string(),
            nullable=True,
            metadata={"comment": "Describes the first page of an article.", "xsd_type": "xs:string"},
        ),
        pa.field(
            "_last",
            pa.string(),
            nullable=True,
            metadata={"comment": "Describes the last page of an article.", "xsd_type": "xs:string"},
        ),
        pa.field(
            "_publisher",
            pa.string(),
            nullable=True,
            metadata={"comment": "Describes the publisher of a book.", "xsd_type": "xs:string"},
        ),
        pa.field(
            "_city",
            pa.string(),
            nullable=True,
            metadata={"comment": "Describes the city where a book was published.", "xsd_type": "xs:string"},
        ),
        pa.field(
            "_db",
            pa.string(),
            nullable=True,
            metadata={"comment": "Describes the database name of submissions.", "xsd_type": "xs:string"},
        ),
        pa.field(
            "_number",
            pa.string(),
            nullable=True,
            metadata={"comment": "Describes a patent number.", "xsd_type": "xs:string"},
        ),
        pa.field(
            "_institute",
            pa.string(),
            nullable=True,
            metadata={"comment": "Describes the institute where a thesis was made.", "xsd_type": "xs:string"},
        ),
        pa.field(
            "_country",
            pa.string(),
            nullable=True,
            metadata={"comment": "Describes the country where a thesis was made.", "xsd_type": "xs:string"},
        ),
        pa.field(
            "title",
            pa.string(),
            nullable=True,
            metadata={
                "comment": "Describes the title of a citation. Equivalent to the flat file RT-line.",
                "xsd_type": "xs:string",
            },
        ),
        pa.field(
            "editorList",
            NAME_LIST_STRUCT,
            nullable=True,
            metadata={
                "comment": "Describes the editors of a book (only used for books). Equivalent to part of the flat file RL-line of books."
            },
        ),
        pa.field(
            "authorList",
            NAME_LIST_STRUCT,
            nullable=True,
            metadata={"comment": "Describes the authors of a citation. Equivalent to the flat file RA-line."},
        ),
        pa.field(
            "locator",
            pa.string(),
            nullable=True,
            metadata={
                "comment": "Describes the location (URL) of an online journal article. No flat file equivalent.",
                "xsd_type": "xs:string",
            },
        ),
        pa.field(
            "dbReference",
            pa.list_(pa.field("item", DB_REFERENCE_STRUCT, nullable=True)),
            nullable=True,
            metadata={
                "comment": "Describes cross-references to bibliography databases (MEDLINE, PubMed, AGRICOLA) or other online resources (DOI). Equivalent to the flat file RX-line."
            },
        ),
    ]
)

STRAIN_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

PLASMID_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

TRANSPOSON_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

TISSUE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

SOURCE_DATA_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("strain", pa.list_(pa.field("item", STRAIN_STRUCT, nullable=True)), nullable=True),
        pa.field("plasmid", pa.list_(pa.field("item", PLASMID_STRUCT, nullable=True)), nullable=True),
        pa.field("transposon", pa.list_(pa.field("item", TRANSPOSON_STRUCT, nullable=True)), nullable=True),
        pa.field("tissue", pa.list_(pa.field("item", TISSUE_STRUCT, nullable=True)), nullable=True),
    ]
)

REFERENCE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
        pa.field("_key", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "citation",
            CITATION_STRUCT,
            nullable=False,
            metadata={
                "comment": "Describes different types of citations. Equivalent to the flat file RX-, RG-, RA-, RT- and RL-lines."
            },
        ),
        pa.field(
            "scope",
            pa.list_(pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"})),
            nullable=False,
            metadata={"comment": "Describes the scope of a citation. Equivalent to the flat file RP-line."},
        ),
        pa.field(
            "source",
            SOURCE_DATA_STRUCT,
            nullable=True,
            metadata={
                "comment": "Describes the source of the sequence according to the citation. Equivalent to the flat file RC-line."
            },
        ),
    ]
)

ABSORPTION_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("max", EVIDENCED_STRING_STRUCT, nullable=True),
        pa.field("text", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
    ]
)

KINETICS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("KM", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
        pa.field("Vmax", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
        pa.field("text", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
    ]
)

PH_DEPENDENCE_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("text", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=False)]
)

REDOX_POTENTIAL_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("text", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=False)]
)

TEMPERATURE_DEPENDENCE_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("text", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=False)]
)

REACTION_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
        pa.field("text", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "dbReference",
            pa.list_(pa.field("item", DB_REFERENCE_STRUCT, nullable=True)),
            nullable=False,
            metadata={"comment": "Describes a database cross-reference. Equivalent to the flat file DR-line."},
        ),
    ]
)

PHYSIOLOGICAL_REACTION_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_direction", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
        pa.field(
            "dbReference",
            DB_REFERENCE_STRUCT,
            nullable=False,
            metadata={"comment": "Describes a database cross-reference. Equivalent to the flat file DR-line."},
        ),
    ]
)

COFACTOR_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
        pa.field("name", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "dbReference",
            DB_REFERENCE_STRUCT,
            nullable=False,
            metadata={"comment": "Describes a database cross-reference. Equivalent to the flat file DR-line."},
        ),
    ]
)

SUBCELLULAR_LOCATION_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("location", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=False),
        pa.field("topology", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
        pa.field("orientation", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
    ]
)

CONFLICT_SEQUENCE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_resource", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("_id", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("_version", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"}),
    ]
)

CONFLICT_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_type", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "_ref",
            pa.string(),
            nullable=True,
            metadata={"comment": "Refers to the 'key' attribute of a 'reference' element.", "xsd_type": "xs:string"},
        ),
        pa.field("sequence", CONFLICT_SEQUENCE_STRUCT, nullable=True),
    ]
)

LINK_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("_uri", pa.string(), nullable=False, metadata={"xsd_type": "xs:anyURI"})]
)

EVENT_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("_type", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"})]
)

ISOFORM_NAME_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

ISOFORM_SEQUENCE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_type", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("_ref", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

ISOFORM_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "id",
            pa.list_(pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"})),
            nullable=False,
        ),
        pa.field("name", pa.list_(pa.field("item", ISOFORM_NAME_STRUCT, nullable=True)), nullable=False),
        pa.field("sequence", ISOFORM_SEQUENCE_STRUCT, nullable=False),
        pa.field("text", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
    ]
)

INTERACTANT_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_intactId", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("id", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("label", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "dbReference",
            DB_REFERENCE_STRUCT,
            nullable=True,
            metadata={"comment": "Describes a database cross-reference. Equivalent to the flat file DR-line."},
        ),
    ]
)

DISEASE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_id", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("name", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("acronym", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("description", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "dbReference",
            DB_REFERENCE_STRUCT,
            nullable=False,
            metadata={"comment": "Describes a database cross-reference. Equivalent to the flat file DR-line."},
        ),
    ]
)

POSITION_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_position", pa.uint64(), nullable=True, metadata={"xsd_type": "xs:unsignedLong"}),
        pa.field("_status", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
    ]
)

LOCATION_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_sequence", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("begin", POSITION_STRUCT, nullable=True),
        pa.field("end", POSITION_STRUCT, nullable=True),
        pa.field("position", POSITION_STRUCT, nullable=True),
    ]
)

COMMENT_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_type",
            pa.string(),
            nullable=True,
            metadata={
                "comment": 'Describes the type of a general annotation. Equivalent to the flat file CC comment topics (except for "DATABASE" which is translated to "online information").',
                "xsd_type": "xs:string",
            },
        ),
        pa.field(
            "_locationType",
            pa.string(),
            nullable=True,
            metadata={
                "comment": 'Describes the type of sequence location in \'RNA editing\' annotations. Common values are "Not_applicable" and "Undetermined".',
                "xsd_type": "xs:string",
            },
        ),
        pa.field(
            "_name",
            pa.string(),
            nullable=True,
            metadata={"comment": "Describes an optional name for an 'online information'.", "xsd_type": "xs:string"},
        ),
        pa.field(
            "_mass",
            pa.float64(),
            nullable=True,
            metadata={
                "comment": "Describes the molecular mass in 'mass spectrometry' annotations.",
                "xsd_type": "xs:float",
            },
        ),
        pa.field(
            "_error",
            pa.string(),
            nullable=True,
            metadata={
                "comment": "Describes the error of the mass measurement in 'mass spectrometry' annotations.",
                "xsd_type": "xs:string",
            },
        ),
        pa.field(
            "_method",
            pa.string(),
            nullable=True,
            metadata={
                "comment": "Describes the experimental method in 'mass spectrometry' annotations.",
                "xsd_type": "xs:string",
            },
        ),
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
        pa.field(
            "molecule",
            MOLECULE_STRUCT,
            nullable=True,
            metadata={"comment": "Describes a molecule by name or unique identifier."},
        ),
        pa.field("absorption", ABSORPTION_STRUCT, nullable=True),
        pa.field("kinetics", KINETICS_STRUCT, nullable=True),
        pa.field("phDependence", PH_DEPENDENCE_STRUCT, nullable=True),
        pa.field("redoxPotential", REDOX_POTENTIAL_STRUCT, nullable=True),
        pa.field("temperatureDependence", TEMPERATURE_DEPENDENCE_STRUCT, nullable=True),
        pa.field("reaction", REACTION_STRUCT, nullable=True, metadata={"comment": "Describes a chemical reaction."}),
        pa.field(
            "physiologicalReaction",
            pa.list_(pa.field("item", PHYSIOLOGICAL_REACTION_STRUCT, nullable=True)),
            nullable=True,
            metadata={"comment": "Describes a physiological reaction."},
        ),
        pa.field(
            "cofactor",
            pa.list_(pa.field("item", COFACTOR_STRUCT, nullable=True)),
            nullable=True,
            metadata={"comment": "Describes a cofactor."},
        ),
        pa.field(
            "subcellularLocation",
            pa.list_(pa.field("item", SUBCELLULAR_LOCATION_STRUCT, nullable=True)),
            nullable=True,
            metadata={
                "comment": "Describes the subcellular location and optionally the topology and orientation of a molecule."
            },
        ),
        pa.field(
            "conflict", CONFLICT_STRUCT, nullable=True, metadata={"comment": "Used in 'sequence caution' annotations."}
        ),
        pa.field(
            "link",
            pa.list_(pa.field("item", LINK_STRUCT, nullable=True)),
            nullable=True,
            metadata={"comment": "Used in 'online information' annotations."},
        ),
        pa.field(
            "event",
            pa.list_(pa.field("item", EVENT_STRUCT, nullable=True)),
            nullable=True,
            metadata={"comment": "Describes the type of events that cause alternative products."},
        ),
        pa.field(
            "isoform",
            pa.list_(pa.field("item", ISOFORM_STRUCT, nullable=True)),
            nullable=True,
            metadata={"comment": "Describes isoforms in 'alternative products' annotations."},
        ),
        pa.field("interactant", pa.list_(pa.field("item", INTERACTANT_STRUCT, nullable=True)), nullable=True),
        pa.field("organismsDiffer", pa.bool_(), nullable=True, metadata={"xsd_type": "xs:boolean"}),
        pa.field("experiments", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"}),
        pa.field("disease", DISEASE_STRUCT, nullable=True, metadata={"comment": "Used in 'disease' annotations."}),
        pa.field(
            "location",
            pa.list_(pa.field("item", LOCATION_STRUCT, nullable=True)),
            nullable=True,
            metadata={"comment": "Used in 'mass spectrometry' and 'sequence caution' annotations."},
        ),
        pa.field("text", pa.list_(pa.field("item", EVIDENCED_STRING_STRUCT, nullable=True)), nullable=True),
    ]
)

PROTEIN_EXISTENCE_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("_type", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"})]
)

KEYWORD_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
        pa.field("_id", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

LIGAND_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("name", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "dbReference",
            DB_REFERENCE_STRUCT,
            nullable=True,
            metadata={"comment": "Describes a database cross-reference. Equivalent to the flat file DR-line."},
        ),
        pa.field("label", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("note", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

LIGAND_PART_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("name", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "dbReference",
            DB_REFERENCE_STRUCT,
            nullable=True,
            metadata={"comment": "Describes a database cross-reference. Equivalent to the flat file DR-line."},
        ),
        pa.field("label", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("note", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

FEATURE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_type",
            pa.string(),
            nullable=False,
            metadata={
                "comment": "Describes the type of a sequence annotation. Equivalent to the flat file FT feature keys, but using full terms instead of acronyms.",
                "xsd_type": "xs:string",
            },
        ),
        pa.field("_id", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("_description", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "_evidence",
            pa.list_(pa.field("item", pa.int32(), nullable=True, metadata={"xsd_type": "xs:int"})),
            nullable=True,
        ),
        pa.field("_ref", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field(
            "original",
            pa.string(),
            nullable=True,
            metadata={
                "comment": "Describes the original sequence in annotations that describe natural or artifical sequence variations.",
                "xsd_type": "xs:string",
            },
        ),
        pa.field(
            "variation",
            pa.list_(pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"})),
            nullable=True,
            metadata={
                "comment": "Describes the variant sequence in annotations that describe natural or artifical sequence variations."
            },
        ),
        pa.field(
            "location",
            LOCATION_STRUCT,
            nullable=False,
            metadata={"comment": "Describes the sequence coordinates of the annotation."},
        ),
        pa.field(
            "ligand",
            LIGAND_STRUCT,
            nullable=True,
            metadata={
                "comment": "Describes the chemical entity that is bound in annotations that describe binding sites."
            },
        ),
        pa.field(
            "ligandPart",
            LIGAND_PART_STRUCT,
            nullable=True,
            metadata={
                "comment": "Describes the specific part of a molecule that is bound in annotations that describe binding sites."
            },
        ),
    ]
)

SOURCE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_ref", pa.int64(), nullable=True, metadata={"xsd_type": "xs:integer"}),
        pa.field(
            "dbReference",
            DB_REFERENCE_STRUCT,
            nullable=True,
            metadata={"comment": "Describes a database cross-reference. Equivalent to the flat file DR-line."},
        ),
    ]
)

IMPORTED_FROM_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "dbReference",
            DB_REFERENCE_STRUCT,
            nullable=False,
            metadata={"comment": "Describes a database cross-reference. Equivalent to the flat file DR-line."},
        )
    ]
)

EVIDENCE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "_type",
            pa.string(),
            nullable=False,
            metadata={
                "comment": "Describes the type of an evidence using the Evidence Code Ontology (http://www.obofoundry.org/cgi-bin/detail.cgi?id=evidence_code).",
                "xsd_type": "xs:string",
            },
        ),
        pa.field(
            "_key",
            pa.int64(),
            nullable=False,
            metadata={
                "comment": "A unique key to link annotations (via 'evidence' attributes) to evidences.",
                "xsd_type": "xs:integer",
            },
        ),
        pa.field(
            "source",
            SOURCE_STRUCT,
            nullable=True,
            metadata={
                "comment": "Describes the source of the data using a database cross-reference (or a 'ref' attribute when the source cannot be found in a public data source, such as PubMed, and is cited only within the UniProtKB entry)."
            },
        ),
        pa.field(
            "importedFrom",
            IMPORTED_FROM_STRUCT,
            nullable=True,
            metadata={
                "comment": "Describes the source of the evidence, when it is not assigned by UniProt, but imported from an external database."
            },
        ),
    ]
)

SEQUENCE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_length", pa.int32(), nullable=False, metadata={"xsd_type": "xs:int"}),
        pa.field("_mass", pa.int32(), nullable=False, metadata={"xsd_type": "xs:int"}),
        pa.field("_checksum", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("_modified", pa.date32(), nullable=False, metadata={"xsd_type": "xs:date"}),
        pa.field("_version", pa.int32(), nullable=False, metadata={"xsd_type": "xs:int"}),
        pa.field("_precursor", pa.bool_(), nullable=True, metadata={"xsd_type": "xs:boolean"}),
        pa.field("_fragment", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
        pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"}),
    ]
)

ENTRY_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("_dataset", pa.string(), nullable=False, metadata={"xsd_type": "xs:string"}),
        pa.field("_created", pa.date32(), nullable=False, metadata={"xsd_type": "xs:date"}),
        pa.field("_modified", pa.date32(), nullable=False, metadata={"xsd_type": "xs:date"}),
        pa.field("_version", pa.int32(), nullable=False, metadata={"xsd_type": "xs:int"}),
        pa.field(
            "accession",
            pa.list_(pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"})),
            nullable=False,
        ),
        pa.field(
            "name",
            pa.list_(pa.field("item", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"})),
            nullable=False,
        ),
        pa.field(
            "protein",
            PROTEIN_STRUCT,
            nullable=False,
            metadata={
                "comment": "Describes the names for the protein and parts thereof. Equivalent to the flat file DE-line."
            },
        ),
        pa.field(
            "gene",
            pa.list_(pa.field("item", GENE_STRUCT, nullable=True)),
            nullable=True,
            metadata={"comment": "Describes a gene. Equivalent to the flat file GN-line."},
        ),
        pa.field("organism", ORGANISM_STRUCT, nullable=False, metadata={"comment": "Describes the source organism."}),
        pa.field(
            "organismHost",
            pa.list_(pa.field("item", ORGANISM_STRUCT, nullable=True)),
            nullable=True,
            metadata={"comment": "Describes the source organism."},
        ),
        pa.field(
            "geneLocation",
            pa.list_(pa.field("item", GENE_LOCATION_STRUCT, nullable=True)),
            nullable=True,
            metadata={
                "comment": "Describes non-nuclear gene locations (organelles and plasmids). Equivalent to the flat file OG-line."
            },
        ),
        pa.field(
            "reference",
            pa.list_(pa.field("item", REFERENCE_STRUCT, nullable=True)),
            nullable=False,
            metadata={
                "comment": "Describes a citation and a summary of its content. Equivalent to the flat file RN-, RP-, RC-, RX-, RG-, RA-, RT- and RL-lines."
            },
        ),
        pa.field(
            "comment",
            pa.list_(pa.field("item", COMMENT_STRUCT, nullable=True)),
            nullable=True,
            metadata={
                "comment": "Describes different types of general annotations. Equivalent to the flat file CC-line."
            },
        ),
        pa.field(
            "dbReference",
            pa.list_(pa.field("item", DB_REFERENCE_STRUCT, nullable=True)),
            nullable=True,
            metadata={"comment": "Describes a database cross-reference. Equivalent to the flat file DR-line."},
        ),
        pa.field(
            "proteinExistence",
            PROTEIN_EXISTENCE_STRUCT,
            nullable=False,
            metadata={
                "comment": "Describes the evidence for the protein's existence. Equivalent to the flat file PE-line."
            },
        ),
        pa.field("keyword", pa.list_(pa.field("item", KEYWORD_STRUCT, nullable=True)), nullable=True),
        pa.field(
            "feature",
            pa.list_(pa.field("item", FEATURE_STRUCT, nullable=True)),
            nullable=True,
            metadata={
                "comment": "Describes different types of sequence annotations. Equivalent to the flat file FT-line."
            },
        ),
        pa.field(
            "evidence",
            pa.list_(pa.field("item", EVIDENCE_STRUCT, nullable=True)),
            nullable=True,
            metadata={"comment": "Describes the evidence for an annotation. No flat file equivalent."},
        ),
        pa.field("sequence", SEQUENCE_STRUCT, nullable=False),
    ]
)

SCHEMA: Final[pa.Schema] = assemble_pyarrow_schema(ENTRY_STRUCT)
COPYRIGHT_SCHEMA: Final[pa.Schema] = assemble_pyarrow_schema(
    [pa.field("#text", pa.string(), nullable=True, metadata={"xsd_type": "xs:string"})]
)
SCHEMAS: Final[dict[str, pa.Schema]] = {"entry": SCHEMA, "copyright": COPYRIGHT_SCHEMA}
