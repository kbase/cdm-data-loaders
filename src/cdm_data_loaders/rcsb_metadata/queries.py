"""GraphQL query strings and field lists for each RCSB entity type.

Each entry in :data:`ENTITY_QUERIES` maps an entity name (which is also used
as the output NDJSON filename stem) to the GraphQL query string that fetches
it.  All queries accept an ``$ids`` variable — a list of PDB entry IDs.

Supported entity types:
    - ``entries``           — Core entry metadata (method, resolution, dates, organism, DOI)
    - ``validation``        — wwPDB validation metrics (Ramachandran, clashscore, RMSZ)
    - ``taxonomy``          — Full taxonomy lineage per entry
    - ``ligands``           — Bound ligand/chemical components per entry
    - ``citations``         — Primary and related citations
    - ``pfam``              — Pfam domain annotations (via polymer entity instances)
    - ``sequence_clusters`` — RCSB sequence cluster membership
"""


# ---------------------------------------------------------------------------
# Canonical order of entity types for deterministic run output
# ---------------------------------------------------------------------------
ENTITY_TYPES: list[str] = [
    "entries",
    "validation",
    "taxonomy",
    "ligands",
    "citations",
    "pfam",
    "sequence_clusters",
]

# ---------------------------------------------------------------------------
# GraphQL queries — each accepts variable $ids: [String!]!
# ---------------------------------------------------------------------------

ENTITY_QUERIES: dict[str, str] = {
    "entries": """
query Entries($ids: [String!]!) {
  entries(entry_ids: $ids) {
    rcsb_id
    struct { title }
    exptl { method }
    refine { ls_d_res_high ls_R_factor_R_work ls_R_factor_R_free }
    rcsb_entry_info {
      resolution_combined
      structure_determination_methodology
      deposited_polymer_monomer_count
    }
    rcsb_accession_info {
      deposit_date
      initial_release_date
      major_revision
      minor_revision
      revision_date
    }
    citation {
      pdbx_database_id_DOI
      journal_abbrev
      year
    }
    rcsb_entry_container_identifiers {
      entry_id
    }
    polymer_entities {
      rcsb_entity_host_organism {
        ncbi_scientific_name
        ncbi_taxonomy_id
      }
    }
  }
}
""",
    "validation": """
query Validation($ids: [String!]!) {
  entries(entry_ids: $ids) {
    rcsb_id
    pdbx_vrpt_summary {
      report_creation_date
    }
    pdbx_vrpt_summary_geometry {
      clashscore
      clashscore_full_length
      percent_ramachandran_outliers
      percent_ramachandran_outliers_full_length
      percent_rotamer_outliers
      angles_RMSZ
      bonds_RMSZ
    }
    pdbx_vrpt_summary_diffraction {
      percent_RSRZ_outliers
    }
  }
}
""",
    "taxonomy": """
query Taxonomy($ids: [String!]!) {
  entries(entry_ids: $ids) {
    rcsb_id
    polymer_entities {
      rcsb_entity_host_organism {
        ncbi_scientific_name
        ncbi_taxonomy_id
        scientific_name
      }
      rcsb_entity_source_organism {
        ncbi_scientific_name
        ncbi_taxonomy_id
        scientific_name
      }
    }
  }
}
""",
    "ligands": """
query Ligands($ids: [String!]!) {
  entries(entry_ids: $ids) {
    rcsb_id
    nonpolymer_entities {
      nonpolymer_comp {
        chem_comp {
          id
          name
          type
          formula
          formula_weight
        }
      }
    }
  }
}
""",
    "citations": """
query Citations($ids: [String!]!) {
  entries(entry_ids: $ids) {
    rcsb_id
    citation {
      id
      title
      year
      journal_abbrev
      pdbx_database_id_DOI
      pdbx_database_id_PubMed
      rcsb_authors
    }
  }
}
""",
    "pfam": """
query Pfam($ids: [String!]!) {
  entries(entry_ids: $ids) {
    rcsb_id
    polymer_entities {
      rcsb_id
      rcsb_polymer_entity_container_identifiers {
        auth_asym_ids
        uniprot_ids
      }
      pfams {
        rcsb_pfam_accession
        rcsb_pfam_identifier
      }
    }
  }
}
""",
    "sequence_clusters": """
query SequenceClusters($ids: [String!]!) {
  entries(entry_ids: $ids) {
    rcsb_id
    polymer_entities {
      rcsb_id
      rcsb_cluster_membership {
        cluster_id
        identity
      }
    }
  }
}
""",
}


def get_query(entity_type: str) -> str:
    """Return the GraphQL query string for *entity_type*.

    :param entity_type: one of :data:`ENTITY_TYPES`
    :raises KeyError: if *entity_type* is not recognised
    :return: GraphQL query string
    """
    if entity_type not in ENTITY_QUERIES:
        msg = f"Unknown RCSB entity type: {entity_type!r}. Valid types: {ENTITY_TYPES}"
        raise KeyError(msg)
    return ENTITY_QUERIES[entity_type]
