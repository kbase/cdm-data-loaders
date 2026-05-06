# Copilot's Size Estimates

Now I have enough data to give you a solid estimate. Here's what the numbers look like based on the wwPDB holdings and RCSB statistics (as of April 2026):

---

## Record counts

| File type | Entries | Notes |
|---|---|---|
| **structures** | ~226,000 | Every released entry has an mmCIF coordinate file |
| **experimental_data** | ~194,000 | X-ray: ~180K structure factor files; NMR: ~13K restraint/shift files; EM: smaller subset (most EM density maps live in EMDB, not here) |
| **validation_reports** | ~220,000 | Required for all depositions since 2021; most older entries have been backfilled |
| **assemblies** | ~200,000+ entries; many more files | Most entries have 1–3 assembly files; ribosomes and virus particles can have dozens |

The RCSB stats page shows **193,791** structures with experimental data and the holdings file sizes (`released_structures_last_modified_dates.json.gz` = 1.1 MB compressed, `released_experimental_data_last_modified_dates.json.gz` = 1.0 MB) are consistent with ~220–226K and ~194–200K entries respectively.

---

## Size estimates (compressed)

These are rough extrapolations based on typical file sizes and the "over 1 TB" total archive figure from wwPDB (which includes coordinates in 3 formats — we're only doing mmCIF):

| File type | Compressed estimate | Rationale |
|---|---|---|
| **structures** | ~50–100 GB | avg mmCIF ~250–450 KB compressed; EM structures can be 10–50 MB |
| **experimental_data** | ~30–60 GB | SF files avg ~150–300 KB; NMR restraints smaller |
| **validation_reports** | **~200–400 GB** | Largest category: PDF per entry ~1–3 MB, plus XML/mmCIF reports; 3–5 files per entry |
| **assemblies** | ~30–70 GB | avg 2–3 assemblies per entry ~250–350 KB each; outliers are large |

**Total: ~300–600 GB compressed** for the four Beta archive file types.

Validation reports dominate by a significant margin. If you're planning to store everything, budget for roughly 500 GB compressed (uncompressed would be 3–8× that depending on file type, since the gzip compression ratio for mmCIF/CIF text is high but PDFs compress poorly).

---

A few caveats worth noting for planning:
- The ~226K entry count grows by ~15–18K entries per year (Wednesday releases)
- EM entries are growing fastest and tend to have the largest coordinate files
- The `current_file_holdings.json.gz` (17.6 MB compressed) in the Beta holdings would give exact per-entry file-type availability if you need to stratify precisely



# Notes on what was "brought in" with BERIL repo code

Here's what was brought in:

---

## What files were ingested

The script pulled **metadata only** (no coordinate/structure files) from two sources:

| Source | What was fetched |
|---|---|
| RCSB GraphQL API (`data.rcsb.org/graphql`) | Entry-level metadata: method, resolution, R-factors, organism, dates, DOI — batched 1000 IDs at a time |
| EBI SIFTS FTP (`ftp.ebi.ac.uk/.../pdb_chain_uniprot.tsv.gz`) | PDB chain → UniProt accession mapping |

These were downloaded to scratch at `/pscratch/sd/p/psdehal/pdb_collection/` and converted to TSV, then uploaded to the lakehouse.

**Confirmed ingested (2026-03-14, from changelog):**

| File | Rows | Content |
|---|---|---|
| `pdb_entries.tsv` | 250,741 | One row/entry: pdb_id, title, method, resolution, r_work, r_free, organism, tax_id, dates, DOI |
| `pdb_uniprot_mapping.tsv` | 966,977 | PDB chain → UniProt (from SIFTS) |
| `pdb_validation.tsv` | ~250K | clashscore, Ramachandran outliers, rotamer outliers, angles/bonds RMSZ |

**Also defined in config (added ~1 month ago, ingestion status unclear):**

- `pdb_taxonomy.tsv` — pdb_id, taxonomy_id, organism
- `pdb_ligands.tsv` — pdb_id, ligand_id, name, type, formula, formula_weight
- `pdb_citations.tsv` — pdb_id, citation_id, title, year, journal, DOI, pubmed_id, authors
- `pdb_pfam.tsv` — pdb_id, chain_id, uniprot_accession, pfam_id, coverage
- `pdb_sequence_clusters.tsv` — cluster_id, pdb_entity_id, identity_level

---

## Where is it in the lakehouse

From `pdb_collection.json`:

**Bronze (raw TSVs):**
```
s3a://cdm-lake/tenant-general-warehouse/kescience/datasets/pdb/
  pdb_entries.tsv
  pdb_uniprot_mapping.tsv
  pdb_validation.tsv
  [+ 5 extended .tsv files if ingested]
```

**Silver (Delta Lake / SQL):**
```
s3a://cdm-lake/tenant-sql-warehouse/kescience/kescience_pdb.db
  → kescience_pdb.pdb_entries
  → kescience_pdb.pdb_uniprot_mapping
  → kescience_pdb.pdb_validation
```

---

**Key gap relative to what we're planning to load:** The BERIL collection is entirely **derived metadata** (GraphQL + SIFTS), not the raw wwPDB archive files (mmCIF coordinates, structure factors, validation XMLs/PDFs, assembly files). So "at least everything in this link" means the 3+ derived metadata tables — not the raw archive files described elsewhere in your notes.




# From our Google doc

* Assignee: Matt
* Status: Investigating
* Source Info: https://www.wwpdb.org/ftp/pdb-beta-ftp-sites 
  * https://www.wwpdb.org/ftp/pdb-beta-ftp-sites (new ID format and download instructions)
  * https://mmcif.wwpdb.org/docs/user-guide/guide.html (mmCIF format)
  * https://mmcif.wwpdb.org/ (mmCIF Database)
  * https://rcsbapi.readthedocs.io/en/latest/ (python package for interacting with rcsb.org API)
* DTS Manifest:
* Data Files:
* Sample Metadata: n/a?
* Script(s):
* Notes: existing stuff has been pulled in via https://github.com/kbaseincubator/BERIL-research-observatory/tree/main/data/pdb_collection 
  * https://data.rcsb.org/ 
  * https://www.wwpdb.org/ftp/pdb-beta-ftp-sites 
  * rscb.org endpoints follow similar structure as mmCIF (with some differences)
  * Accessing Data
    * AlphaFold data not in PDB archive (just linked on website) - download from Google Cloud / FTP site
    * Use rsync for bulk transfer
    * Use short URL format for downloading individual files (wwPDB: PDB Beta Archive Download)
    * Use rscb.org REST API (/search and /data) for finding records and getting metadata
  * Versioning
    * Each recordset has Major and Minor version
      * Major: change in coordinates
      * Minor: updates to metadata
    * Keeps only latest minor version in archive
    * Provides historical archive snapshots (could get previous minor versions this way)

---

# RCSB Current Archive vs. RCSB Beta Archive — Data Availability

## Background

The **PDB Beta Archive** is not a replacement for the RCSB Data API or any derived
metadata service — it is a reorganization of the same raw wwPDB archive files under
extended 8-character IDs (e.g., `pdb_00001abc`). The transition is driven by the
fact that 4-character PDB IDs (`1ABC`) are expected to be exhausted around 2028.

There are actually **four separate systems** to understand:

| System | Updated | What it contains |
|--------|---------|-----------------|
| **Current wwPDB archive** (`files.wwpdb.org`) | Weekly (Wednesday) | Raw structure files with classic 4-char IDs |
| **PDB Beta Archive** (`files-beta.rcsb.org`) | Weekly (Wednesday) | Same raw files, reorganized with extended 8-char IDs |
| **PDB NextGen Archive** (`files-nextgen.rcsb.org`) | Monthly (1st Wednesday) | Enriched mmCIF files with SIFTS annotations embedded (UniProt, Pfam, SCOP2 at residue level) |
| **RCSB Data API** (`data.rcsb.org`) | Weekly | Derived/enriched metadata: taxonomy, clusters, citations, cross-refs, CSMs |

---

## What IS in the Beta Archive (mirrors current archive)

The Beta archive contains **all the same raw data** as the current wwPDB archive,
reorganized with extended IDs and a per-entry directory structure. Everything below
is available from both archives.

### Atomic coordinates

| Format | Beta archive URL | Current archive URL |
|--------|-----------------|---------------------|
| PDBx/mmCIF (compressed) | `https://files-beta.rcsb.org/download/pdb_00001abc.cif.gz` | `https://files.rcsb.org/download/4hhb.cif.gz` |
| PDBx/mmCIF (uncompressed) | `https://files-beta.rcsb.org/download/pdb_00001abc.cif` | `https://files.rcsb.org/download/4hhb.cif` |
| PDBML/XML | `https://files-beta.rcsb.org/download/pdb_00001abc.xml.gz` | `https://files.rcsb.org/download/4hhb.xml.gz` |
| Legacy PDB format | `https://files-beta.rcsb.org/download/pdb_00001abc.pdb.gz` | `https://files.rcsb.org/download/4hhb.pdb.gz` |

> **Note:** Legacy PDB format will be discontinued for entries issued after
> 4-char IDs are exhausted (~2028). Only mmCIF will be supported for new extended IDs.

### Experimental data

| Data type | Beta archive location | rsync path |
|-----------|----------------------|------------|
| X-ray structure factors (mmCIF) | `https://files-beta.rcsb.org/download/pdb_00001abc-sf.cif.gz` | `pdb_data/entries/{hash}/{id}/experimental_data/` |
| NMR combined data (NEF format) | `https://files-beta.rcsb.org/download/pdb_00001abc_nmr-data.nef.gz` | same |
| NMR combined data (NMR-STAR) | `https://files-beta.rcsb.org/download/pdb_00001abc_nmr-data.str.gz` | same |
| NMR chemical shifts | `https://files-beta.rcsb.org/download/pdb_00001abc_cs.str.gz` | same |

### Validation reports

| Format | Beta archive URL |
|--------|-----------------|
| PDF (compressed) | `https://files-beta.rcsb.org/validation/download/pdb_00001abc_validation.pdf.gz` |
| XML/mmCIF reports | `pdb_data/entries/{hash}/{id}/validation_reports/` (rsync) |

Rsync command for all validation reports:
```
rsync -rlpt -v --delete --port=32382 --prune-empty-dirs \
  --include '*/' --include='*/validation_reports/***' --exclude='*' \
  rsync-beta.rcsb.org::pdb_data/entries/ ./validation/
```

### Biological assemblies

| Format | Beta archive URL |
|--------|-----------------|
| PDBx/mmCIF (compressed) | `https://files-beta.rcsb.org/download/pdb_00001abc-assembly1.cif.gz` |

Full directory: `pdb_data/entries/{hash}/{id}/assemblies/` (rsync)

### Reference data (small molecules)

| Data type | Beta archive location |
|-----------|----------------------|
| Chemical Component Dictionary (CCD) | `https://files-beta.rcsb.org/ligands/download/ATP.cif` |
| CCD rsync | `rsync --port=32382 rsync-beta.rcsb.org::pdb_refdata/chem_comp/` |
| BIRD (biologically interesting molecules) | `https://files-beta.rcsb.org/birds/download/PRD_000006.cif` |
| CCD derived data (SMILES, InChI, variants) | `https://files-beta.rcsb.org/pub/wwpdb/refdata/metadata/` |

### Holdings / inventory files

Available at `https://files-beta.rcsb.org/pub/wwpdb/pdb/holdings/` (same in current archive):

| File | Content |
|------|---------|
| `current_file_holdings.json.gz` | All released entries with file types present |
| `released_structures_last_modified_dates.json.gz` | Release + last-modified dates for coordinate files |
| `released_experimental_data_last_modified_dates.json.gz` | Same for experimental data files |
| `obsolete_structures_last_modified_dates.json.gz` | Obsoleted entries |
| `all_removed_entries.json.gz` | Obsoleted entries with author, title, dates, superseding ID |
| `unreleased_entries.json.gz` | On-hold entries and pre-release sequence info |

### Archive snapshots (annual)

Annual snapshots of the full archive for reproducible research:

| Protocol | URL |
|----------|-----|
| HTTPS (S3 explorer) | `https://s3snapshots.rcsb.org/` |
| AWS S3 sync | `aws s3 sync s3://pdbsnapshots/ ./local-directory/ --no-sign-request` |

---

## What is NOT in the Beta Archive

### 1. RCSB-enriched derived metadata (Data API only)

The **RCSB Data API** (`https://data.rcsb.org`) provides metadata that is
**not stored in the raw archive files** — it is derived/integrated by RCSB from
multiple sources. This data is currently accessible only via the API (REST or
GraphQL) and is **not affected by the Beta archive transition**. The API already
accepts both classic (`4HHB`) and extended (`pdb_00004hhb`) IDs.

| Data type | API endpoint | Notes |
|-----------|-------------|-------|
| Entry metadata (method, resolution, R-factors, dates, title) | `GET https://data.rcsb.org/rest/v1/core/entry/{id}` or GraphQL `entries()` | Core mmCIF fields + RCSB additions |
| Polymer entity metadata (sequence, source organism) | `GET https://data.rcsb.org/rest/v1/core/polymer_entity/{id}/{entity_id}` | |
| Taxonomy (NCBI taxonomy lineage via SIFTS) | GraphQL `polymer_entities { rcsb_entity_source_organism }` | |
| UniProt cross-references | GraphQL `polymer_entities { rcsb_polymer_entity_container_identifiers { reference_sequence_identifiers } }` | |
| Pfam domain annotations | GraphQL `polymer_entity_instances { rcsb_polymer_instance_annotation }` | Positional features |
| SCOP / CATH domain classifications | GraphQL `polymer_entity_instances { rcsb_polymer_instance_annotation }` | Positional features |
| Sequence cluster membership | GraphQL `polymer_entities { rcsb_cluster_membership }` | DIAMOND at 30–100% identity |
| Citations and PubMed IDs | GraphQL `entries { rcsb_primary_citation }` | |
| Ligand / chemical component metadata | GraphQL `chem_comps()` or `nonpolymer_entities()` | |
| DrugBank integration | `GET https://data.rcsb.org/rest/v1/core/drugbank/{comp_id}` | |
| Biological assembly metadata | `GET https://data.rcsb.org/rest/v1/core/assembly/{id}/{assembly_id}` | |
| Branched entity (carbohydrate) data | GraphQL `branched_entities()` | |
| Integrative/hybrid (IHM) structure flags | GraphQL `entries { rcsb_entry_info { ihm_* } }` | |
| Full current entry ID list | `GET https://data.rcsb.org/rest/v1/holdings/current/entry_ids` | |

GraphQL endpoint: `https://data.rcsb.org/graphql`
Interactive explorer: `https://data.rcsb.org/graphiql/index.html`

### 2. Computed Structure Models (CSMs)

CSMs (AlphaFold, ModelArchive) are **not in the wwPDB archive at all** —
neither the current archive nor the Beta archive. They are hosted separately
and integrated into RCSB services only.

| Source | Entry count | Access |
|--------|------------|--------|
| AlphaFold Protein Structure Database | ~1,000,000 models at RCSB | `https://alphafold.ebi.ac.uk/` or `https://ftp.ebi.ac.uk/pub/databases/alphafold/` |
| ModelArchive (Baker lab complexes, etc.) | Several datasets | `https://modelarchive.org/` |

RCSB IDs for CSMs use prefixed format: `AF_AFP68871F1` (AlphaFold), `MA_*` (ModelArchive).
Accessible via Data API: `https://data.rcsb.org/rest/v1/core/entry/AF_AFP68871F1`

### 3. PDB NextGen Archive enrichments (embedded SIFTS)

Since February 2023, the **PDB NextGen Archive** distributes mmCIF files that
embed SIFTS-style annotations directly (UniProt, Pfam, SCOP2 mappings at residue
and atom level). These enriched files are **not in the Beta archive** — they are
a separate archive updated monthly.

| Item | URL |
|------|-----|
| RCSB NextGen mirror | `https://files-nextgen.rcsb.org/` |
| rsync | `rsync-nextgen.rcsb.org::` (port 873) |
| Enriched file naming | `pdb_00001abc_xyz-enrich.cif.gz` |

### 4. EMDB electron density maps

Cryo-EM density maps are maintained by **EMDB** (separate from the PDB archive).
The Beta archive files only contain the mmCIF coordinate models for EM structures —
the density maps themselves must be downloaded from EMDB.

| Resource | URL |
|----------|-----|
| EMDB file downloads (RCSB mirror) | `https://files.rcsb.org/pub/emdb/structures` |
| EMDB file downloads (PDBe) | `https://ftp.ebi.ac.uk/pub/databases/emdb/structures` |

### 5. IHM (Integrative/Hybrid Methods) structures

Structures determined by integrative methods are in a **separate sub-archive**
(`pdb_ihm`) that is independent of the Beta archive:

| Item | URL |
|------|-----|
| IHM data files | `https://files.wwpdb.org/pub/pdb_ihm/data/entries/` |
| IHM holdings | `https://files.wwpdb.org/pub/pdb_ihm/holdings/` |

### 6. RCSB-specific API services (no archive equivalent)

These services are derived/computed by RCSB and have no equivalent in any raw archive:

| Service | Endpoint | What it provides |
|---------|----------|-----------------|
| Search API | `https://search.rcsb.org/` | Full-text + sequence + structure similarity search across all PDB + CSMs |
| Sequence Coordinates API | `https://sequence-coordinates.rcsb.org/` | Alignments: PDB ↔ UniProt, RefSeq, NCBI genomic sequences |
| Alignment API | `https://alignment.rcsb.org/` | On-demand pairwise structure alignment |
| ModelServer API | `https://models.rcsb.org/` | Subset retrieval of coordinates in BinaryCIF format |
| VolumeServer API | `https://maps.rcsb.org/` | Subsets of electron density volumetric data |
| Sequence clusters (flat files) | `https://cdn.rcsb.org/resources/sequence/clusters/clusters-by-entity-{30,40,50,70,90,95,100}.txt` | DIAMOND clustering results |
| FASTA sequences (all entries) | `https://files.rcsb.org/pub/pdb/metadata/pdb_seqres.txt.gz` | All PDB sequences in FASTA |
| BinaryCIF coordinate files | `https://models.rcsb.org/{id}.bcif.gz` | RCSB-specific binary format |

---

## Timeline and transition

| Milestone | Expected date | Notes |
|-----------|--------------|-------|
| 4-char PDB IDs exhausted | ~2028 | New depositions get extended IDs only |
| Beta archive becomes the primary archive | At or after 4-char exhaustion | Beta archive is the intended replacement |
| Legacy PDB format discontinued for new entries | At 4-char exhaustion | PDB format cannot represent 8-char IDs |
| RCSB Data API, Search API, etc. | Unaffected — already running | APIs already accept both ID formats |

**Current status (May 2026):** The Beta archive is a complete mirror of the current
archive. All ~226K released entries are available from both. New entries continue
to be added to both on the same Wednesday release schedule. The transition to the
Beta archive as the *only* archive is anticipated around 2028, timed to when
4-char IDs are consumed.

---

## Summary: Where to get each data type

| Data type | Beta archive | Current archive | RCSB Data API | Other |
|-----------|-------------|-----------------|---------------|-------|
| Atomic coordinates (mmCIF) | ✓ | ✓ | — | — |
| Atomic coordinates (XML, legacy PDB) | ✓ | ✓ | — | — |
| Structure factors (X-ray SF) | ✓ | ✓ | — | — |
| NMR restraints / chemical shifts | ✓ | ✓ | — | — |
| Validation reports (PDF, XML) | ✓ | ✓ | — | — |
| Biological assemblies (mmCIF) | ✓ | ✓ | assembly metadata | — |
| Chemical Component Dictionary (CCD) | ✓ | ✓ | `chem_comps()` | — |
| BIRD (biologically interesting molecules) | ✓ | ✓ | — | — |
| Holdings / inventory files | ✓ | ✓ | `/holdings/current/entry_ids` | — |
| Entry metadata (title, method, resolution) | — | — | ✓ | — |
| Taxonomy / organism data | — | — | ✓ (via SIFTS) | NextGen archive (embedded) |
| UniProt cross-references | — | — | ✓ | NextGen archive (embedded) |
| Pfam / SCOP / CATH annotations | — | — | ✓ | NextGen archive (embedded) |
| Sequence cluster membership | — | — | ✓ | Flat files at cdn.rcsb.org |
| Citations / PubMed integration | — | — | ✓ | — |
| DrugBank ligand data | — | — | ✓ | — |
| Computed structure models (AlphaFold, etc.) | — | — | ✓ | AlphaFold DB, ModelArchive |
| EMDB electron density maps | — | — | — | EMDB (`ftp.ebi.ac.uk`) |
| IHM integrative structures | — | — | ✓ (metadata) | `files.wwpdb.org/pub/pdb_ihm/` |
| SIFTS enrichments (embedded in mmCIF) | — | — | — | NextGen archive only |
| Sequence search / structure similarity | — | — | — | RCSB Search API |
| BinaryCIF coordinates | — | — | — | `models.rcsb.org` |

---

# BERIL vs. CDM Data Loaders: Metadata Coverage Comparison

## What BERIL ingested

The [BERIL PDB collection script](https://github.com/kbaseincubator/BERIL-research-observatory/blob/main/data/pdb_collection/scripts/download_pdb_data.py)
downloads from two sources: the RCSB GraphQL API and a single SIFTS file from EBI FTP.
It outputs TSVs that are ingested into a Delta Lake database (`kescience_pdb`).

Full schema docs: [docs/schemas/pdb.md](https://github.com/kbaseincubator/BERIL-research-observatory/blob/main/docs/schemas/pdb.md)

### BERIL tables — confirmed ingested (2026-03-14)

| Table | Rows | Source | Columns |
|-------|------|--------|---------|
| `pdb_entries` | 250,741 | RCSB GraphQL `entries()` | pdb_id, title, method, method_full, resolution, r_work, r_free, organism *(1st entity only)*, deposition_date, release_date, citation_doi |
| `pdb_uniprot_mapping` | 966,977 | SIFTS [`pdb_chain_uniprot.tsv.gz`](https://ftp.ebi.ac.uk/pub/databases/msd/sifts/flatfiles/tsv/pdb_chain_uniprot.tsv.gz) | pdb_id, chain_id, uniprot_accession, res_beg, res_end, pdb_beg, pdb_end, sp_beg, sp_end |
| `pdb_validation` | ~250K | RCSB GraphQL `pdbx_vrpt_summary_geometry` | pdb_id, clashscore, percent_ramachandran_outliers, percent_rotamer_outliers, angles_rmsz, bonds_rmsz |

### BERIL tables — defined in config, ingestion status unclear

These 5 tables appear in [`pdb_collection.json`](https://github.com/kbaseincubator/BERIL-research-observatory/blob/main/data/pdb_collection/scripts/pdb_collection.json)
but are not mentioned in the changelog as having been ingested:

| Table | Source | Columns |
|-------|--------|---------|
| `pdb_taxonomy` | RCSB GraphQL | pdb_id, taxonomy_id, organism |
| `pdb_ligands` | RCSB GraphQL | pdb_id, ligand_id, ligand_name, ligand_type, formula, formula_weight |
| `pdb_citations` | RCSB GraphQL | pdb_id, citation_id, is_primary, title, year, journal, volume, page_first, doi, pubmed_id, authors |
| `pdb_pfam` | RCSB GraphQL | pdb_id, chain_id, uniprot_accession, pfam_id, coverage |
| `pdb_sequence_clusters` | RCSB GraphQL | cluster_id, pdb_entity_id, identity_level |

---

## Side-by-side coverage comparison

The table below covers every data item BERIL fetches (or planned to fetch) and shows
whether our RCSB metadata notebook (`pdb_rcsb_metadata.ipynb`) and SIFTS notebook
(`pdb_sifts.ipynb`) already cover it — and at what fidelity.

**Legend:** ✓ = covered, ✓+ = covered with more fields/detail, — = not covered

### RCSB-sourced metadata

| Data item | BERIL fields | CDM RCSB notebook | Notes |
|-----------|-------------|-------------------|-------|
| Entry core metadata | pdb_id, title, method, method_full, resolution, r_work, r_free, organism, deposition_date, release_date, citation_doi | ✓+ | CDM also captures: `major_revision`, `minor_revision`, `revision_date`, `structure_determination_methodology`, `deposited_polymer_monomer_count`, `refine` block; organism limited to 1st entity in both |
| Validation metrics | clashscore, %Rama outliers, %rotamer outliers, angles/bonds RMSZ | ✓+ | CDM also captures: `clashscore_full_length`, `percent_ramachandran_outliers_full_length`, `percent_RSRZ_outliers` (X-ray only) |
| Taxonomy per entity | taxonomy_id, organism | ✓+ | CDM captures both `rcsb_entity_source_organism` **and** `rcsb_entity_host_organism` per polymer entity, including NCBI lineage |
| Ligands / chemical components | ligand_id, name, type, formula, formula_weight | ✓ | Same fields |
| Citations | citation_id, is_primary, title, year, journal, volume, page_first, doi, pubmed_id, authors | ✓ | CDM uses `rcsb_authors` (list), journal_abbrev; equivalent coverage |
| Pfam domain annotations | pdb_id, chain_id, uniprot_accession, pfam_id, coverage | ✓+ | CDM captures `rcsb_pfam_accession` + `rcsb_pfam_identifier` per polymer entity; also see SIFTS |
| Sequence cluster membership | cluster_id, pdb_entity_id, identity_level | ✓ | CDM captures cluster_id + identity per polymer entity |

### SIFTS-sourced metadata

| Data item | BERIL coverage | CDM SIFTS notebook | Notes |
|-----------|---------------|-------------------|-------|
| PDB chain → UniProt mapping | ✓ `pdb_chain_uniprot.tsv.gz` | ✓ same file | Identical source |
| CATH domain classifications | — | ✓ `pdb_chain_cath_uniprot.tsv.gz` | Not in BERIL |
| Ensembl gene mapping | — | ✓ `pdb_chain_ensembl.tsv.gz` | Not in BERIL |
| Enzyme classification (EC numbers) | — | ✓ `pdb_chain_enzyme.tsv.gz` | Not in BERIL |
| Gene Ontology (GO) annotations | — | ✓ `pdb_chain_go.tsv.gz` | Not in BERIL |
| HMMER domain annotations | — | ✓ `pdb_chain_hmmer.tsv.gz` | Not in BERIL |
| InterPro domain annotations | — | ✓ `pdb_chain_interpro.tsv.gz` | Not in BERIL |
| Pfam (chain level, from EBI) | — | ✓ `pdb_chain_pfam.tsv.gz` | Complements RCSB Pfam; EBI source vs RCSB source |
| SCOP structural classifications | — | ✓ `pdb_chain_scop_uniprot.tsv.gz` | Not in BERIL |
| SCOP2 structural classifications | — | ✓ `pdb_chain_scop2_uniprot.tsv.gz` + `pdb_chain_scop2b_sf_uniprot.tsv.gz` | Not in BERIL |
| Taxonomy (chain level, from EBI) | — | ✓ `pdb_chain_taxonomy.tsv.gz` | Not in BERIL |
| Pfam residue-level mapping | — | ✓ `pdb_pfam_mapping.tsv.gz` | Not in BERIL |
| PDB → PubMed citations (EBI) | — | ✓ `pdb_pubmed.tsv.gz` | Complements RCSB citations; EBI source |
| Reverse UniProt → PDB mapping | — | ✓ `uniprot_pdb.tsv.gz` | Not in BERIL |
| UniProt segments with observed structure | — | ✓ `uniprot_segments_observed.tsv.gz` | Not in BERIL |

### Summary

- **Our RCSB notebook is a strict superset of all BERIL RCSB-sourced data**, with additional
  fields in most entity types (revision history, full-length validation scores, X-ray RSRZ,
  both source + host organism).
- **Our SIFTS notebook covers 15× more data than BERIL's SIFTS usage** — BERIL only used
  `pdb_chain_uniprot.tsv.gz`; we download all 16 EBI flat files, adding CATH, GO, InterPro,
  SCOP/SCOP2, Ensembl, EC numbers, HMMER, and residue-level Pfam.
- The 5 BERIL "planned" tables (taxonomy, ligands, citations, pfam, sequence_clusters) are all
  covered — and exceeded — by our RCSB metadata notebook output.
- CDM stores data as NDJSON (one file per entity type) while BERIL used per-table TSVs; both
  are straightforward to load into Delta Lake.
