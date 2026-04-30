Copilot's Size Estimates:

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



Notes on what was "brought in" with BERIL repo code:

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
