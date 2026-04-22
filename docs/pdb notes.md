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

From our Google doc:

Assignee: Matt
Status: Investigating
Source Info: https://www.wwpdb.org/ftp/pdb-beta-ftp-sites 
https://www.wwpdb.org/ftp/pdb-beta-ftp-sites (new ID format and download instructions)
https://mmcif.wwpdb.org/docs/user-guide/guide.html (mmCIF format)
https://mmcif.wwpdb.org/ (mmCIF Database)
https://rcsbapi.readthedocs.io/en/latest/ (python package for interacting with rcsb.org API)
DTS Manifest:
Data Files:
Sample Metadata: n/a?
Script(s):
Notes: existing stuff has been pulled in via https://github.com/kbaseincubator/BERIL-research-observatory/tree/main/data/pdb_collection 
https://data.rcsb.org/ 
https://www.wwpdb.org/ftp/pdb-beta-ftp-sites 
rscb.org endpoints follow similar structure as mmCIF (with some differences)
Accessing Data
AlphaFold data not in PDB archive (just linked on website) - download from Google Cloud / FTP site
Use rsync for bulk transfer
Use short URL format for downloading individual files (wwPDB: PDB Beta Archive Download)
Use rscb.org REST API (/search and /data) for finding records and getting metadata
Versioning
Each recordset has Major and Minor version
Major: change in coordinates
Minor: updates to metadata
Keeps only latest minor version in archive
Provides historical archive snapshots (could get previous minor versions this way)
