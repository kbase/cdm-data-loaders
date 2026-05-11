RefData (8 modules)
  - refdata_ncbi.md — 4M+ NCBI assemblies, 5.3M file manifest, GTDB:left_right_arrow:NCBI tax maps (6 tables)
  - refdata_pdb.md — 253K PDB entries with SIFTS UniProt mapping, validation, GO/Pfam/InterPro/EC (12 tables)
  - refdata_spire.md — 1.16M SPIRE MAGs + 107K representatives + ENVO + geocoordinates (6 tables)
  - refdata_jgi_gem_mags.md — 52,515 JGI GEM MAGs with GTDB taxonomy (5 tables)
  - refdata_uniprot.md — Full UniProt graph: 215M proteins, 4.35B identifiers, 734M names (13 tables)
  - refdata_uniref.md — Combined module covering UniRef50/90/100 across both 2025-03 and 2026-01 snapshots
  - (noted: refdata_gem_mags is a phantom — HMS entry but missing S3 data)

  MSyScoLo
  - msyscolo_grow.md — GROW (Genome Resolved Open Watersheds): 163 freshwater samples, 2,093 dereplicated MAGs with DRAM traits

  PROTECT (3 modules)
  - protect_genomedepot.md — refreshed: 37 tables (existing docs/schemas/protect.md only listed 6)
  - protect_integration.md — 76 patients, 4,405 isolates, REDCap clinical :left_right_arrow: multi-omics linkage
  - protect_mind.md — CF metagenomics/metatranscriptomics: 195M+ stratified counts, PA-aware substrate scoring, prebiotic candidates

  Notable findings

  - Massive UniProt scale: refdata_uniprot.identifier has 4.35B rows; always use identifier_partitioned with filters.
  - refdata_gem_mags is broken — registered in HMS but s3a://cdm-lake/tenant-sql-warehouse/refdata/refdata_gem_mags.db/ is empty. Use refdata_jgi_gem_mags.
  - docs/schemas/protect.md is incomplete (6 of 37 tables documented) — refreshed via the new module.
  - PROTECT-MIND links to PROTECT-Integration via sample_id/patient_id for full clinical-omics analysis.
  - GROW has two MAG tiers (3,191 raw / 2,093 dereplicated) — joins silently drop rows if you mix.