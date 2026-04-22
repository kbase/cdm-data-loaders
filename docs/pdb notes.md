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
