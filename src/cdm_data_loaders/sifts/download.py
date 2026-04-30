"""Download EBI SIFTS mapping files via FTP.

SIFTS (Structure Integration with Function, Taxonomy and Sequences) provides
cross-reference mappings between PDB chains and UniProt, Pfam, GO, and other
databases.  Files are published monthly to the EBI FTP server.

Relevant files (all under ``/pub/databases/msd/sifts/flatfiles/tsv/``):

* ``pdb_chain_uniprot.tsv.gz``  — PDB chain → UniProt accession mapping
* ``pdb_chain_pfam.tsv.gz``     — PDB chain → Pfam domain mapping (optional)

Each function returns the local path to the downloaded file and its MD5 hex
digest so callers can decide whether to re-upload to S3.
"""

from __future__ import annotations

import contextlib
from pathlib import Path  # noqa: TC003
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ftplib import FTP

from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.ftp_client import connect_ftp, ftp_download_file

logger = get_cdm_logger()

DEFAULT_SIFTS_HOST = "ftp.ebi.ac.uk"
SIFTS_BASE_PATH = "/pub/databases/msd/sifts/flatfiles/tsv"

# Canonical filenames published by EBI
SIFTS_UNIPROT_FILE = "pdb_chain_uniprot.tsv.gz"
SIFTS_PFAM_FILE = "pdb_chain_pfam.tsv.gz"


def download_sifts_file(
    filename: str,
    dest_dir: Path,
    ftp_host: str = DEFAULT_SIFTS_HOST,
    ftp_base_path: str = SIFTS_BASE_PATH,
) -> Path:
    """Download a single SIFTS TSV file from the EBI FTP server.

    The file is saved to ``dest_dir / filename``.

    :param filename: bare filename on the FTP server, e.g. ``"pdb_chain_uniprot.tsv.gz"``
    :param dest_dir: local directory to save the file into
    :param ftp_host: EBI FTP hostname
    :param ftp_base_path: remote directory containing the SIFTS TSV files
    :return: local :class:`~pathlib.Path` to the downloaded file
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    local_path = dest_dir / filename
    remote_path = f"{ftp_base_path.rstrip('/')}/{filename}"

    logger.info("Connecting to EBI FTP: %s", ftp_host)
    ftp: FTP = connect_ftp(ftp_host)
    try:
        logger.info("Downloading %s -> %s", remote_path, local_path)
        ftp_download_file(ftp, remote_path, str(local_path))
    finally:
        with contextlib.suppress(Exception):
            ftp.quit()

    logger.info("Downloaded %s (%d bytes)", filename, local_path.stat().st_size)
    return local_path
