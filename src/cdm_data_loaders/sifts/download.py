"""Download EBI SIFTS mapping files via FTP.

SIFTS (Structure Integration with Function, Taxonomy and Sequences) provides
cross-reference mappings between PDB chains and UniProt, Pfam, GO, and other
databases.  Files are published monthly to the EBI FTP server at:

    ftp://ftp.ebi.ac.uk/pub/databases/msd/sifts/flatfiles/tsv/

Use :data:`ALL_SIFTS_FILES` for the full list of available files, or call
:func:`download_sifts_files` to download a subset in one FTP session.
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

# Canonical filenames published by EBI (sorted alphabetically)
ALL_SIFTS_FILES: list[str] = [
    "pdb_chain_cath_uniprot.tsv.gz",
    "pdb_chain_ensembl.tsv.gz",
    "pdb_chain_enzyme.tsv.gz",
    "pdb_chain_go.tsv.gz",
    "pdb_chain_hmmer.tsv.gz",
    "pdb_chain_interpro.tsv.gz",
    "pdb_chain_pfam.tsv.gz",
    "pdb_chain_scop2_uniprot.tsv.gz",
    "pdb_chain_scop2b_sf_uniprot.tsv.gz",
    "pdb_chain_scop_uniprot.tsv.gz",
    "pdb_chain_taxonomy.tsv.gz",
    "pdb_chain_uniprot.tsv.gz",
    "pdb_pfam_mapping.tsv.gz",
    "pdb_pubmed.tsv.gz",
    "uniprot_pdb.tsv.gz",
    "uniprot_segments_observed.tsv.gz",
]

# Convenience aliases for common single-file usage
SIFTS_UNIPROT_FILE = "pdb_chain_uniprot.tsv.gz"
SIFTS_PFAM_FILE = "pdb_chain_pfam.tsv.gz"


def download_sifts_files(
    filenames: list[str],
    dest_dir: Path,
    ftp_host: str = DEFAULT_SIFTS_HOST,
    ftp_base_path: str = SIFTS_BASE_PATH,
) -> list[Path]:
    """Download one or more SIFTS TSV files using a single FTP connection.

    :param filenames: list of bare filenames to download, e.g.
        ``["pdb_chain_uniprot.tsv.gz", "pdb_chain_go.tsv.gz"]``
    :param dest_dir: local directory to save files into
    :param ftp_host: EBI FTP hostname
    :param ftp_base_path: remote directory containing the SIFTS TSV files
    :return: list of local :class:`~pathlib.Path` objects in the same order as *filenames*
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []

    logger.debug("Connecting to EBI FTP: %s", ftp_host)
    ftp: FTP = connect_ftp(ftp_host)
    try:
        for filename in filenames:
            local_path = dest_dir / filename
            remote_path = f"{ftp_base_path.rstrip('/')}/{filename}"
            logger.debug("Downloading %s -> %s", remote_path, local_path)
            ftp_download_file(ftp, remote_path, str(local_path))
            logger.debug("Downloaded %s (%d bytes)", filename, local_path.stat().st_size)
            paths.append(local_path)
    finally:
        with contextlib.suppress(Exception):
            ftp.quit()

    return paths


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

    logger.debug("Connecting to EBI FTP: %s", ftp_host)
    ftp: FTP = connect_ftp(ftp_host)
    try:
        logger.debug("Downloading %s -> %s", remote_path, local_path)
        ftp_download_file(ftp, remote_path, str(local_path))
    finally:
        with contextlib.suppress(Exception):
            ftp.quit()

    logger.debug("Downloaded %s (%d bytes)", filename, local_path.stat().st_size)
    return local_path
