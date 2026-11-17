"""Shared constants and S3 staging helpers for ``datasets.ncbi_ftp`` unit tests."""

import hashlib
from pathlib import Path, PurePosixPath
from typing import Final

import botocore.client

from tests.helpers.paths_and_constants import TEST_BUCKET as TEST_BUCKET_STR

TEST_BUCKET: Final[PurePosixPath] = PurePosixPath(TEST_BUCKET_STR)

SAMPLE_SUMMARY: Final[str] = (
    "# assembly_accession\tbioproject\tbiosample\twgs_master\trefseq_category\t"
    "taxid\tspecies_taxid\torganism_name\tinfraspecific_name\tisolate\t"
    "version_status\tassembly_level\trelease_type\tgenome_rep\tseq_rel_date\t"
    "asm_name\t16\t17\t18\tftp_path\n"
    "GCF_000001215.4\tPRJNA13812\tSAMN02803731\t\treference genome\t7227\t7227\t"
    "Drosophila melanogaster\t\t\tlatest\tChromosome\tMajor\tFull\t2014/10/21\t"
    "Release_6_plus_ISO1_MT\t\t\t\t"
    "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT\n"
    "GCF_000001405.40\tPRJNA168\tna\t\treference genome\t9606\t9606\t"
    "Homo sapiens\t\t\tlatest\tChromosome\tPatch\tFull\t2022/02/03\t"
    "GRCh38.p14\t\t\t\t"
    "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14\n"
    "GCF_000005845.2\tPRJNA57779\tSAMN02604091\t\trepresentative genome\t511145\t562\t"
    "Escherichia coli\t\t\treplaced\tComplete Genome\tMajor\tFull\t2013/09/26\t"
    "ASM584v2\t\t\t\t"
    "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/005/845/GCF_000005845.2_ASM584v2\n"
    "GCF_000009999.1\tPRJNA999\tSAMN999\t\tna\t0\t0\t"
    "Test organism\t\t\tsuppressed\tScaffold\tMajor\tFull\t2010/01/01\t"
    "ASM999v1\t\t\t\t"
    "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/009/999/GCF_000009999.1_ASM999v1\n"
    "GCF_000099999.1\tPRJNA888\tSAMN888\t\tna\t0\t0\t"
    "Test organism 2\t\t\tlatest\tContig\tMajor\tFull\t2023/06/15\t"
    "ASM9999v1\t\t\t\tna\n"
)

ACC_PATH_215: Final[PurePosixPath] = (
    PurePosixPath("GCF") / "000" / "001" / "215" / "GCF_000001215.4_Release_6_plus_ISO1_MT"
)
ACC_PATH_405: Final[PurePosixPath] = PurePosixPath("GCF") / "000" / "001" / "405" / "GCF_000001405.40_GRCh38.p14"
ACC_PATH_845: Final[PurePosixPath] = PurePosixPath("GCF") / "000" / "005" / "845" / "GCF_000005845.2_ASM584v2"

MD5_CHECKSUMS_TXT: Final[str] = (
    "d41d8cd98f00b204e9800998ecf8427e  ./GCF_000001215.4_Release_6_plus_ISO1_MT_genomic.fna.gz\n"
    "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4  ./GCF_000001215.4_Release_6_plus_ISO1_MT_protein.faa.gz\n"
    "ffffffffffffffffffffffffffffffff  ./GCF_000001215.4_Release_6_plus_ISO1_MT_assembly_report.txt\n"
    "0000000000000000000000000000dead  ./GCF_000001215.4_Release_6_plus_ISO1_MT_README.txt\n"
)


def md5_hex(data: bytes) -> str:
    """Return the hex-encoded MD5 digest of ``data``.

    :param data: Payload bytes.
    :type data: bytes
    :return: Lowercase hexadecimal MD5 digest.
    :rtype: str
    """
    return hashlib.md5(data, usedforsecurity=False).hexdigest()


def write_manifest(tmp_path: Path, accessions: list[str], name: str = "manifest.txt") -> Path:
    """Write a manifest file containing one accession per line.

    :param tmp_path: Target directory.
    :type tmp_path: Path
    :param accessions: Accession strings.
    :type accessions: list[str]
    :param name: Filename inside ``tmp_path``.
    :type name: str
    :return: Written manifest path.
    :rtype: Path
    """
    path = tmp_path / name
    path.write_text("\n".join(accessions) + "\n", encoding="utf-8")
    return path


def stage_files_with_sidecars(
    s3: botocore.client.BaseClient,
    staging_base: PurePosixPath,
    files: dict[PurePosixPath, bytes],
    *,
    bucket: PurePosixPath = TEST_BUCKET,
    with_md5: bool = True,
    with_crc64: bool = False,
) -> list[PurePosixPath]:
    """Stage files under ``staging_base`` in S3 with optional ``.md5`` and ``.crc64nvme`` sidecars.

    :param s3: Boto3 S3 client.
    :type s3: botocore.client.BaseClient
    :param staging_base: Base S3 key prefix.
    :type staging_base: PurePosixPath
    :param files: Mapping of relative key paths to payload bytes.
    :type files: dict[PurePosixPath, bytes]
    :param bucket: Target S3 bucket path.
    :type bucket: PurePosixPath
    :param with_md5: Whether to write ``.md5`` sidecar objects.
    :type with_md5: bool
    :param with_crc64: Whether to write ``.crc64nvme`` sidecar objects.
    :type with_crc64: bool
    :return: List of staged data file keys (excluding sidecars).
    :rtype: list[PurePosixPath]
    """
    keys: list[PurePosixPath] = []
    for fname, content in files.items():
        key: PurePosixPath = staging_base / fname
        s3.put_object(Bucket=str(bucket), Key=str(key), Body=content)  # type: ignore[attr-defined]
        keys.append(key)
        if with_md5:
            s3.put_object(  # type: ignore[attr-defined]
                Bucket=str(bucket),
                Key=str(key.with_name(f"{key.name}.md5")),
                Body=md5_hex(content).encode("utf-8"),
            )
        if with_crc64:
            s3.put_object(  # type: ignore[attr-defined]
                Bucket=str(bucket),
                Key=str(key.with_name(f"{key.name}.crc64nvme")),
                Body=b"fake-crc",
            )
    return keys
