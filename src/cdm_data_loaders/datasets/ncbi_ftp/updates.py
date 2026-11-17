"""Filter NCBI FTP assembly modification times by date, RefSeq/GenBank source, and assemblies of interest."""

import math
import operator
import re
from collections.abc import Iterable
from datetime import date
from enum import StrEnum
from logging import Logger, getLogger
from pathlib import Path
from typing import Final

import pandas as pd

logger: Logger = getLogger(__name__)

ASSEMBLY_DIR_RE: Final[re.Pattern[str]] = re.compile(r"^(GC[AF]_\d{9}\.\d+)_(.+)$")
BATCH_SIZE: Final[int] = 1000


class Source(StrEnum):
    """NCBI assembly database source."""

    REFSEQ = "refseq"
    GENBANK = "genbank"


class Period(StrEnum):
    """Temporal partition relative to the modification date threshold."""

    BEFORE = "before"
    AFTER = "after"


type Splits = dict[tuple[Source, Period], pd.DataFrame]


def parse_assembly_dir(assembly_dir: str) -> tuple[str, str]:
    """Split an assembly directory name into ``(accession_id, assembly_name)``.

    :param assembly_dir: assembly directory string (for example, ``"GCF_000123456.1_SomeName"``)
    :type assembly_dir: str
    :return: tuple of ``(accession_id, assembly_name)``
    :rtype: tuple[str, str]
    :raises ValueError: if ``assembly_dir`` does not match the expected NCBI directory pattern
    """
    match = ASSEMBLY_DIR_RE.match(assembly_dir)
    if not match:
        msg = f"assembly_dir '{assembly_dir}' does not match expected pattern"
        raise ValueError(msg)
    return match.group(1), match.group(2)


def infer_source(assembly_id: str) -> Source:
    """Infer whether ``assembly_id`` belongs to RefSeq or GenBank.

    :param assembly_id: assembly accession identifier
    :type assembly_id: str
    :return: :class:`Source` enum member
    :rtype: Source
    :raises ValueError: if ``assembly_id`` lacks a ``GCF_`` or ``GCA_`` prefix
    """
    if assembly_id.startswith("GCF_"):
        return Source.REFSEQ
    if assembly_id.startswith("GCA_"):
        return Source.GENBANK
    msg = f"Unrecognized assembly_id prefix: {assembly_id}"
    raise ValueError(msg)


def load_ftp_mod_times(path: Path) -> pd.DataFrame:
    """Load and parse ``ftp_mod_times.txt(.gz)`` into a tidy DataFrame.

    :param path: path to the raw TSV modification times file
    :type path: Path
    :return: DataFrame with ``assembly_dir``, ``assembly_id``, ``assembly_name``, ``source``, and ``modification_date``
    :rtype: pd.DataFrame
    """
    raw = pd.read_csv(
        path,
        sep="\t",
        header=None,
        names=["assembly_dir", "modification_date"],
        dtype=str,
    )

    ids_and_names = raw["assembly_dir"].apply(parse_assembly_dir)
    raw["assembly_id"] = ids_and_names.apply(operator.itemgetter(0))
    raw["assembly_name"] = ids_and_names.apply(operator.itemgetter(1))
    raw["source"] = raw["assembly_id"].apply(infer_source)
    raw["modification_date"] = pd.to_datetime(raw["modification_date"], format="%Y/%m/%d").dt.date

    return raw[["assembly_dir", "assembly_id", "assembly_name", "source", "modification_date"]]


def load_interesting_assembly_ids(path: Path) -> set[str]:
    """Read a single-column headerless file of assembly IDs of interest.

    :param path: path to the assembly ID list file
    :type path: Path
    :return: set of assembly accession strings
    :rtype: set[str]
    """
    df = pd.read_csv(path, sep="\t", header=None, names=["assembly_id"], dtype=str)
    return set(df["assembly_id"])


def build_interesting_membership_mask(
    parsed_df: pd.DataFrame,
    interesting_ids: set[str],
    *,
    filter_refseq: bool,
) -> pd.Series:
    """Build a boolean Series selecting rows to retain from ``parsed_df``.

    :param parsed_df: parsed modification times DataFrame
    :type parsed_df: pd.DataFrame
    :param interesting_ids: allowlist of assembly accessions
    :type interesting_ids: set[str]
    :param filter_refseq: when ``True``, filter RefSeq rows against ``interesting_ids`` as well as GenBank rows
    :type filter_refseq: bool
    :return: boolean selection mask
    :rtype: pd.Series
    """
    in_interesting = parsed_df["assembly_id"].isin(interesting_ids)
    if filter_refseq:
        return in_interesting

    is_refseq = parsed_df["source"] == Source.REFSEQ
    return is_refseq | in_interesting


def filter_to_interesting(
    parsed_df: pd.DataFrame,
    interesting_ids: set[str],
    *,
    filter_refseq: bool = False,
) -> pd.DataFrame:
    """Filter and deduplicate ``parsed_df`` to the latest modification date per interesting assembly ID.

    :param parsed_df: parsed modification times DataFrame
    :type parsed_df: pd.DataFrame
    :param interesting_ids: allowlist of assembly accessions
    :type interesting_ids: set[str]
    :param filter_refseq: when ``True``, filter RefSeq rows against ``interesting_ids``
    :type filter_refseq: bool
    :return: deduplicated DataFrame sorted by ``assembly_id``
    :rtype: pd.DataFrame
    """
    mask = build_interesting_membership_mask(parsed_df, interesting_ids, filter_refseq=filter_refseq)
    subset = parsed_df[mask].copy()
    sorted_subset = subset.sort_values("modification_date", ascending=False)
    deduped_subset = sorted_subset.drop_duplicates(subset="assembly_id", keep="first")
    return deduped_subset.sort_values("assembly_id")


def split_by_period(interesting_df: pd.DataFrame, mod_date: date) -> dict[Period, pd.DataFrame]:
    """Partition ``interesting_df`` into rows on/after ``mod_date`` vs before ``mod_date``.

    :param interesting_df: filtered assemblies DataFrame
    :type interesting_df: pd.DataFrame
    :param mod_date: modification date threshold
    :type mod_date: date
    :return: dictionary keyed by :class:`Period`
    :rtype: dict[Period, pd.DataFrame]
    """
    is_after = interesting_df["modification_date"] >= mod_date
    return {
        Period.AFTER: interesting_df[is_after].sort_values("assembly_id"),
        Period.BEFORE: interesting_df[~is_after].sort_values("assembly_id"),
    }


def split_by_source(df: pd.DataFrame) -> dict[Source, pd.DataFrame]:
    """Partition ``df`` by its ``source`` column.

    :param df: assemblies DataFrame
    :type df: pd.DataFrame
    :return: dictionary keyed by :class:`Source`
    :rtype: dict[Source, pd.DataFrame]
    """
    return {source: df[df["source"] == source] for source in Source}


def build_splits(interesting_df: pd.DataFrame, mod_date: date) -> Splits:
    """Build the four ``(Source, Period)`` DataFrame partitions.

    :param interesting_df: filtered assemblies DataFrame
    :type interesting_df: pd.DataFrame
    :param mod_date: modification date threshold
    :type mod_date: date
    :return: dictionary keyed by ``(Source, Period)``
    :rtype: Splits
    """
    by_period = split_by_period(interesting_df, mod_date)
    splits: Splits = {}
    for period, period_df in by_period.items():
        for source, source_df in split_by_source(period_df).items():
            splits[(source, period)] = source_df
    return splits


def save_tsv(df: pd.DataFrame, path: Path) -> None:
    """Write ``df`` as a tab-separated file at ``path``.

    :param df: DataFrame to write
    :type df: pd.DataFrame
    :param path: destination TSV file path
    :type path: Path
    """
    df.to_csv(path, sep="\t", index=False)
    logger.info("Wrote %d rows -> %s", len(df), path)


def batch(items: list[str], size: int) -> Iterable[list[str]]:
    """Yield contiguous chunks of ``items`` of length up to ``size``.

    :param items: list of strings to batch
    :type items: list[str]
    :param size: maximum batch size
    :type size: int
    :return: iterable of string batches
    :rtype: Iterable[list[str]]
    """
    for i in range(0, len(items), size):
        yield items[i : i + size]


def batch_count(n_items: int, batch_size: int) -> int:
    """Compute the number of batches needed for ``n_items`` at ``batch_size``.

    :param n_items: total item count
    :type n_items: int
    :param batch_size: items per batch
    :type batch_size: int
    :return: number of batches
    :rtype: int
    """
    return math.ceil(n_items / batch_size) if n_items else 0


def compute_batch_width(splits: Splits, batch_size: int) -> int:
    """Compute zero-padding digit width across all ``splits`` for ``batch_size``.

    :param splits: ``(Source, Period)`` DataFrame splits
    :type splits: Splits
    :param batch_size: items per batch
    :type batch_size: int
    :return: minimum zero-padding width (at least ``1``)
    :rtype: int
    """
    max_batches = max((batch_count(len(df), batch_size) for df in splits.values()), default=0)
    return max(len(str(max_batches)), 1)


def write_id_batches(ids: list[str], out_dir: Path, prefix: str, batch_size: int, width: int) -> int:
    """Write sequentially numbered batch text files of assembly IDs under ``out_dir``.

    :param ids: ordered assembly IDs
    :type ids: list[str]
    :param out_dir: output directory for batch files
    :type out_dir: Path
    :param prefix: file name prefix
    :type prefix: str
    :param batch_size: maximum IDs per batch file
    :type batch_size: int
    :param width: zero-padding digit width for batch file numbers
    :type width: int
    :return: number of batch files written
    :rtype: int
    """
    n_written = 0
    for i, chunk in enumerate(batch(ids, batch_size), start=1):
        file_num = str(i).zfill(width)
        out_path = out_dir / f"{prefix}-{file_num}.txt"
        out_path.write_text("\n".join(chunk) + "\n", encoding="utf-8")
        n_written += 1
    return n_written
