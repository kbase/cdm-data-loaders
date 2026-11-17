"""Parser for UniProt release notes files."""

import datetime as dt
import re
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Final

RELEASE_VERSION_DATE: Final[re.Pattern[str]] = re.compile(
    r"is pleased to announce UniProt Knowledgebase \(UniProtKB\) Release\s+(\w+) \((\d{1,2}-[a-zA-Z]+-\d{4})\)\."
)

UNIPROT_TREMBL_STATS: Final[re.Pattern[str]] = re.compile(
    r"UniProtKB Release \w+ consists of ([\d,]+) entries \(UniProtKB/Swiss-Prot:\n([\d,]+) entries and UniProtKB/TrEMBL: ([\d,]+) entries\)"
)

RELEASE_STATS: Final[re.Pattern[str]] = re.compile(r"(\w+) Release .*? consists of ([\d,]+) entries")

DATE_FORMAT: Final[str] = "%d-%b-%Y"
EXPECTED_RELNOTES_PARTS: Final[int] = 2

logger: Logger = getLogger(__name__)


def parse_relnotes(relnotes_path: Path) -> dict[str, Any]:
    """Read and parse a UniProt release notes file from disk.

    :param relnotes_path: path to the release notes file
    :type relnotes_path: Path
    :return: parsed release version, UTC publication date, and entry counts
    :rtype: dict[str, Any]
    """
    rel_text = relnotes_path.read_text(encoding="utf-8")
    return parse(rel_text)


def parse(relnotes: str) -> dict[str, Any]:
    """Parse UniProt release notes text into version, UTC publication date, and entry counts.

    :param relnotes: contents of the release notes file as a string
    :type relnotes: str
    :return: key-value pairs with release metadata and entry counts
    :rtype: dict[str, Any]
    :raises RuntimeError: if expected release headers or UniRef entry counts are missing
    """
    errors: list[str] = []
    stats: dict[str, Any] = {}

    relnotes_parts = relnotes.strip().split("\n\n", 1)
    if len(relnotes_parts) != EXPECTED_RELNOTES_PARTS:
        msg = "Could not find double line break. Relnotes file format may have changed."
        logger.error(msg)
        raise RuntimeError(msg)

    intro_str, stats_str = relnotes_parts
    intro_str = intro_str.replace("\n", " ")

    rv = re.search(RELEASE_VERSION_DATE, intro_str)
    if not rv:
        errors.append("Could not find text matching the release version date regex.")
    else:
        stats["version"] = rv.groups()[0]
        stats["date_published"] = dt.datetime.strptime(rv.groups()[1], DATE_FORMAT).replace(tzinfo=dt.UTC)

    uniprot_trembl = re.search(UNIPROT_TREMBL_STATS, stats_str)
    if not uniprot_trembl:
        errors.append("Could not find text matching the UniProt/TrEMBL stats regex.")
    else:
        numbers = uniprot_trembl.groups()
        stats["UniProtKB"] = numbers[0]
        stats["UniProtKB/Swiss-Prot"] = numbers[1]
        stats["UniProtKB/TrEMBL"] = numbers[2]

    all_releases = re.findall(RELEASE_STATS, stats_str)
    if not all_releases:
        errors.append("Could not find text matching the release stats regex.")
    else:
        for db, number in all_releases:
            if db not in stats:
                stats[db] = number

    errors.extend([f"No stats for UniRef{n} found." for n in ("50", "90", "100") if f"UniRef{n}" not in stats])

    if errors:
        logger.error("\n".join(errors))
        raise RuntimeError("\n".join(errors))

    return stats
