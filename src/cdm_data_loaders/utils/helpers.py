"""Misc database-related helpers."""


def _ensembl_type(xref: str) -> str:
    """Given an Ensembl ID, return the likely type."""
    prefixes = {
        "ENST": "transcript",
        "ENSP": "protein sequence",
        "ENSG": "gene sequence",
        "ENSE": "exon",
        "ENSFM": "protein family",
        "ENSGT": "gene tree",
        "ENSR": "regulatory feature",
    }

    return prefixes.get(xref[0:4], prefixes.get(xref[0:5], "sequence"))

