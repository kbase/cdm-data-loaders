"""Shared XML helper utilities used by XML readers and converters."""


def local_name(tag: str) -> str:
    """Return the local (namespace-stripped) part of an XML tag or attribute name.

    Handles Clark notation (``"{http://example.com}entry"`` -> ``"entry"``) and returns
    already-unprefixed names unchanged.

    :param tag: XML tag or attribute name, optionally in Clark notation.
    :type tag: str
    :return: local tag or attribute name without namespace braces.
    :rtype: str
    """
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) and "}" in tag else tag
