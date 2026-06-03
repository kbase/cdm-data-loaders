"""Shared S3 test helpers.

# NOTE: Moto currently does not support CRC64NVME; remove this helper when it does.
"""

import functools
from collections.abc import Callable
from typing import Any


def strip_checksum_algorithm(method: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a boto3 S3 method to remove the ChecksumAlgorithm argument before calling moto.

    Moto does not implement CRC64NVME checksums, so any call that includes
    ChecksumAlgorithm='CRC64NVME' would fail. This wrapper silently drops the
    argument so the rest of the call proceeds normally against the moto backend.
    """

    @functools.wraps(method)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        """Remove the ChecksumAlgorithm argument from the call."""
        kwargs.pop("ChecksumAlgorithm", None)
        return method(*args, **kwargs)

    return wrapper
