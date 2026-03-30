"""File system-related utilities."""

import re
from pathlib import Path
from re import Pattern

# Matches files like: name_00001.ext or name_00001.ext.gz
FILE_NAME_REGEX = re.compile(r"^\w+_(\d+)(\.\w+)+$")


class BatchCursor:
    """A batcher that can be used to retrieve batches of files from a directory."""

    def __init__(self, directory: str | Path, batch_size: int = 1, start_at: int = 1, end_at: int = 0) -> None:
        """Initialise a new directory batch cursor.

        :param directory: directory to retrieve files from
        :type directory: str | Path
        :param batch_size: number of files to return per invocation, defaults to 1
        :type batch_size: int, optional
        :param start_at: file number to start at, defaults to 1
        :type start_at: int, optional
        :param end_at: file number to end at, inclusive (i.e. if set to 15, file_0015.txt will be the last file)
                        Defaults to 0, which implies no end_at
        :type end_at: int, optional
        """
        errs = []
        if not isinstance(batch_size, int) or batch_size < 1:
            errs.append("batch_size must be an integer, 1 or greater")
        if not isinstance(start_at, int) or start_at < 0:
            errs.append("start_at must be an integer, 0 or greater")
        if not isinstance(end_at, int) or end_at < 0:
            errs.append("end_at must be an integer, 1 or greater")
        elif end_at > 0 and end_at < start_at:
            # end_at must be greater than start_at
            errs.append("end_at must be greater than start_at")
        if errs:
            err_msg = f"Error{'' if len(errs) == 1 else 's'} initialising BatchCursor:{'\n- '.join(errs)}\n"
            raise ValueError(err_msg)

        self.directory = Path(directory)
        self.batch_size: int = batch_size
        self.start_at: int = start_at
        self.end_at: int | None = end_at if end_at > 0 else None
        self.file_regex: Pattern[str] = FILE_NAME_REGEX

    def _get_sequence_number(self, path: Path) -> int:
        match = self.file_regex.match(path.name)
        return int(match.group(1))  # pyright: ignore[reportOptionalMemberAccess]

    def get_batch(self) -> list[Path]:
        """Return the next `batch_size` files whose sequence number >= start_at.

        Re-scans the directory on every call to pick up newly added files and
        updates `start_at` to the next file in the directory list.
        """
        if self.end_at and self.start_at > self.end_at:
            return []

        matched = sorted(p for p in self.directory.iterdir() if p.is_file() and self.file_regex.match(p.name))
        eligible = [
            p
            for p in matched
            if self._get_sequence_number(p) >= self.start_at
            and (self.end_at is None or self._get_sequence_number(p) <= self.end_at)
        ]

        batch = eligible[: self.batch_size]
        if batch:
            self.start_at = self._get_sequence_number(batch[-1]) + 1

        return batch
