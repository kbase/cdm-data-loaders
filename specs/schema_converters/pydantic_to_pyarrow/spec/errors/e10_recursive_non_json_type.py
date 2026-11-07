"""E10 fixture: Recursive BaseModel containing a non-JSON-serializable date field."""

import datetime as dt

from pydantic import BaseModel


class DatedTreeNode(BaseModel):
    name: str
    created_on: dt.date
    child: "DatedTreeNode | None" = None


class DatedTreeRoot(BaseModel):
    root: DatedTreeNode


MODEL = DatedTreeRoot
