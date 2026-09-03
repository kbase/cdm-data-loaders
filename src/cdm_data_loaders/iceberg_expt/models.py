"""
Pydantic models representing two successive versions of a fictional
"support ticket" API object, in the style of models generated from an
OpenAPI schema. V2 adds a new scalar column (`priority`) and a new
nested/struct column (`assignee`) relative to V1.
"""

import datetime

from pydantic import BaseModel, Field


class SupportTicketV1(BaseModel):
    """Initial schema: matches the first version of the OpenAPI spec."""

    ticket_id: int = Field(..., description="Unique identifier for the support ticket.")
    customer_id: int = Field(..., description="Identifier of the customer who raised the ticket.")
    subject: str = Field(..., description="Short summary of the ticket's subject.")
    status: str = Field(..., description="Current status of the ticket (open, in_progress, closed).")
    created_at: datetime.datetime = Field(..., description="Timestamp the ticket was created.")
    updated_at: datetime.datetime = Field(..., description="Timestamp the ticket was last updated.")


class Assignee(BaseModel):
    """New nested object introduced in the V2 schema."""

    agent_id: int = Field(..., description="Identifier of the support agent assigned to the ticket.")
    agent_name: str = Field(..., description="Display name of the assigned agent.")


class SupportTicketV2(BaseModel):
    """
    Evolved schema: adds `priority` (new scalar column) and `assignee`
    (new nested/struct column, nullable) relative to V1.
    """

    ticket_id: int = Field(..., description="Unique identifier for the support ticket.")
    customer_id: int = Field(..., description="Identifier of the customer who raised the ticket.")
    subject: str = Field(..., description="Short summary of the ticket's subject.")
    status: str = Field(..., description="Current status of the ticket (open, in_progress, closed).")
    created_at: datetime.datetime = Field(..., description="Timestamp the ticket was created.")
    updated_at: datetime.datetime = Field(..., description="Timestamp the ticket was last updated.")
    priority: str | None = Field(None, description="Priority level assigned to the ticket. Added in schema v2.")
    assignee: Assignee | None = Field(None, description="Agent assigned to the ticket, if any. Added in schema v2.")
