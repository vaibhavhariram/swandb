"""Add idempotency_key, event_count, max_event_ts, max_ingest_ts to ingestion_jobs.

Revision ID: 003_ingestion
Revises: 002_transform_spec
Create Date: 2025-03-05

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "003_ingestion"
down_revision: Union[str, None] = "002_transform_spec"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "ingestion_jobs",
        sa.Column("idempotency_key", sa.Text(), nullable=True),
    )
    op.add_column(
        "ingestion_jobs",
        sa.Column("event_count", sa.Integer(), nullable=True),
    )
    op.add_column(
        "ingestion_jobs",
        sa.Column("max_event_ts", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "ingestion_jobs",
        sa.Column("max_ingest_ts", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_ingestion_jobs_tenant_source_idempotency",
        "ingestion_jobs",
        ["tenant_id", "source_id", "idempotency_key"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_ingestion_jobs_tenant_source_idempotency",
        table_name="ingestion_jobs",
    )
    op.drop_column("ingestion_jobs", "max_ingest_ts")
    op.drop_column("ingestion_jobs", "max_event_ts")
    op.drop_column("ingestion_jobs", "event_count")
    op.drop_column("ingestion_jobs", "idempotency_key")
