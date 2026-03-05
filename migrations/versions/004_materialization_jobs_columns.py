"""Add range_start, range_end, feature_refs, event_count, feature_row_count to materialization_jobs.

Revision ID: 004_materialization
Revises: 003_ingestion
Create Date: 2025-03-05

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "004_materialization"
down_revision: Union[str, None] = "003_ingestion"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "materialization_jobs",
        sa.Column("range_start", sa.Date(), nullable=True),
    )
    op.add_column(
        "materialization_jobs",
        sa.Column("range_end", sa.Date(), nullable=True),
    )
    op.add_column(
        "materialization_jobs",
        sa.Column("feature_refs", postgresql.JSONB(), nullable=True),
    )
    op.add_column(
        "materialization_jobs",
        sa.Column("event_count", sa.Integer(), nullable=True),
    )
    op.add_column(
        "materialization_jobs",
        sa.Column("feature_row_count", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("materialization_jobs", "feature_row_count")
    op.drop_column("materialization_jobs", "event_count")
    op.drop_column("materialization_jobs", "feature_refs")
    op.drop_column("materialization_jobs", "range_end")
    op.drop_column("materialization_jobs", "range_start")
