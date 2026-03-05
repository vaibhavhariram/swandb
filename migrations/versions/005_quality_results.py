"""Add quality_results table for parity validation.

Revision ID: 005_quality
Revises: 004_materialization
Create Date: 2025-03-05

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "005_quality"
down_revision: Union[str, None] = "004_materialization"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "quality_results",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_type", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("sample_size", sa.Integer(), nullable=True),
        sa.Column("mismatch_count", sa.Integer(), nullable=True),
        sa.Column("mismatch_rate", sa.Float(), nullable=True),
        sa.Column("threshold", sa.Float(), nullable=True),
        sa.Column("details", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_quality_results_tenant_id"),
        "quality_results",
        ["tenant_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_quality_results_tenant_id"), table_name="quality_results")
    op.drop_table("quality_results")
