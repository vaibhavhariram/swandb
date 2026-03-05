"""Add transform_spec and spec_hash to feature_versions.

Revision ID: 002_transform_spec
Revises: 001_initial
Create Date: 2025-03-04

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "002_transform_spec"
down_revision: Union[str, None] = "001_initial"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Hash of canonical_json({}) for default/backfill
_DEFAULT_SPEC_HASH = "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"


def upgrade() -> None:
    op.add_column(
        "feature_versions",
        sa.Column("transform_spec", postgresql.JSONB(), nullable=False, server_default="{}"),
    )
    op.add_column(
        "feature_versions",
        sa.Column("spec_hash", sa.Text(), nullable=False, server_default=_DEFAULT_SPEC_HASH),
    )
    op.alter_column(
        "feature_versions",
        "spec_hash",
        server_default=None,
    )
    op.alter_column(
        "feature_versions",
        "transform_spec",
        server_default=None,
    )
    op.alter_column(
        "feature_versions",
        "schema",
        existing_type=postgresql.JSONB(),
        nullable=True,
    )
    op.create_index(
        op.f("ix_feature_versions_spec_hash"),
        "feature_versions",
        ["spec_hash"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_feature_versions_spec_hash"), table_name="feature_versions")
    op.alter_column(
        "feature_versions",
        "schema",
        existing_type=postgresql.JSONB(),
        nullable=False,
    )
    op.drop_column("feature_versions", "spec_hash")
    op.drop_column("feature_versions", "transform_spec")
