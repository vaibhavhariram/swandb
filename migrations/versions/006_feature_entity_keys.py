"""Add entity_keys to features table.

Revision ID: 006_entity_keys
Revises: 005_quality
Create Date: 2025-03-05

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "006_entity_keys"
down_revision: Union[str, None] = "005_quality"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "features",
        sa.Column("entity_keys", postgresql.JSONB(), nullable=False, server_default='["user_id"]'),
    )


def downgrade() -> None:
    op.drop_column("features", "entity_keys")
