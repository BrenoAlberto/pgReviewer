"""fix indexes from initial migration for pgReviewer demo"""

from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_events_account_id",
        "events",
        ["account_id"],
        unique=False,
        postgresql_concurrently=True,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_events_account_id",
        table_name="events",
        postgresql_concurrently=True,
    )
