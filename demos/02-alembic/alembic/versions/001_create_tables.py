"""create initial tables with intentional migration issues for pgReviewer demo"""

import sqlalchemy as sa
from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "accounts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("email", sa.String(length=255), nullable=False, unique=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
    )

    op.create_table(
        "events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("payload", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
    )

    op.execute(
        """
        ALTER TABLE events
        ADD CONSTRAINT events_account_id_fkey
        FOREIGN KEY (account_id)
        REFERENCES accounts (id)
        """
    )

    op.execute(
        """
        INSERT INTO accounts (id, email)
        VALUES (1, 'demo@example.com')
        """
    )

    # Seed enough rows to model this as a table that already has data.
    op.execute(
        """
        INSERT INTO events (account_id, event_type, payload)
        SELECT 1, 'signup', '{}'::text
        FROM generate_series(1, 250000)
        """
    )

    # Intentionally missing postgresql_concurrently=True for demo warning.
    op.create_index("ix_events_created_at", "events", ["created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_events_created_at", table_name="events")
    op.drop_table("events")
    op.drop_table("accounts")
