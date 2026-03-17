"""Add email column to users table."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision = "0005"
down_revision = "0004"


def upgrade():
    op.add_column("users", sa.Column("email", sa.Text(), nullable=True))
    op.execute("CREATE INDEX CONCURRENTLY idx_users_email ON users (email)")
    op.execute(
        text("UPDATE users SET email = 'unknown@example.com' WHERE email IS NULL")
    )


def downgrade():
    op.drop_index("idx_users_email", table_name="users")
    op.drop_column("users", "email")
