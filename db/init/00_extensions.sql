-- Enable hypopg in the current database (usually POSTGRES_DB)
CREATE EXTENSION IF NOT EXISTS hypopg;

-- Also enable it in the default 'postgres' database to ensure utility commands work
\c postgres
CREATE EXTENSION IF NOT EXISTS hypopg;
