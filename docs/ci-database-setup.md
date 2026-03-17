# CI Database Setup

Connecting pgReviewer to a staging database is the most important setup step. This guide covers three patterns from simplest to most secure.

> **In a hurry?** Jump to [Pattern 1 (Direct Connection)](#pattern-1-direct-connection) — it works with any hosted Postgres (RDS, Cloud SQL, Supabase, Neon) in under 10 minutes.

---

## Minimum Required Permissions

Regardless of how you connect, the database user needs these privileges. Run this SQL once on your staging database:

```sql
-- Create a dedicated read-only CI role
CREATE ROLE pgreviewer_ci WITH LOGIN PASSWORD 'change-me';

-- pgReviewer reads system catalogs to collect schema info
GRANT pg_read_all_stats TO pgreviewer_ci;         -- pg_stat_* views (Postgres 10+)
GRANT SELECT ON ALL TABLES IN SCHEMA pg_catalog TO pgreviewer_ci;
GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO pgreviewer_ci;

-- Access to your application schema(s)
GRANT USAGE ON SCHEMA public TO pgreviewer_ci;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pgreviewer_ci;

-- HypoPG requires CREATE TEMPORARY on the session to install hypothetical indexes
-- HypoPG operations always run inside a rolled-back transaction — no data is persisted
GRANT TEMPORARY ON DATABASE your_db_name TO pgreviewer_ci;

-- If using pg_stat_statements (recommended for slow-query detection)
GRANT SELECT ON pg_stat_statements TO pgreviewer_ci;
```

> **Note on HypoPG**: pgReviewer uses HypoPG to validate index recommendations. The extension must be installed on the target database (`CREATE EXTENSION IF NOT EXISTS hypopg;`). The `TEMPORARY` grant lets the session load the extension in a sandboxed transaction that is always rolled back.

---

## Pattern 1: Direct Connection

**Best for**: Teams with an existing staging database on RDS, Cloud SQL, Supabase, or Neon.

### Step 1 — Add the `DATABASE_URL` secret

In your GitHub repository: **Settings → Secrets and variables → Actions → New repository secret**

```
Name:  DATABASE_URL
Value: postgresql://pgreviewer_ci:change-me@your-staging-host:5432/your_db
```

For Supabase, the URL is under **Project Settings → Database → Connection string (URI)**.
For Neon, find it under **Dashboard → Connection Details → Connection string**.

### Step 2 — Ensure the database is reachable from GitHub Actions

GitHub Actions runners use IP ranges published at `https://api.github.com/meta` (field `actions`). If your database is inside a VPC or behind a firewall, add those CIDR blocks to the allowlist, or use a bastion / VPN tunnel step.

For public-internet-accessible staging databases (Supabase, Neon, Render), no firewall changes are needed.

### Step 3 — Verify HypoPG is installed

```sql
-- Run on your staging database
CREATE EXTENSION IF NOT EXISTS hypopg;
SELECT extname FROM pg_extension WHERE extname = 'hypopg';
-- Should return: hypopg
```

### Step 4 — Add the workflow

```yaml
# .github/workflows/pgreviewer.yml
name: pgreviewer

on:
  pull_request:
    paths:
      - "**.sql"
      - "**/migrations/**"
      - "**/models.py"
      - "**/models/**/*.py"

jobs:
  review-sql:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: BrenoAlberto/pgReviewer@v5
        with:
          db_connection: ${{ secrets.DATABASE_URL }}
```

That's it. pgReviewer will connect, run `EXPLAIN` plans against your staging schema, and post the results as a PR comment.

---

## Pattern 2: Docker Sidecar

**Best for**: Teams that want an isolated, ephemeral database with a known schema subset — no shared staging DB required.

GitHub Actions supports running a containerized Postgres instance alongside your job via the `services:` block. The container is available at `localhost:5432` for the duration of the job.

### Option A — Minimal (empty database)

Use this if pgReviewer only needs to parse `EXPLAIN` plans from a fresh schema (no data required for plan cost estimates).

```yaml
# .github/workflows/pgreviewer.yml
name: pgreviewer

on:
  pull_request:
    paths:
      - "**.sql"
      - "**/migrations/**"
      - "**/models.py"
      - "**/models/**/*.py"

jobs:
  review-sql:
    runs-on: ubuntu-latest

    services:
      postgres:
        image: ghcr.io/crystaldba/postgres-mcp:latest  # Postgres 16 + HypoPG + pg_stat_statements
        env:
          POSTGRES_USER: pgreviewer_ci
          POSTGRES_PASSWORD: pgreviewer_ci
          POSTGRES_DB: review_db
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 5s
          --health-timeout 5s
          --health-retries 10

    steps:
      - uses: actions/checkout@v4

      # Apply your schema migrations so the planner knows the table structure
      - name: Apply schema
        run: psql $DATABASE_URL -f db/schema.sql
        env:
          DATABASE_URL: postgresql://pgreviewer_ci:pgreviewer_ci@localhost:5432/review_db

      - uses: BrenoAlberto/pgReviewer@v5
        with:
          db_connection: postgresql://pgreviewer_ci:pgreviewer_ci@localhost:5432/review_db
```

> **Why crystaldba/postgres-mcp image?** It ships Postgres 16 with HypoPG and pg_stat_statements pre-installed. You can substitute the official `postgres:16` image and run `CREATE EXTENSION hypopg;` in a setup step.

### Option B — Seeded schema (representative statistics)

`EXPLAIN` cost estimates depend on table statistics. For meaningful recommendations, seed a representative row count:

```yaml
    steps:
      - uses: actions/checkout@v4

      - name: Apply schema and seed data
        env:
          DATABASE_URL: postgresql://pgreviewer_ci:pgreviewer_ci@localhost:5432/review_db
        run: |
          psql $DATABASE_URL -f db/schema.sql
          # Seed a subset of prod data (e.g. 50K rows) from a fixture or anonymized dump
          psql $DATABASE_URL -f db/fixtures/seed_ci.sql
          # Run ANALYZE so the planner has fresh stats
          psql $DATABASE_URL -c "ANALYZE;"

      - uses: BrenoAlberto/pgReviewer@v5
        with:
          db_connection: postgresql://pgreviewer_ci:pgreviewer_ci@localhost:5432/review_db
```

### Granting minimum permissions on the sidecar

If you use a superuser (`POSTGRES_USER: postgres`) for setup but a restricted user for pgReviewer, run the permission SQL from the [Minimum Required Permissions](#minimum-required-permissions) section in a setup step:

```yaml
      - name: Grant pgreviewer permissions
        run: psql postgresql://postgres:postgres@localhost:5432/review_db -f db/ci/pgreviewer_permissions.sql
```

---

## Pattern 3: Cloud SQL Proxy (Google Cloud)

**Best for**: Teams using Google Cloud SQL who want pgReviewer to connect to the same staging instance as their app — without exposing it to the public internet.

The [google-github-actions/cloud-sql-proxy](https://github.com/google-github-actions/cloud-sql-proxy) action starts the Cloud SQL Auth Proxy as a background step, making the database available at `127.0.0.1:5432`.

### Step 1 — Create a GCP service account for CI

```bash
# Create the service account
gcloud iam service-accounts create pgreviewer-ci \
  --display-name "pgReviewer CI"

# Grant Cloud SQL Client role (allows proxy connections)
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:pgreviewer-ci@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/cloudsql.client"

# Create and download a JSON key
gcloud iam service-accounts keys create pgreviewer-ci-key.json \
  --iam-account=pgreviewer-ci@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

### Step 2 — Add secrets to GitHub

| Secret name | Value |
|---|---|
| `GCP_CREDENTIALS` | Contents of `pgreviewer-ci-key.json` |
| `DATABASE_URL` | `postgresql://pgreviewer_ci:change-me@127.0.0.1:5432/your_db` |

> Use `127.0.0.1` not `localhost` — on Linux runners, `localhost` resolves to a Unix socket path which the proxy does not serve.

### Step 3 — Add the workflow

```yaml
# .github/workflows/pgreviewer.yml
name: pgreviewer

on:
  pull_request:
    paths:
      - "**.sql"
      - "**/migrations/**"
      - "**/models.py"
      - "**/models/**/*.py"

jobs:
  review-sql:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - uses: google-github-actions/auth@v2
        with:
          credentials_json: ${{ secrets.GCP_CREDENTIALS }}

      - uses: google-github-actions/cloud-sql-proxy@v2
        with:
          instance: YOUR_PROJECT_ID:REGION:INSTANCE_NAME=tcp:5432

      - uses: BrenoAlberto/pgReviewer@v5
        with:
          db_connection: ${{ secrets.DATABASE_URL }}
```

### IAM permissions summary

| Permission | Why it's needed |
|---|---|
| `roles/cloudsql.client` | Allows the Cloud SQL Auth Proxy to open a connection to the instance |
| Database-level grants | See [Minimum Required Permissions](#minimum-required-permissions) above |

---

## Troubleshooting

### `HypoPG extension not found`

Install it on your database:
```sql
CREATE EXTENSION IF NOT EXISTS hypopg;
```
For RDS, enable the `hypopg` parameter group option and reboot. For Cloud SQL, enable the `hypopg` flag in **Edit instance → Database flags**.

### `permission denied for table pg_stat_statements`

```sql
GRANT SELECT ON pg_stat_statements TO pgreviewer_ci;
-- If the extension is not installed:
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```

### `connection refused` on Cloud SQL Proxy

Ensure you use `127.0.0.1:5432` not `localhost:5432` in `DATABASE_URL`. On Linux, `localhost` resolves to a Unix socket by default.

### `SSL connection required`

Append `?sslmode=require` to the connection string:
```
postgresql://pgreviewer_ci:change-me@host:5432/db?sslmode=require
```

---

## See Also

- [Getting Started](getting-started.md) — Local development setup
- [Configuration](configuration.md) — All pgReviewer environment variables
- [GitHub Action example](examples/pgreviewer-action.yml) — Minimal workflow file
