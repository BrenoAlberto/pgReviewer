# Dogfooding pgReviewer

pgReviewer analyzes its own migrations on every PR that touches `db/migrations/`.
This document records the first self-review run.

---

## What was tested

PR #240 (branch `feature/5.1.8-dogfood`) introduced `db/migrations/0001_initial_schema.sql`.
The migration creates four tables (`users`, `products`, `orders`, `order_items`) and adds
foreign key constraints via `ALTER TABLE` — **without** creating indexes on the FK columns.

This is the classic "missing FK index" pattern: Postgres does not automatically create an
index when a foreign key is declared, which causes sequential scans on JOIN and ON DELETE
operations.

---

## First run — before the fix

pgReviewer detected **3 CRITICAL issues** via the `add_foreign_key_without_index` detector:

| Table | FK column | Issue |
|---|---|---|
| `orders` | `user_id` | FK to `users(id)` has no index |
| `order_items` | `order_id` | FK to `orders(id)` has no index |
| `order_items` | `product_id` | FK to `products(id)` has no index |

For each issue, pgReviewer generated the remediation SQL:

```sql
CREATE INDEX CONCURRENTLY idx_orders_user_id ON orders (user_id);
CREATE INDEX CONCURRENTLY idx_order_items_order_id ON order_items (order_id);
CREATE INDEX CONCURRENTLY idx_order_items_product_id ON order_items (product_id);
```

The check run concluded as **`failure`** and the PR was blocked at the `critical` threshold.

---

## Second run — after adding the fix migration

Adding `db/migrations/0002_add_missing_indexes.sql` with the three `CREATE INDEX CONCURRENTLY`
statements above re-ran pgReviewer. The detector found no further issues:

- 0 CRITICAL · 0 WARNING · 0 INFO
- Check run concluded as **`success`**
- The PR comment was updated in-place (same comment, new body via PATCH)

---

## How the self-review workflow works

`.github/workflows/pgreviewer-dogfood.yml` runs on every PR touching `db/migrations/**.sql`:

1. Builds the local `db/Dockerfile` image (Postgres 16 + HypoPG).
2. Starts the image as a Docker container exposed on `127.0.0.1:5432`.
3. Enables the `hypopg` extension.
4. Downloads the PR unified diff via `gh pr diff`.
5. Runs `pgr diff /tmp/pr.diff --ci` against the sidecar database.

The `--ci` flag exits with code 1 when a CRITICAL issue is found, failing the check.

---

## Outcome

- pgReviewer successfully reviewed its own migration and caught real bugs.
- The `add_foreign_key_without_index` detector found all three missing FK indexes.
- The fix migration (`0002`) cleared all findings and the check passed.
- See PR [#240](https://github.com/BrenoAlberto/pgReviewer/pull/240) for the live
  check-run history.
