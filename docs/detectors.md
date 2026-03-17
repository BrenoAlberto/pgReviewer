# Issue Detectors

pgReviewer ships three families of detectors. All findings include a `severity`,
a `description`, and a copy-ready `suggested_action`.

---

## EXPLAIN-based detectors

These run after `EXPLAIN (FORMAT JSON)` is executed against your database.
They analyze the parsed plan tree and cross-reference schema metadata
(`pg_class`, `pg_indexes`, `pg_stats`).

| Detector | Severity | What it catches |
|---|---|---|
| `sequential_scan` | WARNING / CRITICAL | Seq scan on tables above the row threshold |
| `missing_index_on_filter` | WARNING | Filter condition with no supporting index |
| `nested_loop_large_outer` | WARNING / CRITICAL | Nested loop join with a large outer relation |
| `high_cost` | WARNING / CRITICAL | Total plan cost exceeds the cost threshold |
| `sort_without_index` | WARNING | Sort node that could be served by an index |
| `cartesian_join` | **always CRITICAL** | Join without a condition (cross product) |

Thresholds are configurable — see [configuration.md](configuration.md).

---

## Migration safety detectors

These run against the raw SQL text of migration statements — no database connection
required. They catch patterns that are safe in development but dangerous on a
production table.

| Detector | Severity | What it catches |
|---|---|---|
| `add_foreign_key_without_index` | **always CRITICAL** | FK column with no supporting index — causes seq scans on joins and ON DELETE |
| `add_not_null_without_default` | CRITICAL | Adding NOT NULL to an existing column without a default rewrites the table |
| `add_column_with_default` | WARNING | Non-trivial default on an existing column rewrites the table (pre-PG11) |
| `create_index_not_concurrently` | WARNING | `CREATE INDEX` without `CONCURRENTLY` holds a write lock |
| `alter_column_type` | CRITICAL | Changing a column type rewrites the table; safe widening (e.g. varchar) excluded |
| `destructive_ddl` | WARNING | `DROP TABLE`, `DROP COLUMN`, `TRUNCATE` |
| `large_table_ddl` | WARNING | Any DDL on a table above the row count threshold |
| `drop_column_referenced` | CRITICAL | Column being dropped is still referenced in queries found in the same diff |

Each migration detector receives the full list of statements in the file, so
`add_foreign_key_without_index` correctly suppresses its finding when a matching
`CREATE INDEX CONCURRENTLY` appears later in the same migration.

---

## Code pattern detectors

These analyze Python source code via tree-sitter — no database required.

| Detector | Severity | What it catches |
|---|---|---|
| `query_in_loop` | WARNING / CRITICAL | DB call directly inside a `for` / `while` loop |
| cross-file N+1 | WARNING | Loop in a service method that calls a repository function which executes a query |
| SQLAlchemy model diff | WARNING / CRITICAL | Removed indexes, missing FK indexes detected via model comparison |

For cross-file N+1, pgReviewer builds a query catalog by scanning the codebase for
functions that execute queries, then traces call chains up to two levels deep.

---

## Writing a custom EXPLAIN detector

Add a file to `pgreviewer/analysis/issue_detectors/`:

```python
from pgreviewer.analysis.issue_detectors import BaseDetector
from pgreviewer.analysis.plan_parser import walk_nodes
from pgreviewer.core.models import ExplainPlan, Issue, SchemaInfo, Severity


class LargeHashBuildDetector(BaseDetector):
    @property
    def name(self) -> str:
        return "large_hash_build"

    def detect(self, plan: ExplainPlan, schema: SchemaInfo) -> list[Issue]:
        issues = []
        for node in walk_nodes(plan):
            if node.node_type == "Hash" and (node.plan_rows or 0) > 100_000:
                issues.append(Issue(
                    severity=Severity.WARNING,
                    detector_name=self.name,
                    description=f"Hash build on {node.plan_rows:,} rows",
                    suggested_action="Consider a merge join or adding a filter",
                ))
        return issues
```

Detected automatically — no registration needed.

## Writing a custom migration detector

Add a file to `pgreviewer/analysis/migration_detectors/`:

```python
from pgreviewer.analysis.migration_detectors import BaseMigrationDetector
from pgreviewer.core.models import Issue, ParsedMigration, SchemaInfo, Severity


class NoTransactionDDLDetector(BaseMigrationDetector):
    @property
    def name(self) -> str:
        return "ddl_outside_transaction"

    def detect(self, migration: ParsedMigration, schema: SchemaInfo) -> list[Issue]:
        issues = []
        for stmt in migration.statements:
            if stmt.statement_type == "ALTER TABLE" and "BEGIN" not in migration.raw:
                issues.append(Issue(
                    severity=Severity.WARNING,
                    detector_name=self.name,
                    description="ALTER TABLE outside explicit transaction",
                    suggested_action="Wrap in BEGIN/COMMIT for atomicity",
                    context={"line_number": stmt.line_number},
                ))
        return issues
```

## Disabling detectors

Via `.pgreviewer.yml`:

```yaml
rules:
  high_cost:
    enabled: false
  large_table_ddl:
    severity: info   # downgrade instead of disable
```

Via environment variable:

```bash
DISABLED_DETECTORS='["high_cost", "large_table_ddl"]'
```
