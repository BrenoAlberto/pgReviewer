# GitHub Actions Integration

The [README](../README.md#add-to-your-repo-in-one-step) covers the core workflow setup. This page documents optional configuration for advanced use cases.

---

## Always-comment mode

By default pgReviewer is silent on PRs that have never had findings — it only posts when there are issues, and updates to a ✅ pass state when existing findings are resolved. This avoids comment noise on PRs that touch Python or SQL files but have no database interaction.

Set `ALWAYS_COMMENT: "true"` in your workflow env to post a status comment on every PR regardless:

```yaml
jobs:
  review:
    env:
      ALWAYS_COMMENT: "true"   # useful for test-beds where silence = ambiguity
```

This makes it possible to tell "pgReviewer ran and found nothing" apart from "pgReviewer didn't run at all".

---

## Staging database connection patterns

See [ci-database-setup.md](ci-database-setup.md) for Docker sidecar, Cloud SQL Proxy, and direct connection examples.
