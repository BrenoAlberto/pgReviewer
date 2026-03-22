# GitHub Actions Integration

The [README](../README.md#add-to-your-repo) covers the quick-start workflow. This page covers the full upgrade path and advanced options.

---

## Setup tiers

pgReviewer is zero-config by default. Add capabilities progressively:

| Tier | Requirements | What you get |
|---|---|---|
| **0 — Static analysis** | Workflow file only | Automatic static analysis on every PR push |
| **1 — Bot identity** | + [pgreviewer-ci app](https://github.com/apps/pgreviewer-ci) + `id-token: write` | Comments posted as `pgreviewer-ci[bot]` |
| **2 — LLM enriched** | + LLM API key secret | AI-generated fix suggestions |
| **3 — Full analysis** | + `database-url` + `issue_comment` trigger | On-demand `/pgr review` with EXPLAIN plans |

### Tier 0 — Zero-config static analysis

No secrets, no database, no app install required:

```yaml
name: pgReviewer

on:
  pull_request:
    types: [opened, synchronize]

permissions:
  contents: read
  issues: write
  pull-requests: write

jobs:
  pgreviewer:
    uses: BrenoAlberto/pgReviewer/.github/workflows/review.yml@main
```

pgReviewer posts findings as inline fix suggestions using your repository's `GITHUB_TOKEN`.

### Tier 1 — Bot identity

Install the [pgreviewer-ci GitHub App](https://github.com/apps/pgreviewer-ci) on your repository, then add `id-token: write` to your permissions:

```yaml
permissions:
  contents: read
  issues: write
  pull-requests: write
  id-token: write   # required for pgreviewer-ci[bot] identity
```

Comments will now appear from `pgreviewer-ci[bot]` instead of `github-actions[bot]`.

### Tier 2 — LLM-enriched analysis

Add one or more LLM secrets (**Settings → Secrets → Actions**) and forward them to the workflow:

```yaml
jobs:
  pgreviewer:
    uses: BrenoAlberto/pgReviewer/.github/workflows/review.yml@main
    secrets:
      ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}   # Anthropic (default)
      OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}         # OpenAI
      GEMINI_API_KEY: ${{ secrets.GEMINI_API_KEY }}         # Google Gemini
```

Only the secrets you define are used. Avoid `secrets: inherit` — it passes all repository secrets to the called workflow.

### Tier 3 — Full analysis with EXPLAIN

Add the `issue_comment` trigger, `checks: write` permission, and a `database-url` input:

```yaml
on:
  issue_comment:
    types: [created]
  pull_request:
    types: [opened, synchronize]

permissions:
  contents: read
  issues: write
  pull-requests: write
  checks: write
  id-token: write

jobs:
  pgreviewer:
    uses: BrenoAlberto/pgReviewer/.github/workflows/review.yml@main
    secrets:
      ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
    with:
      database-url: postgresql://user:pass@127.0.0.1:5432/mydb
      # run-migrations: true   # run alembic upgrade head before analysis
```

Reviewers can then trigger full EXPLAIN-based analysis by commenting `/pgr review` on any PR.

---

## LLM provider setup

pgReviewer supports Anthropic, OpenAI, and Gemini. Add the matching secret to your repository (**Settings → Secrets → Actions**), then pass it to the workflow:

```yaml
jobs:
  pgreviewer:
    uses: BrenoAlberto/pgReviewer/.github/workflows/review.yml@main
    secrets:
      ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
      OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
      GEMINI_API_KEY: ${{ secrets.GEMINI_API_KEY }}
    with:
      database-url: postgresql://user:pass@127.0.0.1:5432/mydb
```

Only the secrets explicitly declared in pgReviewer's workflow (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY`) are forwarded. Omit the ones you don't use. Avoid `secrets: inherit` — it passes all repository secrets to the called workflow and will flag security audits.

| Secret | Provider | Default model |
|---|---|---|
| `ANTHROPIC_API_KEY` | Anthropic | `claude-sonnet-4-5` |
| `OPENAI_API_KEY` | OpenAI | `gpt-4o` |
| `GEMINI_API_KEY` | Google Gemini | `gemini-2.0-flash` |

Once a secret is configured, reviewers can select a provider from the PR comment:

```
/pgr review                          # Anthropic (default)
/pgr review --model gpt-4o           # OpenAI
/pgr review --model gemini-2.0-flash # Gemini
```

The model flag is parsed from the comment body — no workflow changes required. Auto-inference maps model names to their provider (`claude-*` → Anthropic, `gpt-*`/`o1`/`o3`/`o4` → OpenAI, `gemini-*` → Gemini).

---

## On-demand trigger via PR comment

Use GitHub's `issue_comment` event and gate the job to PR comments that contain `/pgr review`:

```yaml
on:
  issue_comment:
    types: [created]

jobs:
  review:
    if: |
      github.event.issue.pull_request != '' &&
      contains(github.event.comment.body, '/pgr review')
```

The event payload does not include the PR head SHA directly. Fetch it before running analysis:

```bash
PR_NUMBER=${{ github.event.issue.number }}
HEAD_SHA=$(gh api repos/${{ github.repository }}/pulls/$PR_NUMBER --jq '.head.sha')
gh pr diff $PR_NUMBER > /tmp/pr.diff
```

Optional UX polish: add a 👍 reaction to the trigger comment so reviewers know the request was accepted.

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
