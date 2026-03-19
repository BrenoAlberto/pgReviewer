# GitHub Actions Integration

The [README](../README.md#add-to-your-repo-in-one-step) covers the core workflow setup. This page documents optional configuration for advanced use cases.

---

## LLM provider setup

pgReviewer supports Anthropic, OpenAI, and Gemini. Add the matching secret to your repository (**Settings → Secrets → Actions**), then pass it to the workflow:

```yaml
jobs:
  pgreviewer:
    uses: BrenoAlberto/pgReviewer/.github/workflows/review.yml@main
    secrets: inherit          # forwards ANTHROPIC_API_KEY / OPENAI_API_KEY / GEMINI_API_KEY
    with:
      database-url: postgresql://user:pass@127.0.0.1:5432/mydb
```

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
