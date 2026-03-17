# Documentation

## Guides

- **[Getting Started](getting-started.md)** — Installation, database setup, first analysis
- **[CI Database Setup](ci-database-setup.md)** — Connect to staging in CI (Docker sidecar, Cloud SQL Proxy, direct)
- **[Configuration](configuration.md)** — `.pgreviewer.yml`, environment variables, thresholds

## Reference

- **[Issue Detectors](detectors.md)** — All 16 built-in detectors (EXPLAIN, migration safety, code patterns) and the custom detector API
- **[Analysis Pipeline](analysis.md)** — How `pgr check` and `pgr diff` work under the hood

## Diagrams

SVG assets in [`assets/`](assets/):

| Diagram | Description |
|---|---|
| [pipeline.svg](assets/pipeline.svg) | Full analysis pipeline — both entry points |
| [architecture.svg](assets/architecture.svg) | System architecture overview |
| [hypopg-flow.svg](assets/hypopg-flow.svg) | HypoPG validation decision flow |
| [detector-architecture.svg](assets/detector-architecture.svg) | Pluggable detector system |
