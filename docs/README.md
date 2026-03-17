# Documentation

## Guides

- **[Getting Started](getting-started.md)** — Installation, setup, and your first analysis
- **[CI Database Setup](ci-database-setup.md)** — Connect pgReviewer to staging in CI (direct, Docker sidecar, Cloud SQL Proxy)
- **[Configuration](configuration.md)** — All settings, thresholds, and environment variables

## Reference

- **[Analysis Pipeline](analysis.md)** — How the multi-stage analysis engine works
- **[Issue Detectors](detectors.md)** — Built-in detectors and how to write custom ones
- **[Debug Store](debug-store.md)** — Artifact persistence and inspection

## Architecture

<p align="center">
  <img src="assets/architecture.svg" alt="System Architecture" width="700" />
</p>

## Diagrams

All diagrams are SVG files in [`docs/assets/`](assets/):

| Diagram | Description |
|---------|-------------|
| [pipeline.svg](assets/pipeline.svg) | Analysis pipeline flow |
| [architecture.svg](assets/architecture.svg) | System architecture overview |
| [hypopg-flow.svg](assets/hypopg-flow.svg) | HypoPG validation decision flow |
| [detector-architecture.svg](assets/detector-architecture.svg) | Pluggable detector system |
