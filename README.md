# pgReviewer

A tool for PostgreSQL query analysis and optimization.

## Development Environment

### Prerequisites
- Docker & Docker Compose
- `uv` (for Python dependency management)

### Starting the Database
To start the development database with HypoPG pre-installed:

```bash
docker compose up -d
```

### Stopping the Database
To stop the services:

```bash
docker compose down
```

To wipe the database and start fresh (removes volumes):

```bash
docker compose down -v
```
