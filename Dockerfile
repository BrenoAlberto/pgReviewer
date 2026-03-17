FROM python:3.12-slim AS builder

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml /app/pyproject.toml
COPY pgreviewer /app/pgreviewer
RUN printf "# pgreviewer\n" > /app/README.md

RUN uv pip install --system /app

FROM python:3.12-slim

WORKDIR /app

LABEL org.opencontainers.image.title="pgReviewer" \
      org.opencontainers.image.description="Automated PostgreSQL query performance review for pull requests." \
      org.opencontainers.image.source="https://github.com/BrenoAlberto/pgReviewer" \
      org.opencontainers.image.licenses="MIT"

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin/pgr /usr/local/bin/pgr
COPY pgreviewer /app/pgreviewer
COPY pyproject.toml /app/pyproject.toml

ENTRYPOINT ["pgr"]
