ARG PG_VERSION=16
FROM postgres:${PG_VERSION}

# Re-declare ARG after FROM so it overrides the postgres image's PG_VERSION env
# var (which contains the full version string like "17.9-1.pgdg13+1") and keeps
# just the major version needed for the apt package name.
ARG PG_VERSION=16
RUN apt-get update && apt-get install -y \
    postgresql-${PG_VERSION}-hypopg \
    && rm -rf /var/lib/apt/lists/*
