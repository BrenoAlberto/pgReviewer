CREATE TABLE events (
    id         BIGSERIAL PRIMARY KEY,
    tenant_id  UUID NOT NULL,
    user_id    BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Missing: composite indexes on (tenant_id, user_id) and (tenant_id, created_at)
CREATE INDEX idx_events_user_id ON events (user_id);  -- wrong: tenant_id not leading
