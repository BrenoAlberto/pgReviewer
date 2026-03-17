-- Replace single-column index with tenant-leading composite indexes.
DROP INDEX IF EXISTS idx_events_user_id;

CREATE INDEX idx_events_tenant_user_id ON events (tenant_id, user_id);
CREATE INDEX idx_events_tenant_created_at ON events (tenant_id, created_at DESC);
