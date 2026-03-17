-- Typical multi-tenant point lookup: scoped by tenant + resource id.
SELECT id, event_type, created_at
FROM events
WHERE tenant_id = '11111111-1111-1111-1111-111111111111'
  AND user_id = 42
ORDER BY created_at DESC
LIMIT 50;

-- Tenant-scoped time-window query.
SELECT id, user_id, event_type
FROM events
WHERE tenant_id = '11111111-1111-1111-1111-111111111111'
  AND created_at >= NOW() - INTERVAL '7 days'
ORDER BY created_at DESC
LIMIT 100;
