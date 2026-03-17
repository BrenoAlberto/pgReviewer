SELECT
    e.customer_id,
    e.event_type,
    e.occurred_at,
    ROW_NUMBER() OVER (
        PARTITION BY e.customer_id
        ORDER BY e.occurred_at DESC
    ) AS event_rank,
    SUM(e.duration_ms) OVER (
        PARTITION BY e.customer_id
    ) AS customer_duration_ms
FROM event_logs AS e
WHERE e.occurred_at >= NOW() - INTERVAL '30 days';
