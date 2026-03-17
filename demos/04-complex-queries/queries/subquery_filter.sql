SELECT
    c.id,
    c.email,
    c.region
FROM customers AS c
WHERE (
    SELECT COUNT(*)
    FROM orders AS o
    WHERE o.customer_id = c.id
      AND o.status = 'paid'
      AND o.created_at >= CURRENT_DATE - INTERVAL '90 days'
) >= 3;
