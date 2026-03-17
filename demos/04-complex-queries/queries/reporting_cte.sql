WITH recent_orders AS (
    SELECT
        o.id,
        o.customer_id,
        o.created_at
    FROM orders AS o
    WHERE o.created_at >= CURRENT_DATE - INTERVAL '180 days'
),
order_totals AS (
    SELECT
        ro.customer_id,
        ro.id AS order_id,
        SUM(oi.quantity * oi.unit_price) AS order_total
    FROM recent_orders AS ro
    JOIN order_items AS oi ON oi.order_id = ro.id
    GROUP BY ro.customer_id, ro.id
),
customer_rollup AS (
    SELECT
        c.region,
        ot.customer_id,
        COUNT(*) AS order_count,
        SUM(ot.order_total) AS total_revenue
    FROM order_totals AS ot
    JOIN customers AS c ON c.id = ot.customer_id
    GROUP BY c.region, ot.customer_id
)
SELECT
    cr.region,
    COUNT(*) AS active_customers,
    SUM(cr.order_count) AS total_orders,
    SUM(cr.total_revenue) AS total_revenue
FROM customer_rollup AS cr
GROUP BY cr.region
ORDER BY total_revenue DESC;
