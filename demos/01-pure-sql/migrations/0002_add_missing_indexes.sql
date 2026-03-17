-- Demo: pure-SQL migrations — corrective migration
--
-- Adds the missing indexes on FK columns flagged by pgreviewer in 0001.
-- After applying this migration, pgr diff reports 0 findings.

CREATE INDEX CONCURRENTLY idx_orders_user_id
    ON orders (user_id);

CREATE INDEX CONCURRENTLY idx_order_items_order_id
    ON order_items (order_id);

CREATE INDEX CONCURRENTLY idx_order_items_product_id
    ON order_items (product_id);
