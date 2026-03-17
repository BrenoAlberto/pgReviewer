DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS event_logs;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    id BIGINT PRIMARY KEY,
    email TEXT NOT NULL,
    region TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE orders (
    id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers(id),
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE order_items (
    id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(id),
    product_id BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL
);

CREATE TABLE event_logs (
    id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers(id),
    event_type TEXT NOT NULL,
    duration_ms INTEGER NOT NULL,
    occurred_at TIMESTAMP NOT NULL
);

INSERT INTO customers (id, email, region, created_at)
SELECT
    g,
    'customer' || g || '@example.com',
    CASE (g % 4)
        WHEN 0 THEN 'north'
        WHEN 1 THEN 'south'
        WHEN 2 THEN 'east'
        ELSE 'west'
    END,
    NOW() - (g % 365) * INTERVAL '1 day'
FROM generate_series(1, 50000) AS g;

INSERT INTO orders (id, customer_id, status, created_at)
SELECT
    g,
    1 + (g % 50000),
    CASE (g % 5)
        WHEN 0 THEN 'paid'
        WHEN 1 THEN 'paid'
        WHEN 2 THEN 'shipped'
        WHEN 3 THEN 'pending'
        ELSE 'cancelled'
    END,
    NOW() - (g % 180) * INTERVAL '1 day'
FROM generate_series(1, 100000) AS g;

INSERT INTO order_items (id, order_id, product_id, quantity, unit_price)
SELECT
    g,
    1 + (g % 100000),
    1 + (g % 2000),
    1 + (g % 4),
    ROUND((5 + (g % 500) / 10.0)::numeric, 2)
FROM generate_series(1, 200000) AS g;

INSERT INTO event_logs (id, customer_id, event_type, duration_ms, occurred_at)
SELECT
    g,
    1 + (g % 50000),
    CASE (g % 4)
        WHEN 0 THEN 'page_view'
        WHEN 1 THEN 'search'
        WHEN 2 THEN 'add_to_cart'
        ELSE 'checkout'
    END,
    50 + (g % 2500),
    NOW() - (g % 90) * INTERVAL '1 day'
FROM generate_series(1, 100000) AS g;

ANALYZE customers;
ANALYZE orders;
ANALYZE order_items;
ANALYZE event_logs;
