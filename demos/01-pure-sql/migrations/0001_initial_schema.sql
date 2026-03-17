-- Demo: pure-SQL migrations — initial schema
-- Tables: users, products, orders, order_items
--
-- NOTE: FK constraints are added intentionally WITHOUT supporting indexes.
-- pgreviewer will flag each one as CRITICAL (add_foreign_key_without_index).
-- The fix is applied in 0002_add_missing_indexes.sql.

CREATE TABLE IF NOT EXISTS users (
    id          SERIAL PRIMARY KEY,
    email       TEXT NOT NULL UNIQUE,
    username    TEXT,
    last_login  TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    id             SERIAL PRIMARY KEY,
    name           TEXT NOT NULL,
    category       TEXT,
    price          DECIMAL(10, 2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS orders (
    id           SERIAL PRIMARY KEY,
    user_id      INTEGER NOT NULL,
    status       TEXT NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE NOT NULL,
    total_amount DECIMAL(12, 2) DEFAULT 0
);

CREATE TABLE IF NOT EXISTS order_items (
    id          SERIAL PRIMARY KEY,
    order_id    INTEGER NOT NULL,
    product_id  INTEGER NOT NULL,
    quantity    INTEGER NOT NULL,
    unit_price  DECIMAL(10, 2) NOT NULL
);

-- Foreign key constraints — no indexes on FK columns (intentional for demo).
ALTER TABLE orders
    ADD CONSTRAINT orders_user_id_fk
    FOREIGN KEY (user_id) REFERENCES users (id);

ALTER TABLE order_items
    ADD CONSTRAINT order_items_order_id_fk
    FOREIGN KEY (order_id) REFERENCES orders (id);

ALTER TABLE order_items
    ADD CONSTRAINT order_items_product_id_fk
    FOREIGN KEY (product_id) REFERENCES products (id);
