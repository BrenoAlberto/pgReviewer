ALTER TABLE orders ADD COLUMN user_id INTEGER REFERENCES users(id);
CREATE INDEX CONCURRENTLY idx_orders_user_id ON orders(user_id);
