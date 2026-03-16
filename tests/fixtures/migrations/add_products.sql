-- Add products table
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(10, 2)
);

CREATE INDEX idx_products_name ON products (name);
