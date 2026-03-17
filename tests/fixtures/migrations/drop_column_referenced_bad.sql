ALTER TABLE orders DROP COLUMN legacy_id;
SELECT legacy_id FROM orders WHERE legacy_id IS NOT NULL;
