def fetch_legacy(cursor):
    cursor.execute("SELECT legacy_id FROM orders WHERE legacy_id IS NOT NULL;")
