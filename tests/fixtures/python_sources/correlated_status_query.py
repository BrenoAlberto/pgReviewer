def fetch_active_orders(cursor):
    cursor.execute("SELECT * FROM orders WHERE status = 'active';")
