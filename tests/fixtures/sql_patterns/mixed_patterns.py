# EXPECTED_SQL: SELECT id FROM users WHERE id = %s
# EXPECTED_SQL: SELECT * FROM orders WHERE status = 'active'

def run_queries(cursor, user_id):
    cursor.execute("SELECT id FROM users WHERE id = %s", (user_id,))

    base = "SELECT * FROM orders"
    where_clause = " WHERE status = 'active'"
    query = base + where_clause
    cursor.execute(query)

    return cursor.fetchall()
