# EXPECTED_SQL: SELECT id, name FROM users WHERE status = 'active'


def list_active_users(cursor):
    base = "SELECT id, name FROM users"
    where_clause = " WHERE status = 'active'"
    query = base + where_clause
    cursor.execute(query)
    return cursor.fetchall()
