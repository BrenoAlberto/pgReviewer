# EXPECTED_SQL: SELECT id, name FROM users WHERE id = %s


def get_user(cursor, user_id):
    cursor.execute("SELECT id, name FROM users WHERE id = %s", (user_id,))
    return cursor.fetchone()
