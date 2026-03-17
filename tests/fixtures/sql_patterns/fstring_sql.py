# EXPECTED_SQL: SELECT * FROM users


def read_table(cursor, table):
    query = f"SELECT * FROM {table}"
    cursor.execute(query)
    return cursor.fetchall()
