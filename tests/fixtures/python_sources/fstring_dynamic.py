def get_from_table(cursor, table):
    cursor.execute(f"SELECT * FROM {table}")
    return cursor.fetchall()
