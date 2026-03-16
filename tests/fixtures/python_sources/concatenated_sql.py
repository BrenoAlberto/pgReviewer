def get_users(cursor, columns):
    sql = "SELECT " + columns + " FROM users"
    cursor.execute(sql)
    return cursor.fetchall()
