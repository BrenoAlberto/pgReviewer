def ignored_query_in_loop(cursor, users):
    for user in users:  # pgreviewer:ignore[query_in_loop]
        cursor.execute("SELECT * FROM users WHERE id = %s", (user.id,))


def small_range_loop(cursor):
    for _i in range(3):
        cursor.execute("SELECT 1", ())
