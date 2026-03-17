def load_users(cursor, user_ids):
    for user_id in user_ids:
        cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
