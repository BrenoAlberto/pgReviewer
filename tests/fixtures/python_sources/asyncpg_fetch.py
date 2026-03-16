async def get_user(conn, user_id):
    row = await conn.fetchrow("SELECT id, name FROM users WHERE id = $1", user_id)
    return row


async def list_products(conn):
    rows = await conn.fetch("SELECT id, name, price FROM products")
    return rows
