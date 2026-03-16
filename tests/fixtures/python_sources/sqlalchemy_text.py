from sqlalchemy import text


def get_orders(session, user_id):
    result = session.execute(
        text("SELECT id, total FROM orders WHERE user_id = :user_id"),
        {"user_id": user_id},
    )
    return result.fetchall()
