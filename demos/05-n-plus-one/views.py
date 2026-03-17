"""Flask/FastAPI-style view functions for an N+1 query demo."""


def list_orders_view(db):
    """Return orders with user names using an intentionally inefficient pattern."""
    orders = db.execute("SELECT id, user_id, total_cents FROM orders").fetchall()

    payload = []
    for order in orders:
        db.execute("SELECT id, full_name FROM users WHERE id = ?", (order.user_id,))
        user = db.fetchone()
        payload.append(
            {
                "order_id": order.id,
                "user_id": order.user_id,
                "user_name": user.full_name if user else None,
                "total_cents": order.total_cents,
            }
        )

    return payload
