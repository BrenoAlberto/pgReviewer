"""Repository layer demo containing an N+1 query pattern."""


class OrderRepository:
    def __init__(self, db):
        self.db = db

    def list_orders_with_user_data(self):
        orders = self.db.execute(
            "SELECT id, user_id, total_cents, status FROM orders ORDER BY id DESC"
        ).fetchall()

        enriched = []
        for order in orders:
            self.db.execute(
                "SELECT id, email, full_name FROM users WHERE id = ?", (order.user_id,)
            )
            user = self.db.fetchone()
            enriched.append(
                {
                    "order_id": order.id,
                    "status": order.status,
                    "user": user,
                }
            )

        return enriched
