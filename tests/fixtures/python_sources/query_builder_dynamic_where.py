def build_order_query(status):
    base = "SELECT * FROM orders WHERE 1=1"
    if status:
        base += " AND status = %s"
    return base
