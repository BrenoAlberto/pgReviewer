from sqlalchemy import select


def load_orders(session, order_model):
    session.query(order_model).filter(order_model.status == "open").all()
    session.query(order_model).filter_by(status="open").limit(10).all()


def load_order_ids(session, order_model):
    return session.execute(
        select(order_model.id).where(order_model.status == "open")
    ).all()
