"""SQLAlchemy ORM query patterns — fixture for test_sqlalchemy_query_extractor."""

from sqlalchemy import select
from sqlalchemy.orm import Session


class User:
    pass


class Order:
    pass


class Item:
    pass


# Pattern 1: session.query(Model).filter(Model.column == value)
def get_order_by_user(session: Session, user_id: int):
    return session.query(Order).filter(Order.user_id == user_id).all()


# Pattern 2: session.query(Model).order_by(Model.column)
def get_recent_orders(session: Session):
    return session.query(Order).order_by(Order.created_at).all()


# Pattern 3: select(Model).where(Model.column == value)
def get_user_by_id(session: Session, user_id: int):
    return session.execute(select(User).where(User.id == user_id)).scalars()


# Pattern 4: session.query(Model).join(OtherModel)
def get_orders_with_items(session: Session):
    return session.query(Order).join(Item).all()


# Pattern 5: session.query(Model).filter_by(column=value)
def get_user_by_email(session: Session, email: str):
    return session.query(User).filter_by(email=email).first()


# Pattern 6: multi-step chain with filter + order_by
def get_active_orders_sorted(session: Session, user_id: int):
    return (
        session.query(Order)
        .filter(Order.user_id == user_id)
        .order_by(Order.created_at)
        .all()
    )
