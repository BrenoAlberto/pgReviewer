"""SQLAlchemy model fixture used by test_sqlalchemy_analyzer.py.

Contains three model classes with a variety of column types, foreign keys,
relationships, and both column-level and explicit Index definitions.
"""

from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String(100), nullable=False, unique=True)
    email = Column(String(200), nullable=False, index=True)

    orders = relationship("Order", back_populates="user")


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    status = Column(String(50), nullable=False)
    created_at = Column(DateTime, nullable=False, index=True)

    user = relationship("User", back_populates="orders", foreign_keys=[user_id])
    items = relationship("Item", back_populates="order")

    __table_args__ = (Index("ix_orders_user_status", "user_id", "status"),)


class Item(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False, index=True)
    name = Column(String(200), nullable=False)
    price = Column(Integer, nullable=False)

    order = relationship("Order", back_populates="items")
