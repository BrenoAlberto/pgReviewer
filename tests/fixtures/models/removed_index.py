from sqlalchemy import Column, Index, Integer, String
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class RemovedIndexBefore(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True)
    status = Column(String(50), nullable=False)

    __table_args__ = (Index("ix_orders_status", "status"),)


class RemovedIndexAfter(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True)
    status = Column(String(50), nullable=False)
