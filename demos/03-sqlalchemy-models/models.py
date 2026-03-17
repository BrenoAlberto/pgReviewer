from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)

    events = relationship("Event", back_populates="account")


class Event(Base):
    __tablename__ = "events"

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    happened_at = Column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    event_key = Column(String(128), nullable=False)

    account = relationship("Account", back_populates="events")

    __table_args__ = (
        UniqueConstraint("account_id", "event_key", name="uq_events_account_event_key"),
    )
