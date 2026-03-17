from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class MissingTablenameModel(Base):
    id = Column(Integer, primary_key=True)
    name = Column(String(50))


class BadModel(Base):
    __tablename__ = "bad_model"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    status = Column(String(50))
