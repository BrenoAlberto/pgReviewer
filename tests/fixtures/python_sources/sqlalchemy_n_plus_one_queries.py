from sqlalchemy.orm import joinedload

from tests.fixtures.python_sources.sqlalchemy_n_plus_one_models import User


def relationship_n_plus_one(session):
    users = session.query(User).all()
    for user in users:
        print(user.orders)


def column_access_not_relationship(session):
    users = session.query(User).all()
    for user in users:
        print(user.name)


def eager_load_not_n_plus_one(session):
    users = session.query(User).options(joinedload(User.orders)).all()
    for user in users:
        print(user.orders)
