from models import User


def relationship_n_plus_one(session):
    users = session.query(User).all()
    for user in users:
        print(user.orders)


def column_access_not_relationship(session):
    users = session.query(User).all()
    for user in users:
        print(user.name)
