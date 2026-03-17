from sqlalchemy.orm import joinedload


def relationship_n_plus_one(session, User):
    users = session.query(User).all()
    for user in users:
        print(user.orders)


def column_access_not_relationship(session, User):
    users = session.query(User).all()
    for user in users:
        print(user.name)


def eager_load_not_n_plus_one(session, User):
    users = session.query(User).options(joinedload(User.orders)).all()
    for user in users:
        print(user.orders)
