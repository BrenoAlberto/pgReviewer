from models import User
from sqlalchemy.orm import selectinload


def eager_load_not_n_plus_one(session):
    users = session.query(User).options(selectinload(User.orders)).all()
    for user in users:
        print(user.orders)
