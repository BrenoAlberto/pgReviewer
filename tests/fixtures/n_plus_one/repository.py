class UserRepository:
    def __init__(self, session):
        self.session = session

    def get_by_id(self, user_id):
        return self.session.execute(
            "SELECT * FROM users WHERE id = :id", {"id": user_id}
        )
