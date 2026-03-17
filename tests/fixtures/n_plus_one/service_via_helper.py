from helper import fetch_user


def process_user(user_repo, user_id):
    return fetch_user(user_repo, user_id)


def hydrate_users(user_repo, users):
    for user in users:
        process_user(user_repo, user.id)


def no_query(users):
    for user in users:
        user.display_name()
