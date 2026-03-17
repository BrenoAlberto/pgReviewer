def enrich_users(user_repo, users):
    for user in users:
        user_repo.get_by_id(user.id)


def summarize_users(users):
    for user in users:
        user.full_name()
