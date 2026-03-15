class PGRReviewerError(Exception):
    """Base exception for pgreviewer."""

    pass


class DBConnectionError(PGRReviewerError):
    """Raised when there is an error connecting to the database."""

    pass


class BudgetExceededError(PGRReviewerError):
    """Raised when an LLM call would exceed the monthly budget for a category."""

    pass


class InvalidQueryError(PGRReviewerError):
    """
    Raised when a query is syntactically invalid or referencing
    non-existent objects.
    """

    def __init__(self, sql: str, message: str):
        self.sql = sql
        self.message = message
        super().__init__(f"Invalid query: {message}\nQuery: {sql}")
