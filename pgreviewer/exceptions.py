class PGRReviewerError(Exception):
    """Base exception for pgreviewer."""

    pass


class DBConnectionError(PGRReviewerError):
    """Raised when there is an error connecting to the database."""

    pass


class BudgetExceededError(PGRReviewerError):
    """Raised when an LLM call would exceed the monthly budget for a category."""

    pass
