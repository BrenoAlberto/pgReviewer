class PGRReviewerError(Exception):
    """Base exception for pgreviewer."""

    pass


class DBConnectionError(PGRReviewerError):
    """Raised when there is an error connecting to the database."""

    pass


class BudgetExceededError(PGRReviewerError):
    """Raised when an LLM call would exceed the monthly budget for a category."""

    pass


class LLMUnavailableError(PGRReviewerError):
    """Raised when the configured LLM provider is unavailable."""

    pass


class StructuredOutputError(PGRReviewerError):
    """Raised when structured output cannot be parsed after retries."""

    pass


class ExtensionMissingError(PGRReviewerError):
    """Raised when a required PostgreSQL extension is not installed."""

    def __init__(self, extension: str):
        self.extension = extension
        super().__init__(
            f"Required PostgreSQL extension '{extension}' is not installed. "
            f"Install it with: CREATE EXTENSION {extension};"
        )


class InvalidQueryError(PGRReviewerError):
    """
    Raised when a query is syntactically invalid or referencing
    non-existent objects.
    """

    def __init__(self, sql: str, message: str):
        self.sql = sql
        self.message = message
        super().__init__(f"Invalid query: {message}\nQuery: {sql}")
