class PGRReviewerError(Exception):
    """Base exception for pgreviewer."""

    pass


class DBConnectionError(PGRReviewerError):
    """Raised when there is an error connecting to the database."""

    pass
