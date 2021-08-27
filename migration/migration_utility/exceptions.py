from typing import List


class UnknownDatabaseError(Exception):
    """Raised when the database attempted for creation is not known by the utility."""


class MissingRequiredConfigurationParamError(Exception):
    """Raised when configuration models are missing conditionally required fields."""


class InsertionWasCancelledError(Exception):
    """Raised when document insertion was cancelled."""

    def __init__(
        self,
        cancelled_documents: List[dict],
        inserted_documents: List[dict],
        exception_details: dict,
    ):
        self.cancelled_documents = cancelled_documents
        self.inserted_documents = inserted_documents
        self.exception_details = exception_details


class RetryableFetchingError(Exception):
    """Raised when document fetching fails, but can be retried."""


class FetchingTerminatedError(Exception):
    """Raised when fetching still fails and we are not going to retry."""
