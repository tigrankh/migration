from pydantic import BaseModel, Field
from typing import Any, List
from .enums import FieldQueryOperation


class FieldQuery(BaseModel):
    """Model that holds information and actions for field queries."""

    field_name: str = Field(
        ..., description="name of the field that is being checked in the query"
    )
    operation: FieldQueryOperation = Field(
        ..., description="operation performed to match the field"
    )
    value: Any = Field(..., description="value against which database field is compare")

    def export_query(self, *args, **kwargs) -> Any:
        """
        Exports query data that corresponds to the underlying database type
        Args:
            *args: positional arguments
            **kwargs: keyword arguments

        Returns: Query data for a single field

        """

        raise NotImplementedError("This method should be implemented")


class ReadQueryResult(BaseModel):
    """Model that holds information returned from the read query."""

    documents: List[dict] = Field(
        ..., description="list of documents returned from the query"
    )
    has_more: bool = Field(
        ..., description="indicates whether there are more documents to read"
    )
    last_evaluated_key: dict = Field(
        None, description="key data of the latest evaluated doc. used for pagination"
    )


class WriteQueryResult(BaseModel):
    """Model that holds information returned from the write query."""

    inserted_document_ids: List[str] = Field(
        ..., description="list of document ids written into the database"
    )
    processed_count: int = Field(
        ..., description="number of documents that were acknowledged but not necessarily inserted"
    )
