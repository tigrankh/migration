from typing import List

from pydantic import BaseModel, Field, validator

from migration.migration_utility.data_types import FieldQuery
from migration_utility.enums import MigrationStatus


class RelatedDocument(BaseModel):
    type: str = Field(
        ..., description="type of a parent document"
    )
    relation_field: str = Field(
        ..., description="Name of the relation field on teh current document"
    )


class DocumentConfiguration(BaseModel):
    """Doc."""

    type: str = Field(
        ...,
        description="type of the document. will be used to match this config in certain scenarios",
    )
    collection_name: str = Field(
        ..., description="name of the database collection to read/write documents"
    )
    related_document: RelatedDocument = Field(
        None,
        description="info of the related document",
    )
    queries: List[FieldQuery] = Field(
        None, description="query to find documents to migrate"
    )
    query_index_name: str = Field(
        None, description="name of the index that the query will search in"
    )
    all_fetched: bool = Field(
        None, description="indicates whether the current collection was fetched or not"
    )
    all_inserted: bool = Field(
        None, description="indicates whether the current collection was inserted or not"
    )
    num_migrated: int = Field(
        0, description="number of migrated documents"
    )

    def export_cancelled_doc_info(
        self, cancelled_documents: List[dict], exc_info: dict
    ) -> List[dict]:
        """Exports information about the cancelled documents and the reasons of
        cancellation.

        Args:
            cancelled_documents: list of documents that were cancelled
            exc_info: info about the cancellation exception

        Returns: dictionary with cancellation data
        """
        exported_info = []

        for doc in cancelled_documents:
            exported_info.append(
                {
                    "id": doc.get("id"),
                    "type": doc.get("type"),
                    "migration_status": MigrationStatus.CANCELLED,
                    "exception_info": exc_info,
                }
            )

        return exported_info
