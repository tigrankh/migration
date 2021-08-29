from abc import ABC, abstractmethod
from typing import List, Union

from migration_utility.data_types import FieldQuery, ReadQueryResult, WriteQueryResult


class GenericClient(ABC):
    """Abstract class that defines interface for database clients."""

    @property
    @abstractmethod
    def client_connector(self):
        """Overwritable method that should return DB client connection.

        Raises: NotImplementedError
        """

        raise NotImplementedError("Method should be overwritten")

    @property
    @abstractmethod
    def resource_connector(self):
        """Overwritable method that should return DB resource connection available for
        some databases.

        Raises: NotImplementedError
        """

        raise NotImplementedError("Method should be overwritten")

    @abstractmethod
    def batch_write(
        self, collection_name: str, documents: List[dict]
    ) -> WriteQueryResult:
        """Abstract method that is intended to perform batch write operation on the list
        of documents provided.

        Args:
            collection_name: name of the collection into which the documents will be written
            documents: list of documents that are going to be written into the collection

        Returns: WriteQueryResult instance
        """

        raise NotImplementedError("Method should be overwritten")

    @abstractmethod
    def batch_update(
        self, collection_name: str, updates: List[dict]
    ) -> Union[WriteQueryResult, None]:
        """Abstract method that is intended to perform a batch update operation.

        Args:
            collection_name: name of the collection into which the documents will be written
            updates: list of dicts that contain key data and update fields of the document

        Returns: WriteQueryResult instance or None depending on the client
        """

        raise NotImplementedError("Method should be overwritten")

    @abstractmethod
    def set_last_document(self, last_document: dict):
        """Sets the data of the latest document."""

        raise NotImplementedError("Method should be overwritten")

    @abstractmethod
    def update(self, collection_name: str, update_data: dict):
        """Abstract method that is intended to perform a update operation.

        Args:
            collection_name: name of the collection into which the documents will be written
            update_data: dict that contains key data and update field of the document

        Returns: WriteQueryResult instance or None depending on the client
        """

        raise NotImplementedError("Method should be overwritten")

    @abstractmethod
    def find(
        self, collection_name: str, queries: List[FieldQuery], query_index_name: str, find_all: bool = False
    ) -> ReadQueryResult:
        """
        Abstract method which will return documents from collection based on the query param
        Args:
            collection_name: name of the collection to search for documents in
            queries: queries to be performed
            query_index_name: name of the collection index the query will happen in

        Returns: ReadQueryResult instance

        """

        raise NotImplementedError("Method should be overwritten")

    @abstractmethod
    def find_document(self, collection_name: str, doc_id: str) -> Union[dict, None]:
        """Finds one document that corresponds to the requested id.

        Args:
            collection_name: name of the collection
            doc_id: id of the document

        Returns: matched document or None
        """

        raise NotImplementedError("Method should be overwritten")
