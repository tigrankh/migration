from typing import List, Union

from migration_utility.data_types import ReadQueryResult, WriteQueryResult
from migration_utility.db_clients.generic import GenericClient
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from migration.migration_utility import logging
from migration_utility.db_clients.mongodb.data_types import FieldQuery
from migration_utility.exceptions import InsertionWasCancelledError


class MongoDbClient(GenericClient):
    """DynamoDB client class that ensure connectivity and operations with DynamoDB."""

    def __init__(self, batch_size: int, connection_string: str, database_name: str):
        self._client_connector = None
        self._last_document = None
        self._batch_size = batch_size
        self._connection_string = connection_string
        self._database_name = database_name

    @property
    def client_connector(self) -> MongoClient:
        """Creates a MongoDB client connection.

        Returns: instance that represents client connection
        """

        if not self._client_connector:
            self._client_connector = MongoClient(host=self._connection_string)

        return self._client_connector[self._database_name]

    @property
    def resource_connector(self):
        """MongoDB doesn't need this connector."""

        raise NotImplementedError("MongoDB instance should not call this method")

    def set_last_document(self, last_document: dict):
        """setting."""

    def last_fetched_key(self):
        """last"""

    def batch_write(
        self, collection_name: str, documents: List[dict]
    ) -> WriteQueryResult:
        """Performs batch write operation by putting the passed in documents into the
        database.

        Args:
            collection_name: name of the collection where write operation is performed
            documents: list of documents to be written

        Returns: list of IDs of processed documents
        """

        documents = self._inject_id_field(documents=documents)

        try:
            logging.info(f"Starting insertion...")

            response = self.client_connector[collection_name].insert_many(documents)

            logging.info(f"Insertion successfully finished...")
            logging.info(
                f"Inserted {len(response.inserted_ids)} documents into collection {collection_name}"
            )
        except BulkWriteError as exc:
            num_inserted = exc.details.get("nInserted")

            logging.exception(
                f"Insertion failed after inserting {num_inserted} document(s)"
            )
            logging.info(
                f"Canceling insertion of the remaining batch into DESTINATION. "
                f"Canceled document IDs will be saved in the internal database"
            )
            raise InsertionWasCancelledError(
                cancelled_documents=documents[num_inserted:],
                inserted_documents=documents[:num_inserted],
                exception_details=exc.details,
            ) from exc

        return WriteQueryResult(inserted_document_ids=response.inserted_ids)

    def batch_update(
        self, collection_name: str, updates: List[dict]
    ) -> Union[WriteQueryResult, None]:
        """Implements batch update."""

    def update(self, collection_name: str, update_data: dict):
        """Updates a single document in the database.

        Args:
            collection_name: name of the collection
            update_data: document data

        Returns: None
        """
        key_data = {"_id": update_data.get("_id")}
        update_data.pop("_id")

        self.client_connector[collection_name].update_one(
            key_data, {"$set": update_data}, upsert=True
        )

    def find(
        self, collection_name: str, queries: List[FieldQuery], query_index_name: str
    ) -> ReadQueryResult:
        """find."""

    def find_document(self, collection_name: str, doc_id: str) -> Union[dict, None]:
        """Finds one document that corresponds to the requested id.

        Args:
            collection_name: name of the collection
            doc_id: id of the document

        Returns: matched document or None
        """

        return self.client_connector[collection_name].find_one({"_id": doc_id})

    def _inject_id_field(self, documents: List[dict]) -> List[dict]:
        """The original document already contains field named 'id'. MongoDB also created
        _id, which should be the same as 'id'.

        Args:
            documents: list of documents into which _id is injected

        Returns: List of updated documents
        """

        for doc in documents:
            doc["_id"] = doc["id"]

        return documents
