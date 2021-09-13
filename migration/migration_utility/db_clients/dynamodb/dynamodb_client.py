import time
from math import ceil

from migration.migration_utility import logging
from functools import reduce
from typing import List, Union
from pydantic import validate_arguments
from migration.migration_utility.db_clients.generic import GenericClient
from boto3 import client, resource
from boto3.resources.factory import ServiceResource
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
from boto3.dynamodb.conditions import Attr

from migration.migration_utility.db_clients.dynamodb.data_types import FieldQuery
from migration_utility.data_types import ReadQueryResult, WriteQueryResult
from migration_utility.exceptions import RetryableFetchingError


class DynamoDbClient(GenericClient):
    """DynamoDB client class that ensure connectivity and operations with DynamoDB."""

    def __init__(self, batch_size: int):
        self._batch_size = batch_size

        self._client_connector = None
        self._resource_connector = None
        self._config = Config(retries={"total_max_attempts": 3, "mode": "legacy"})
        self._last_document = None
        self._last_evaluated_key = None

    @property
    def client_connector(self) -> BaseClient:
        """Creates a DynamoDB client connection.

        Returns: instance that represents client connection
        """

        if not self._client_connector:
            self._client_connector = client("dynamodb", config=self._config)

        return self._client_connector

    @property
    def resource_connector(self) -> ServiceResource:
        """Creates a DynamoDB resource connection.

        Returns: instance that represents resource connection
        """

        if not self._resource_connector:
            self._resource_connector = resource("dynamodb", config=self._config)

        return self._resource_connector

    @property
    def last_fetched_key(self) -> dict:
        """Returns last evaluated key"""

        return self._last_evaluated_key

    def set_last_document(self, last_document: Union[dict, None]):
        """Sets data of the last document for pagination purposes.

        Args:
            last_document: last document data

        Returns: None
        """

        self._last_document = last_document

        if last_document:
            self._last_evaluated_key = {
                "model_type": last_document["model_type"],
                "created_at": last_document["created_at"],
                "id": last_document["id"],
            }
        else:
            self._last_evaluated_key = None

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

        inserted_document_ids = []
        dynamodb_documents = self._prepare_batch_write_request(documents)

        try:
            response = self.client_connector.batch_write_item(
                RequestItems={collection_name: dynamodb_documents}
            )

            unprocessed_ids = [
                TypeDeserializer().deserialize(value=item["PutRequest"]["Item"]["id"])
                for item in response.get(collection_name, [])
            ]

            inserted_document_ids = [
                doc.get("id")
                for doc in documents
                if doc.get("id") not in unprocessed_ids
            ]

        except ClientError as exc:
            logging.exception(
                f"The following exception occurred "
                f"when batch_write_item() was called on {collection_name} --> {exc}"
            )

        return WriteQueryResult(
            inserted_document_ids=inserted_document_ids,
            processed_count=len(inserted_document_ids)
        )

    def batch_update(self, collection_name: str, updates: List[dict]):
        """A method that performs a batch update operation.

        Args:
            collection_name: name of the collection into which the documents will be written
            updates: list of dicts that contains key data and update field of the documents

        Returns: None
        """

        # Batch updates in DynamoDB can be done by using transact_write_items()
        # transact_write_items() supports max 25 items in the batch, so many iterations may happen

        num_partitions = ceil(len(updates) / 25)

        for i in range(num_partitions):
            logging.info(f"Updating partition #{i+1} in collection={collection_name}")
            self._partitioned_batch_update(
                collection_name=collection_name, updates=updates[25 * i : 25 * i + 25]
            )

    def update(self, collection_name: str, update_data: dict):
        """A method that performs a update operation.

        Args:
            collection_name: name of the collection into which the documents will be written
            update_data: dict that contains key data and update field of the documents

        Returns: None
        """

    @validate_arguments
    def find(
        self,
        collection_name: str,
        queries: List[FieldQuery],
        query_index_name: str = None,
        find_all: bool = False
    ) -> ReadQueryResult:
        """Queries documents in the database, based on the collection name and queries.

        Args:
            collection_name: Name of the collection where the query is performed
            queries: list of FieldQuery objects that describe the fields and values of the queries
            query_index_name: name of the collection index the query will happen in

        Returns: List of matched documents
        """

        merged_key_condition = self._merge_queries(
            queries, index_query=bool(query_index_name)
        )

        matched_docs = self._fetch_document_batch(
            collection_name=collection_name,
            key_or_filter_expression=merged_key_condition,
            query_index_name=query_index_name,
            find_all=find_all
        )

        return ReadQueryResult(
            has_more=self._last_evaluated_key is not None,
            documents=matched_docs,
            last_evaluated_key=self._last_evaluated_key,
        )

    def find_document(self, collection_name: str, doc_id: str) -> Union[dict, None]:
        """find doc."""

    def _fetch_document_batch(
        self, collection_name: str, key_or_filter_expression, query_index_name: str, find_all: bool = False
    ) -> List[dict]:
        """Runs iterations of queries in the database, until the configured number of
        documents (batch_size) is not read.

        Args:
            collection_name: Name of the collection where the query is performed
            key_or_filter_expression: DynamoDb-formatted Key or Filter expression
            query_index_name: name of the collection index the query will happen in

        Returns: List of matched documents
        """

        fetched_documents = []

        fetched_documents.extend(
            self._fetch_documents(
                collection_name=collection_name,
                key_or_filter_expression=key_or_filter_expression,
                query_index_name=query_index_name,
                document_count=self._batch_size,
                last_document_id_data=self._last_evaluated_key,
                find_all=find_all
            )
        )

        while len(fetched_documents) < self._batch_size and self._last_evaluated_key:
            fetched_documents.extend(
                self._fetch_documents(
                    collection_name=collection_name,
                    key_or_filter_expression=key_or_filter_expression,
                    query_index_name=query_index_name,
                    last_document_id_data=self._last_evaluated_key,
                    document_count=self._batch_size - len(fetched_documents),
                    find_all=find_all
                )
            )

            logging.info(f"Accumulated number of documents is {len(fetched_documents)}")

        return fetched_documents

    def _fetch_documents(
        self,
        collection_name: str,
        key_or_filter_expression,
        query_index_name: str,
        last_document_id_data: dict = None,
        document_count: int = None,
        check_migration_status: bool = True,
        find_all: bool = False
    ) -> List[dict]:
        """Runs a single query in the database, based on the collection name and queries.

        Args:
            collection_name: Name of the collection where the query is performed
            key_or_filter_expression: DynamoDb-formatted Key or Filter expression
            query_index_name: name of the collection index the query will happen in
            last_document_id_data: ID data of the last document read during previous read operation
            document_count: number of documents to read in this iteration
            check_migration_status: if True retrieves only documents that are marked as migrated

        Returns: List of matched documents
        """

        common_query_settings = {"ScanIndexForward": True}

        if last_document_id_data:
            common_query_settings["ExclusiveStartKey"] = last_document_id_data
        if document_count:
            common_query_settings["Limit"] = document_count
        if check_migration_status and not find_all:
            common_query_settings["FilterExpression"] = Attr("is_migrated").ne(True)

        try:
            if query_index_name:
                query_response = self.resource_connector.Table(collection_name).query(
                    IndexName=query_index_name,
                    KeyConditionExpression=key_or_filter_expression,
                    **common_query_settings,
                )
            else:
                filter_expression = key_or_filter_expression

                fe = common_query_settings.get("FilterExpression")

                if fe:
                    filter_expression &= fe
                    common_query_settings.pop("FilterExpression")

                query_response = self.resource_connector.Table(collection_name).scan(
                    FilterExpression=filter_expression, **common_query_settings
                )
        except ClientError as exc:
            logging.exception(
                f"Failed to fetch new batch. LastEvaluatedKey={self._last_evaluated_key}"
            )
            raise RetryableFetchingError from exc

        self._last_evaluated_key = query_response.get("LastEvaluatedKey")
        logging.info(f"TEST::lek={self._last_evaluated_key}")

        logging.info(
            f"Fetched {len(query_response['Items'])} from collection {collection_name}"
        )

        return query_response["Items"]

    def _prepare_batch_write_request(self, documents: List[dict]) -> List[dict]:
        """Converts the list of documents into a DynamoDB-friendly typed format.

        Args:
            documents: list of documents to be written

        Returns: DynamoDB-friendly list of dictionaries
        """

        converted_documents = []

        for doc in documents:
            dynamodb_document = {"PutRequest": {"Item": {}}}

            for k, v in doc.items():
                dynamodb_document["PutRequest"]["Item"][k] = TypeSerializer().serialize(
                    value=v
                )

            converted_documents.append(dynamodb_document)

        return converted_documents

    def _merge_queries(self, queries: List[FieldQuery], index_query: bool):
        """Merges all queries from the query list.

        Args:
            queries: List of FieldQuery instances
            index_query: indicates whether the performed query is an index query or scan

        Returns: Dynamodb query object
        """
        merged_query = None

        for q in queries:
            if not merged_query:
                merged_query = q.export_query(index_query=index_query)
            else:
                merged_query &= q.export_query(index_query=index_query)

        return merged_query

    def _extract_key_data(self, data_dict: dict) -> dict:
        """Extract key data and serializes to make it DynamoDB-specific.

        Args:
            data_dict: data dictionary that contains id field

        Returns: serialized key data
        """

        val = data_dict.get("id")
        ser_val = TypeSerializer().serialize(value=val)

        return {"id": ser_val}

    def _generate_data_for_transact_update(self, data_dict: dict) -> tuple:
        """
        Generates UpdateExpression, ExpressionAttributes from the given data_dict
        Args:
            data_dict: dict with document updates

        Returns: a tuple of all updatexpression fields

        """

        update_expression = "set "
        expression_attr_names = {}
        expression_attr_values = {}

        i = 0

        for k, v in data_dict.items():
            update_expression += f"#{k}=:v{i},"
            expression_attr_names[f"#{k}"] = k
            expression_attr_values[f":v{i}"] = TypeSerializer().serialize(value=v)

            i += 1

        update_expression = update_expression.rstrip(",")
        return update_expression, expression_attr_names, expression_attr_values

    def _partitioned_batch_update(self, collection_name: str, updates: List[dict]):
        """A method that performs a batch update operation on a limited 25 items.

        Args:
            collection_name: name of the collection into which the documents will be written
            updates: list of dicts that contains key data and update field of the documents

        Returns: None
        """

        transact_items = []

        for update_data in updates:
            transact_item = {"Update": {"Key": {}, "TableName": collection_name}}
            transact_item["Update"]["Key"] = self._extract_key_data(update_data)

            update_data.pop("id")

            (
                update_expr,
                expr_attr_names,
                expr_attr_values,
            ) = self._generate_data_for_transact_update(data_dict=update_data)

            transact_item["Update"]["UpdateExpression"] = update_expr
            transact_item["Update"]["ExpressionAttributeNames"] = expr_attr_names
            transact_item["Update"]["ExpressionAttributeValues"] = expr_attr_values

            transact_items.append(transact_item)

        try:
            self.client_connector.transact_write_items(TransactItems=transact_items)
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ValidationError":
                i = 0

                while i < 3:
                    i += 1
                    logging.info(f"Batch update attempt #{i} started...")

                    time.sleep(i * 60)

                    try:
                        self.client_connector.transact_write_items(TransactItems=transact_items)
                        return
                    except ClientError:
                        logging.info(f"Batch update attempt #{i} failed...")
                        continue

                logging.info(f"Failed to finish the following transaction --> {transact_items}")
