import asyncio
import time

from migration.migration_utility import logging
from typing import List

from migration.migration_utility.configuration.db_configuration import DbConfigurator
from migration.migration_utility.configuration.document_configuration import (
    DocumentConfiguration,
)
from migration.migration_utility.controller.container_manager import ContainerManager
from migration.migration_utility.db_clients.generic import GenericClient
from migration_utility.data_types import ReadQueryResult, WriteQueryResult
from migration_utility.exceptions import (
    InsertionWasCancelledError,
    RetryableFetchingError,
    FetchingTerminatedError,
)


class MigrationController:
    """Holds methods and properties required to migration a collection."""

    def __init__(
        self,
        source_db_config: DbConfigurator,
        destination_db_config: DbConfigurator,
        internal_db_config: DbConfigurator,
        document_configs: List[DocumentConfiguration],
        collections_to_migrate: List[str] = None,
    ):
        """Initializes migration controller object with the given arguments.

        Args:
            source_db_config: DbConfigurator instance for the source database
            destination_db_config: DbConfigurator instance for the destination database
            internal_db_config: DbConfigurator instance for the internal database
            document_configs: list of DocumentConfiguration instances, which describe documents/collections
            collections_to_migrate: a list of collections that need to be migrated. all will be migrated if set to None
        """

        self.source_db_config = source_db_config
        self.destination_db_config = destination_db_config
        self.internal_db_config = internal_db_config
        self.collections_to_migrate = collections_to_migrate

        self._source_db_client = None
        self._destination_db_client = None
        self._internal_db_client = None

        self._document_configuration = None

        if not collections_to_migrate:
            self._document_configs = document_configs
        else:
            self._document_configs = [
                cfg
                for col_name in self.collections_to_migrate
                for cfg in document_configs
                if cfg.collection_name == col_name
            ]

        self.current_doc_cfg = self.next_document_configuration
        self.container_manager = ContainerManager()

        self.connect()

    @property
    def document_configuration(self):
        """
        Returns: A generator that keeps track document configuration sequence

        """

        if not self._document_configuration:
            self._document_configuration = self.document_cfg_sequence

        return self._document_configuration

    @property
    def document_cfg_sequence(self):
        """
        Returns: a generator that points to the current document configuration

        """
        for cfg in self._document_configs:
            yield cfg

    @property
    def next_document_configuration(self):
        """
        Returns: the next document configuration from the generator

        """

        try:
            return next(self.document_configuration)
        except StopIteration:
            logging.info(f"All configured collections have been migrated")
            return None

    @property
    def source_db_client(self) -> GenericClient:
        """Client object of the source database."""

        if not self._source_db_client:
            self._source_db_client = self.source_db_config.create_client()

        return self._source_db_client

    @property
    def destination_db_client(self) -> GenericClient:
        """Client object of the destination database."""

        if not self._destination_db_client:
            self._destination_db_client = self.destination_db_config.create_client()

        return self._destination_db_client

    @property
    def internal_db_client(self) -> GenericClient:
        """Client object of the internally used database."""

        if not self._internal_db_client:
            self._internal_db_client = self.internal_db_config.create_client()

        return self._internal_db_client

    @property
    def last_fetched_key(self) -> dict:
        """Returns the latest evaluated document."""

        if not self.source_db_client.last_fetched_key and not self.current_doc_cfg.all_fetched:
            self.source_db_client.set_last_document(
                self.internal_db_client.find_document(
                    collection_name=self.current_doc_cfg.collection_name,
                    doc_id="LastEvaluatedKey",
                )
                or {}
            )

        return self.source_db_client.last_fetched_key

    def connect(self) -> tuple:
        """Connects to all databases by calling client creation."""

        return self.source_db_client, self.destination_db_client, self.internal_db_client

    def fetch(self, find_all: bool = False) -> ReadQueryResult:
        """Fetches documents from the source database."""

        self.source_db_client.set_last_document(last_document=self.last_fetched_key)

        if self.current_doc_cfg.all_fetched is True:
            self.current_doc_cfg = self.next_document_configuration

            if self.current_doc_cfg is None:
                return ReadQueryResult(documents=[], has_more=False)

        try:
            query_result = self.source_db_client.find(
                collection_name=self.current_doc_cfg.collection_name,
                queries=self.current_doc_cfg.queries,
                query_index_name=self.current_doc_cfg.query_index_name,
                find_all=find_all
            )
        except RetryableFetchingError:
            query_result = self.retry_fetch(find_all=find_all)

        self.container_manager.add_documents(documents=query_result.documents)

        self.current_doc_cfg.all_fetched = query_result.has_more is False

        if query_result.last_evaluated_key:
            self.internal_db_client.update(
                collection_name=self.current_doc_cfg.collection_name,
                update_data={
                    "_id": "LastEvaluatedKey",
                    **query_result.last_evaluated_key,
                },
            )

        return query_result

    def insert(self) -> WriteQueryResult:
        """Inserts the documents from the container into destination DB."""

        self.container_manager.primary_to_transit_bucket()

        try:
            query_res = self.destination_db_client.batch_write(
                collection_name=self.current_doc_cfg.collection_name,
                documents=self.container_manager.transit_bucket,
            )
            self.container_manager.check_move_to_retry_bucket(
                id_list=query_res.inserted_document_ids
            )

            # Will be retried only if there are items in the retry_bucket
            self.retry_insert()

        except InsertionWasCancelledError as exc:
            try:
                self.internal_db_client.batch_write(
                    collection_name=self.current_doc_cfg.collection_name,
                    documents=self.current_doc_cfg.export_cancelled_doc_info(
                        exc.cancelled_documents, exc_info=exc.exception_details
                    ),
                )
                self.container_manager.empty_transit_bucket()
            except InsertionWasCancelledError as e:
                logging.exception(
                    f"The following issue occurred when writing into internal DB --> {e}"
                )

            query_res = WriteQueryResult(
                inserted_document_ids=[doc.get("id") for doc in exc.inserted_documents]
            )

        return query_res

    def insert_update(self) -> WriteQueryResult:
        """Inserts into destination database and updates the source."""

        if self.container_manager.data_exists:

            query_res = self.insert()
            self.source_db_client.batch_update(
                collection_name=self.current_doc_cfg.collection_name,
                updates=self._generate_migration_marks(query_res.inserted_document_ids),
            )

            return query_res

    def reset_migration(self):
        curr_collection_name = self.current_doc_cfg.collection_name
        self.container_manager.primary_to_transit_bucket()

        time.sleep(2)

        id_list = [doc.get("id") for doc in self.container_manager.transit_bucket]

        self.source_db_client.batch_update(
            collection_name=curr_collection_name,
            updates=self._reset_migration_marks(id_list=id_list),
        )

    def retry_insert(self):
        """Retries insertion of documents that failed during previous iteration."""

        i = 0
        while self.container_manager.retry_needed and i < 3:
            logging.info(
                f"{len(self.container_manager.retry_bucket)} items found in retry_bucket"
            )

            time.sleep(120 * (i + 1))

            logging.info(f"Starting retry attempt #{i + 1}")

            query_res = self.destination_db_client.batch_write(
                collection_name=self.current_doc_cfg.collection_name,
                documents=self.container_manager.retry_bucket,
            )

            self.container_manager.check_remove_from_retry_bucket(
                id_list=query_res.inserted_document_ids
            )

            logging.info(
                f"{len(query_res.inserted_document_ids)} items have been inserted after retry attempt"
            )

            i += 1

    def retry_fetch(self, find_all: bool = False):
        """Retries fetch operation."""

        i = 0

        while i < 3:
            logging.info(f"Retrying fetch operation with delays. Iteration #{i+1}")
            time.sleep((i + 1) * 120)  # Delaying for minimum 2 minutes
            i += 1
            try:
                query_res = self.source_db_client.find(
                    collection_name=self.current_doc_cfg.collection_name,
                    queries=self.current_doc_cfg.queries,
                    query_index_name=self.current_doc_cfg.query_index_name,
                    find_all=find_all
                )

                return query_res

            except RetryableFetchingError:
                continue

        raise FetchingTerminatedError(
            f"Terminating fetching documents from {self.current_doc_cfg.collection_name}"
        )

    async def insert_fetch_cycle(self):
        """The insert/fetch sequence ran asynchronously.

        used for repetitions
        """

        loop = asyncio.get_running_loop()

        await loop.run_in_executor(None, self.insert_update)
        await loop.run_in_executor(None, self.fetch)

    def insert_fetch_update_cycle(self):
        """Sync function for alternative lifecycle"""

        if self.container_manager.data_exists:
            query_res = self.insert()
            curr_collection_name = self.current_doc_cfg.collection_name

            # Suspicion is that on EC2, batch_update happens faster than fetch does
            # Which is causing the LastEvaluatedKey to be invalidated, since its out of the query results due to
            # is_migrated = True field.
            # So we do a synchronous fetch in here to avoid issues
            self.fetch()

            self.source_db_client.batch_update(
                collection_name=curr_collection_name,
                updates=self._generate_migration_marks(query_res.inserted_document_ids),
            )

            return query_res
        elif self.current_doc_cfg is not None:
            self.fetch()

    def container_monitor(self):
        """Check whether or not the containers are full."""

    def _generate_migration_marks(self, id_list: List[str]) -> List[dict]:
        """Generates migration marks for migrated documents."""

        migration_marks = []

        for doc_id in id_list:
            migration_marks.append({"id": doc_id, "is_migrated": True})

        return migration_marks

    def _reset_migration_marks(self, id_list: List[str]) -> List[dict]:
        """Resets migration marks for migrated documents."""

        migration_marks = []

        for doc_id in id_list:
            migration_marks.append({"id": doc_id, "is_migrated": False})

        return migration_marks

    def migrate(self, reset_migration: bool = False):
        """Script that starts the migration procedure."""

        # First run initializes containers
        self.fetch(find_all=reset_migration)

        while self.current_doc_cfg is not None:
            if reset_migration:
                logging.info(f"Initiating RESET of migration...")
                self.reset_migration()
                self.fetch(find_all=True)
            else:
                logging.info(f"Initiating migration operation...")
                self.insert_fetch_update_cycle()

        # 2. create task to write into destination
        # 3. create task to read a new batch from source
        # 4. await writer
        # 5. await reader
