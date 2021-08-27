from typing import List

import simplejson


class ContainerManager:
    """Container manager."""

    def __init__(self):
        """Initializes the manager."""

        self._primary_bucket = []
        self.transit_bucket = []
        self.retry_bucket = []
        self.new_arrival = False

    @property
    def retry_needed(self) -> bool:
        """
        Returns: True if the retry bucket contains any data. False otherwise

        """

        return bool(self.retry_bucket)

    @property
    def data_exists(self) -> bool:
        """
        Returns: True if there is data in the primary bucket. false otherwise

        """

        return bool(len(self._primary_bucket))

    def add_document(self, document: dict):
        """Adds the passed in document into the container.

        Args:
            document: the document to add into the container

        Returns: None
        """

        self.new_arrival = True if document else False

        document = simplejson.loads(simplejson.dumps(document, use_decimal=True))

        self._primary_bucket.append(document)

    def add_documents(self, documents: List[dict]):
        """Adds the passed in document into the container.

        Args:
            documents: the documents to add into the container

        Returns: None
        """

        self.new_arrival = True if documents else False

        if not self.new_arrival:
            return

        documents = [
            simplejson.loads(simplejson.dumps(doc, use_decimal=True))
            for doc in documents
        ]

        self._primary_bucket.extend(documents)

    def empty_container(self):
        """Resets the container to an empty state.

        Returns: None
        """

        self._primary_bucket = []
        self.transit_bucket = []
        self.retry_bucket = []

    def _roll_call_ids(self, list_to_check: List[str]) -> List[str]:
        """Roll calls ids in the list_to_check param by comparing IDs in list with list
        of ids registered in the transit bucket.

        Args:
            list_to_check: the list in question to compare

        Returns: list of IDs that are preset in the list_to_check
        """

        registered_list = [doc.get("id") for doc in self.transit_bucket]

        return list(set(registered_list).intersection(set(list_to_check)))

    def check_move_to_retry_bucket(self, id_list: List[str]):
        """Check the Removes list of given ids from the container.

        Args:
            id_list: list of ids to remove

        Returns: None
        """

        matched_ids = self._roll_call_ids(id_list)

        self.retry_bucket = [
            doc for doc in self.transit_bucket if doc.get("id") not in matched_ids
        ]
        self.transit_bucket = []

    def primary_to_transit_bucket(self):
        """Moves documents from the primary_bucket into transit_bucket for further
        pickup.

        Returns: None
        """

        self.transit_bucket.extend(self._primary_bucket)
        self._primary_bucket = []

    def check_remove_from_retry_bucket(self, id_list: List[str]):
        """Removes documents that succeeded retry attempt from retry_bucket."""

        matched_ids = self._roll_call_ids(id_list)

        self.retry_bucket = [
            doc for doc in self.retry_bucket if doc.get("id") not in matched_ids
        ]

    def empty_transit_bucket(self):
        """Empties the transit bucket."""

        self.transit_bucket = []
