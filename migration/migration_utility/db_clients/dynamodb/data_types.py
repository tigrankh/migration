from typing import Any

from ...enums import FieldQueryOperation
from ...data_types import FieldQuery as FQ
from boto3.dynamodb.conditions import Key, Attr


class FieldQuery(FQ):
    """Model that holds information and actions for field queries."""

    def export_query(self, index_query: bool):
        """
        Exports query data that corresponds to the underlying database type
        Args:
            index_query: indicates whether the performed query is an index query or scan

        Returns: Query data for a single field

        """

        if self.operation == FieldQueryOperation.EQ:
            return (
                Key(self.field_name).eq(self.value)
                if index_query
                else Attr(self.field_name).eq(self.value)
            )
        elif self.operation == FieldQueryOperation.GT:
            return (
                Key(self.field_name).gt(self.value)
                if index_query
                else Attr(self.field_name).gt(self.value)
            )
        elif self.operation == FieldQueryOperation.GTE:
            return (
                Key(self.field_name).gte(self.value)
                if index_query
                else Attr(self.field_name).gte(self.value)
            )
        elif self.operation == FieldQueryOperation.LTE:
            return (
                Key(self.field_name).lte(self.value)
                if index_query
                else Attr(self.field_name).lte(self.value)
            )
        elif self.operation == FieldQueryOperation.LT:
            return (
                Key(self.field_name).lt(self.value)
                if index_query
                else Attr(self.field_name).lt(self.value)
            )
