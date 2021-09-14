from enum import Enum


class FieldQueryOperation(str, Enum):
    """Enum with operations allowed during field queries."""

    EQ = "eq"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"


class Databases(str, Enum):
    """Enum that holds the supported databases."""

    DYNAMODB = "dynamodb"
    MONGODB = "mongodb"


class MigrationStatus(str, Enum):
    """Enum with migration status."""

    CANCELLED = "cancelled"


class FlowNames(str, Enum):
    """Enum with flow names"""

    FLAT = "flat"
    HIERARCHICAL = "hierarchical"
