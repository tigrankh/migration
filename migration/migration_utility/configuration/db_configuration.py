from pydantic import BaseModel, Field, validator

from migration.migration_utility.db_clients.dynamodb.dynamodb_client import (
    DynamoDbClient,
)
from migration.migration_utility.db_clients.generic import GenericClient
from migration.migration_utility.enums import Databases
from migration.migration_utility.exceptions import (
    UnknownDatabaseError,
    MissingRequiredConfigurationParamError,
)
from migration_utility.db_clients.mongodb.mongodb_client import MongoDbClient


class DbConfigurator(BaseModel):
    """Db"""

    database: Databases = Field(
        ..., description="name of the database", example="dynamodb"
    )
    database_name: str = Field(
        None,
        description="name of the database. required for certain databases like mongoDB",
    )
    connection_string: str = Field(
        None,
        description="connection string used to connect to the database, e.g. MongoDB",
    )
    batch_size: int = Field(
        100, description="the size of the batch that is read/written"
    )

    def create_client(self) -> GenericClient:
        """Creates a database client instance from the given configurations.

        Returns: instance of the database client
        """

        if self.database == Databases.DYNAMODB:
            return DynamoDbClient(batch_size=self.batch_size)
        elif self.database == Databases.MONGODB:
            return MongoDbClient(
                batch_size=self.batch_size,
                connection_string=self.connection_string,
                database_name=self.database_name,
            )
        else:
            raise UnknownDatabaseError(f"{self.database} is not a known database")

    @validator("connection_string")
    def require_for_mongo(cls, v, values):
        """makes sure that for certain databases connection_string is present."""

        if values["database"] == Databases.MONGODB and v is None:
            raise MissingRequiredConfigurationParamError(
                "Field connection_string cannot be None for MongoDB"
            )

        return v
