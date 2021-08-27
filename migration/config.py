from migration_utility.data_types import FieldQuery
from migration_utility.enums import Databases, FieldQueryOperation

# DOCUMENT CONFIGURATION SECTION

content_item_cfg = {
    "type": "content_item",
    "collection_name": "redacted-content-items-fargate-test",
    "query_index_name": "model-type-created-at-index",
    "queries": [
        {
            "field_name": "model_type",
            "operation": "eq",
            "value": "CONTENT_ITEM",
        },
        {
            "field_name": "created_at",
            "operation": "gte",
            "value": "2021-08-15T00:00:00+00:00",
        },
    ],
}

content_segment_cfg = {
    "type": "content_segment",
    "collection_name": "redacted-content-segments-fargate-test",
    "dominant_type": "content_item",
    "queries": [
        {
            "field_name": "created_at",
            "operation": "gte",
            "value": "2021-06-09T00:00:00+00:00",
        }
    ],
    "query_index_name": "created-at-index",
}

content_rendition_cfg = {
    "type": "content_rendition",
    "collection_name": "redacted-content-renditions-fargate-test",
    "dominant_type": "content_item",
    "queries": [
        {
            "field_name": "model_type",
            "operation": "eq",
            "value": "CONTENT_RENDITION",
        },
        {
            "field_name": "created_at",
            "operation": "gte",
            "value": "2021-02-01T00:00:00+00:00",
        },
    ],
    "query_index_name": "created-at-index",
}

organization_cfg = {
    "type": "organization",
    "collection_name": "redacted-organizations-fargate-test",
    "queries": [
        {
            "field_name": "model_type",
            "operation": "eq",
            "value": "ORGANIZATION",
        }
    ],
}

user_cfg = {
    "type": "user",
    "collection_name": "redacted-users-fargate-test",
}

document_cfgs = [
    content_item_cfg,
    # content_segment_cfg,
    #content_rendition_cfg,
    # organization_cfg,
    # user_cfg,
]


# DB CONFIGURATION SECTION
source_db_cfg = {
    "database": Databases.DYNAMODB,
}
destination_db_cfg = {
    "database": Databases.MONGODB,
    "database_name": "migrated_db",
    "connection_string": "mongodb://redacted-ai-azure-test:9EN9xImfcytQ3aJNSab4Wgyvel8GV6PVDjVJN87QFvIn5s5oH8nFmIcvezNUd7HW0ty4kZnOZIoIQ4ynNaTXgw==@redacted-ai-azure-test.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@redacted-ai-azure-test@",
}
internal_db_cfg = {
    "database": Databases.MONGODB,
    "database_name": "internal_db",
    "connection_string": "mongodb://redacted-ai-azure-test:9EN9xImfcytQ3aJNSab4Wgyvel8GV6PVDjVJN87QFvIn5s5oH8nFmIcvezNUd7HW0ty4kZnOZIoIQ4ynNaTXgw==@redacted-ai-azure-test.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@redacted-ai-azure-test@",
}
