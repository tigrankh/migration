import os
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
            "value": "2021-06-09T00:00:00+00:00",
        },
    ],
}

content_segment_cfg = {
    "type": "content_segment",
    "collection_name": "redacted-content-segments-fargate-test",
    "dominant_type": "content_item",
    "queries": [
        {
            "field_name": "model_type",
            "operation": "eq",
            "value": "CONTENT_SEGMENT",
        },
        {
            "field_name": "created_at",
            "operation": "gte",
            "value": "2021-06-09T00:00:00+00:00",
        }
    ],
    "query_index_name": "model-type-created-at-index",
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
            "value": "2021-06-09T00:00:00+00:00",
        },
    ],
    "query_index_name": "model-type-created-at-index",
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
    "query_index_name": "model-type-created-at-index"
}

user_cfg = {
    "type": "user",
    "collection_name": "redacted-users-fargate-test",
    "queries": [
        {
            "field_name": "model_type",
            "operation": "eq",
            "value": "USER",
        }
    ],
    "query_index_name": "model-type-created-at-index"
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
    "batch_size": 10
}
destination_db_cfg = {
    "database": Databases.MONGODB,
    "database_name": "migrated_db",
    "batch_size": 10,
    "connection_string": os.environ.get("DEST_CONN_STR"),
}
internal_db_cfg = {
    "database": Databases.MONGODB,
    "database_name": "internal_db",
    "batch_size": 10,
    "connection_string": os.environ.get("INT_CONN_STR")
}
