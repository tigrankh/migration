import os
from migration_utility.enums import Databases, FieldQueryOperation

# DOCUMENT CONFIGURATION SECTION

content_item_cfg = {
    "type": "content_item",
    "collection_name": f"redacted-content-items-{os.environ.get('PROJECT_ID')}",
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
    "collection_name": f"redacted-content-segments-{os.environ.get('PROJECT_ID')}",
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
    "collection_name": f"redacted-content-renditions-{os.environ.get('PROJECT_ID')}",
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
    "collection_name": f"redacted-organizations-{os.environ.get('PROJECT_ID')}",
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
    "collection_name": f"redacted-users-{os.environ.get('PROJECT_ID')}",
    "queries": [
        {
            "field_name": "model_type",
            "operation": "eq",
            "value": "USER",
        }
    ],
    "query_index_name": "model-type-created-at-index"
}

allow_deny_list_cfg = {
    "type": "user",
    "collection_name": f"redacted-allow-deny-list-{os.environ.get('PROJECT_ID')}",
    "queries": [
        {
            "field_name": "model_type",
            "operation": "eq",
            "value": "ALLOW_DENY_LIST",
        }
    ],
    "query_index_name": "model-type-created-at-index"
}

allow_deny_keyword_cfg = {
    "type": "user",
    "collection_name": f"redacted-allow-deny-list-{os.environ.get('PROJECT_ID')}",
    "queries": [
        {
            "field_name": "model_type",
            "operation": "eq",
            "value": "ALLOW_DENY_KEYWORD",
        }
    ],
    "query_index_name": "model-type-created-at-index"
}

document_cfgs = [
    content_item_cfg,
    content_segment_cfg,
    content_rendition_cfg,
    organization_cfg,
    user_cfg,
    allow_deny_list_cfg,
    allow_deny_keyword_cfg
]


# DB CONFIGURATION SECTION
source_db_cfg = {
    "database": Databases.DYNAMODB,
    "batch_size": 50
}
destination_db_cfg = {
    "database": Databases.MONGODB,
    "database_name": f"redacted-ai-{os.environ.get('PROJECT_ID')}",
    "batch_size": 50,
    "connection_string": os.environ.get("DEST_CONN_STR"),
}
internal_db_cfg = {
    "database": Databases.MONGODB,
    "database_name": "internal_db",
    "batch_size": 50,
    "connection_string": os.environ.get("INT_CONN_STR")
}
