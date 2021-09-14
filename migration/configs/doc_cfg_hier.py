import os

# DOCUMENT CONFIGURATION SECTION


def get_content_item_relatives(content_item_id: str):
    content_item_cfg = {
        "type": "content_item",
        "collection_name": f"redacted-content-items-{os.environ.get('PROJECT_ID')}",
        "queries": [
            {
                "field_name": "id",
                "operation": "eq",
                "value": content_item_id,
            }
        ],
    }

    content_segment_cfg = {
        "type": "content_segment",
        "collection_name": f"redacted-content-segments-{os.environ.get('PROJECT_ID')}",
        "related_document": {"type": "content_item", "relation_field": "content_item_id"},
        "queries": [
            {
                "field_name": "content_item_id",
                "operation": "eq",
                "value": content_item_id,
            }
        ],
        "query_index_name": "content-item-index",
    }

    content_rendition_cfg = {
        "type": "content_rendition",
        "collection_name": f"redacted-content-renditions-{os.environ.get('PROJECT_ID')}",
        "related_document": {"type": "content_item", "relation_field": "content_item_id"},
        "queries": [
            {
                "field_name": "content_item_id",
                "operation": "eq",
                "value": content_item_id,
            }
        ],
        "query_index_name": "content-item-index",
    }

    return [content_item_cfg, content_segment_cfg, content_rendition_cfg]


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
    "type": "allow_deny_list",
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
    "type": "allow_deny_keyword",
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

document_cfgs = []

for i in ["520931456552e86f01ec1cffa4ee7ae0", "a02adfbed33919b02c9e697b78d7d0b2"]:
    document_cfgs.extend(get_content_item_relatives(i))

document_cfgs += [
    organization_cfg,
    user_cfg,
    allow_deny_list_cfg,
    allow_deny_keyword_cfg
]
