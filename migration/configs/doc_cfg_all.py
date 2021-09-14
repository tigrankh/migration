import os

# DOCUMENT CONFIGURATION SECTION
from typing import List


class FlatConfig:
    @property
    def document_cfgs(self) -> List[dict]:
        doc_configs = []
        doc_configs.extend(self.generate_flat_configs())

        return doc_configs

    def generate_flat_configs(self):
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

        return [
            content_item_cfg,
            content_segment_cfg,
            content_rendition_cfg,
            user_cfg,
            organization_cfg,
            allow_deny_keyword_cfg,
            allow_deny_list_cfg
        ]
