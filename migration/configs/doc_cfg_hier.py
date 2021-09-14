import os

# DOCUMENT CONFIGURATION SECTION
from typing import List


class HierarchicalConfig:
    def __init__(self, id_file_path: str):
        self.id_file_path = id_file_path

    @property
    def document_cfgs(self) -> List[dict]:
        id_list = self.get_id_list_from_file()
        doc_configs = []

        for doc_id in id_list:
            doc_configs.extend(
                self.generate_hier_configs(content_item_id=doc_id)
            )

        doc_configs.extend(self.generate_flat_configs())

        return doc_configs

    def get_id_list_from_file(self) -> List[str]:
        id_list = []

        with open(self.id_file_path) as fh:
            for doc_id in fh.readlines():
                id_list.append(doc_id.strip())

        return id_list

    def generate_hier_configs(self, content_item_id: str):
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

    def generate_flat_configs(self):
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

        content_collection_cfg = {
            "type": "user",
            "collection_name": f"redacted-content-collections-{os.environ.get('PROJECT_ID')}",
            "queries": [
                {
                    "field_name": "model_type",
                    "operation": "eq",
                    "value": "CONTENT_COLLECTION",
                }
            ],
            "query_index_name": "model-type-created-at-index"
        }

        return [organization_cfg, user_cfg, allow_deny_list_cfg, allow_deny_keyword_cfg, content_collection_cfg]

