import os
from migration_utility.enums import Databases, FieldQueryOperation

from configs.doc_cfg_all import document_cfgs
#from configs.doc_cfg_hier import document_cfgs

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
