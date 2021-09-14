import argparse

from migration.config import (
    document_cfgs,
    source_db_cfg,
    destination_db_cfg,
    internal_db_cfg,
)
from migration.migration_utility.configuration.db_configuration import DbConfigurator
from migration.migration_utility.configuration.document_configuration import (
    DocumentConfiguration,
)
from migration.migration_utility.controller.migration_controller import (
    MigrationController,
)
import sys


def main(reset_migration: bool = False, force_migration: bool = False, id_list_path: str = None):
    """main."""

    document_config_models = [DocumentConfiguration(**cfg) for cfg in document_cfgs]
    source_db_cfg_model = DbConfigurator(**source_db_cfg)
    destination_db_cfg_model = DbConfigurator(**destination_db_cfg)
    internal_db_cfg_model = DbConfigurator(**internal_db_cfg)

    migration_ctrl = MigrationController(
        source_db_config=source_db_cfg_model,
        destination_db_config=destination_db_cfg_model,
        internal_db_config=internal_db_cfg_model,
        document_configs=document_config_models,
    )

    migration_ctrl.migrate(reset_migration=reset_migration, force_migration=force_migration)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--reset", action="store_true", help="Resets all previously migrated documents to is_migrated=True state")
    parser.add_argument("--force", action="store_true", help="Forces a repeated migration over all documents")
    parser.add_argument("--id_list_path", default=None, help="Path to a file with list of IDs to migrate")

    args = parser.parse_args()

    main(
        reset_migration=args.reset,
        force_migration=args.force,
        id_list_path=args.id_list_path
    )
