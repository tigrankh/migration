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


def main():
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

    if len(sys.argv) == 2:
        reset_migration = sys.argv[1] == "reset"
        force_migration = sys.argv[1] == "force"
    else:
        reset_migration = False
        force_migration = False

    migration_ctrl.migrate(reset_migration=reset_migration, force_migration=force_migration)


if __name__ == "__main__":
    main()
