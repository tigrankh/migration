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

    migration_ctrl.migrate()


if __name__ == "__main__":
    main()
