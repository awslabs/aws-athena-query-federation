import os,sys, logging,argparse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from athena_federation_testing.infra.federated_source import Federated_Source
from athena_federation_testing.util.tpcds.data_generation import glue_tpcds_data_generator

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True
)
logger = logging.getLogger(__name__)
logger.info("Main started")


full_resource_list = ["dynamodb", "mysql", "documentdb","postgresql", "redshift", "sqlserver", "elasticsearch", "bigquery", "snowflake", "oracle"]
default_test_resource_list = ["dynamodb", "mysql", "documentdb","postgresql", "redshift", "sqlserver", "elasticsearch", "bigquery", "snowflake"]

ALLOWED_ACTIONS = [
    "generate_tpcds",
    "create_infra",
    "destroy_infra",
    "load_data",
    "delete_resource",
    "create_connector",
    "update_connector",
    "migrate_to_glue_catalog",
    "migrate_to_athena_catalog"
]

def handle_action(action, resources):
    """
    Main logic for handling different actions.
    You can expand this function with actual implementation.
    """
    logging.info(f"Selected action: {action}")
    logging.info(f"Resources: {resources}")
    federated_source = Federated_Source.create_federated_source(resources)
    match action:
        case "generate_tpcds":
            logging.info("Generating TPCDS dataset to s3 bucket")
            glue_tpcds_data_generator.generate_tpcds_data()
        case "create_infra":
            logging.info("Creating infrastructure...")
            federated_source.construct_infra()
        case "destroy_infra":
            logging.info("Destroying infrastructure...")
            federated_source.destroy_infra()
        case "load_data":
            logging.info("Loading data into resources...")
            federated_source.load_data()
        case "delete_resource":
            ## delete Glue connection, connectors, etl job, catalogs ... etc
            logging.info("Deleting resources...")
            federated_source.delete_resources()
        case "create_connector":
            ## Create connector through Athena CreateDataCatalog
            logging.info("Creating connector...")
            federated_source.create_connector()
        case "update_connector":
            logging.info("Updating connector...")
            federated_source.update_connector_with_local_repo()
        case "migrate_to_glue_catalog":
            logging.info("Migrating Athena catalog to Glue catalog...")
            federated_source.migrate_athena_catalog_to_glue_catalog_with_lake_formation()
        case "migrate_to_athena_catalog":
            logging.info("Migrating Glue catalog to Athena catalog...")
            federated_source.migrate_glue_catalog_to_athena_catalog()
        case _:
            logging.error(f"Unknown action. action={action}")
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Infra and data management script")

    parser.add_argument(
        "--action",
        required=True,
        choices=ALLOWED_ACTIONS,
        help=f"Action to perform. Allowed actions: {', '.join(ALLOWED_ACTIONS)}"
    )

    parser.add_argument(
        "--resources",
        required=False,
        default= ",".join(default_test_resource_list),
        help="Comma-separated list of resources. Example: snowflake,dynamodb"
    )

    args = parser.parse_args()

    # Split resources by comma and strip spaces
    resources = [r.strip() for r in args.resources.split(",") if r.strip()]

    for resource in resources:
        if resource not in full_resource_list:
            logging.error(f"Resource {resource} is not a valid resource")
            sys.exit(1)
        handle_action(args.action, resource)


if __name__ == "__main__":
    main()