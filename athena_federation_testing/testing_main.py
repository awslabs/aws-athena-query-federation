import os,sys, logging,argparse
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from athena_federation_testing.testing.federated_testing import Federated_Testing
from athena_federation_testing.infra.federated_source import Federated_Source

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True
)
logger = logging.getLogger(__name__)
logger.info("Main started")

complete_resource_list = ["dynamodb", "mysql", "documentdb","postgresql", "redshift", "sqlserver", "elasticsearch", "bigquery", "snowflake"]

ALLOWED_ACTIONS = [
    "test_athena",
    "test_glue",
    "release_test"
]
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 20000)

def handle_action(action, resources, verbose=False):
    """
    Main logic for handling different actions.
    You can expand this function with actual implementation.
    """
    logging.info(f"Selected action: {action}")
    logging.info(f"Resources: {resources}")
    match action:
        case "test_athena":
            logging.info("running query with Athena Catalog...")
            _run_individual_test(resources, True)
        case "test_glue":
            logging.info("running query with Glue Catalog...")
            _run_individual_test(resources, False)
        case "release_test":
            logging.info("running connector release test...")
            run_release_test(resources, verbose)
        case _:
            logging.error(f"Unknown action. action={action}")
            sys.exit(1)

def run_release_test(resources, verbose):
    total_failed_df = pd.DataFrame()
    for resource in resources:
        test = Federated_Testing.create_federated_source(resource)
        resource_df = pd.DataFrame()
        resource_df = pd.concat([resource_df, test.execute_athena_metadata_test()], ignore_index=True)
        resource_df = pd.concat([resource_df, test.execute_athena_simple_select_test()], ignore_index=True)
        resource_df = pd.concat([resource_df, test.execute_athena_predicate_select_test()], ignore_index=True)

        federated_source = Federated_Source.create_federated_source(resource)
        federated_source.migrate_athena_catalog_to_glue_catalog_with_lake_formation()
        test.grant_permission_on_glue_catalog()
        resource_df = pd.concat([resource_df, test.execute_glue_federation_metadata_test()], ignore_index=True)
        resource_df = pd.concat([resource_df, test.execute_glue_federation_simple_select_test()], ignore_index=True)
        resource_df = pd.concat([resource_df, test.execute_glue_federation_predicate_select_test()], ignore_index=True)
        if verbose:
            logging.info("---------Test result---------")
            logging.info(resource_df)

        failed_df = resource_df[resource_df["State"] == "FAILED"]
        failed_df["type"] = resource
        total_failed_df = pd.concat([total_failed_df, failed_df], ignore_index=True)

    if not total_failed_df.empty:
        logging.error("---------Failed Test---------")
        logging.error(total_failed_df)
        logging.error("Test failed exit with non-zero")
        sys.exit(1)

def _run_individual_test(resources, is_athena):
    for resource in resources:
        if resource not in complete_resource_list:
            logging.error(f"Resource {resource} is not a valid resource")
            continue

        test = Federated_Testing.create_federated_source(resource)
        if is_athena:
            print(test.execute_athena_metadata_test())
            print(test.execute_athena_simple_select_test())
            print(test.execute_athena_predicate_select_test())
        else:
            test = Federated_Testing.create_federated_source(resource)
            test.grant_permission_on_glue_catalog()
            print(test.execute_glue_federation_metadata_test())
            print(test.execute_glue_federation_simple_select_test())
            print(test.execute_glue_federation_predicate_select_test())

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
        default= ",".join(complete_resource_list),
        help="Comma-separated list of resources. Example: snowflake,dynamodb"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output"
    )

    args = parser.parse_args()

    # Split resources by comma and strip spaces
    resources = [r.strip() for r in args.resources.split(",") if r.strip()]

    for resource in resources:
        if resource not in complete_resource_list:
            logging.error(f"Resource {resource} is not a valid resource")
            sys.exit(1)

    handle_action(args.action, resources, verbose=args.verbose)


if __name__ == "__main__":
    main()