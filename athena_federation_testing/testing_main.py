import os,sys, logging,argparse
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

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

full_resource_list = ["dynamodb", "mysql", "documentdb","postgresql", "redshift", "sqlserver", "elasticsearch", "bigquery", "snowflake", "oracle"]
default_test_resource_list = ["dynamodb", "mysql", "documentdb","postgresql", "redshift", "sqlserver", "elasticsearch", "bigquery", "snowflake"]

ALLOWED_ACTIONS = [
    "test_athena",
    "test_glue",
    "release_test"
]
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 20000)

MAX_WORKERS = 4

def handle_action(action, resources, verbose=False, output_prefix=""):
    """
    Main logic for handling different actions.
    You can expand this function with actual implementation.
    """
    logging.info(f"Selected action: {action}")
    logging.info(f"Resources: {resources}")
    match action:
        case "test_athena":
            logging.info("running query with Athena Catalog...")
            _run_individual_test(resources, True, output_prefix)
        case "test_glue":
            logging.info("running query with Glue Catalog...")
            _run_individual_test(resources, False, output_prefix)
        case "release_test":
            logging.info("running connector release test...")
            run_release_test(resources, verbose, output_prefix)
        case _:
            logging.error(f"Unknown action. action={action}")
            sys.exit(1)

def _run_test_for_resource(resource, test_athena, test_glue, verbose, output_prefix):
    test = Federated_Testing.create_federated_source(resource)
    resource_df = pd.DataFrame()
    failed_method = None
    
    try:
        if test_athena:
            # metadata test
            metadata_df = test.execute_athena_metadata_test()
            print(metadata_df)
            resource_df = pd.concat([resource_df, metadata_df], ignore_index=True)
            # simple test
            simple_df = test.execute_athena_simple_select_test()
            print(simple_df)
            resource_df = pd.concat([resource_df, simple_df], ignore_index=True)
            # predicate test
            predicate_df = test.execute_athena_predicate_select_test()
            print(predicate_df)
            resource_df = pd.concat([resource_df, predicate_df], ignore_index=True)
        
        if test_glue:
            federated_source = Federated_Source.create_federated_source(resource)
            federated_source.migrate_athena_catalog_to_glue_catalog_with_lake_formation()
            test.grant_permission_on_glue_catalog()
            # metadata test
            metadata_df = test.execute_glue_federation_metadata_test()
            print(metadata_df)
            resource_df = pd.concat([resource_df, metadata_df], ignore_index=True)
            # simple test
            simple_df = test.execute_glue_federation_simple_select_test()
            print(simple_df)
            resource_df = pd.concat([resource_df, simple_df], ignore_index=True)
            # predicate test
            predicate_df = test.execute_glue_federation_predicate_select_test()
            print(predicate_df)
            resource_df = pd.concat([resource_df, predicate_df], ignore_index=True)

    except Exception as e:
        failed_method = str(e)
        print(e)
        if resource_df.empty:
            resource_df = pd.DataFrame([{"Test": "Test Failed", "State": "FAILED", "Description": f"Exception: {failed_method}"}])
    
    if verbose:
        logging.info("---------Test result---------")
        logging.info(resource_df)
    
    if output_prefix:
        output_file = f"{output_prefix}_{resource}.csv"
        resource_df.to_csv(output_file, index=False)
        logging.info(f"Test results for {resource} saved to {output_file}")
    
    failed_df = resource_df[resource_df["State"] == "FAILED"]
    failed_df["type"] = resource
    if failed_method:
        raise Exception(f"Test failed for {resource}: {failed_method}")
    return failed_df

def _run_tests_parallel(resources, test_athena, test_glue, verbose, output_prefix):
    total_failed_df = pd.DataFrame()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_run_test_for_resource, resource, test_athena, test_glue, verbose, output_prefix): resource for resource in resources}
        for future in as_completed(futures):
            resource = futures[future]
            try:
                failed_df = future.result()
                total_failed_df = pd.concat([total_failed_df, failed_df], ignore_index=True)
            except Exception as e:
                logging.error(f"Test failed for {resource}: {e}")
    
    if not total_failed_df.empty:
        logging.error("---------Failed Test---------")
        logging.error(total_failed_df)
        logging.error("Test failed exit with non-zero")
        sys.exit(1)

def run_release_test(resources, verbose, output_prefix=""):
    _run_tests_parallel(resources, test_athena=True, test_glue=True, verbose=verbose, output_prefix=output_prefix)

def _run_individual_test(resources, is_athena, output_prefix):
    _run_tests_parallel(resources, test_athena=is_athena, test_glue=not is_athena, verbose=False, output_prefix=output_prefix)

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

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output"
    )

    parser.add_argument(
        "--output_file_prefix",
        required=False,
        default= "",
        help="Prefix for output files. Each resource will generate a separate file: <prefix>_<resource>.csv"
    )

    args = parser.parse_args()

    # Split resources by comma and strip spaces
    resources = [r.strip() for r in args.resources.split(",") if r.strip()]

    for resource in resources:
        if resource not in full_resource_list:
            logging.error(f"Resource {resource} is not a valid resource")
            sys.exit(1)

    handle_action(args.action, resources, verbose=args.verbose, output_prefix=args.output_file_prefix)


if __name__ == "__main__":
    main()