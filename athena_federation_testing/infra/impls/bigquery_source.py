import json, logging
import importlib.resources as resources

from opensearchpy import OpenSearch

from athena_federation_testing.util.client import cfn_client, rds_client, aws_client_factory,glue_job_client
from athena_federation_testing.util import config_helper
from botocore.exceptions import ClientError
from athena_federation_testing.infra import common_infra
from athena_federation_testing.infra.federated_source import Federated_Source
from athena_federation_testing.resources.infra import cfn_template
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

resources.files(cfn_template).joinpath("config.properties")

class BigQuery_Federated_Source(Federated_Source, connection_type_name = "bigquery", athena_federation_sdk_folder_name="google-bigquery"):

    def get_glue_job_name(self):
        return "athena_integ_s3_to_bigquery"

    def get_glue_script_name(self):
        return "s3_to_bigquery.py"

    def get_glue_etl_connection_name(self):
        return "athena_integ_s3_to_bigquery_glue_connection"

    def construct_infra(self, parameter=None) -> None:
        logging.info("BIGQUERY_SOURCE does not support creating infra, please created manually before executing framework")

    def destroy_infra(self):
        bq_client = self._get_big_query_client()
        # We only delete what is the current select scale factor to avoid accidental deletion for rest of prefix start with*
        dataset_id = f"{bq_client.project}.{config_helper.get_tpcds_scale_factor()}"

        try:
            bq_client.delete_dataset(
                dataset_id,
                delete_contents=True,  # deletes all tables under dataset
                not_found_ok=True  # ignore if dataset not found
            )
            logging.info(f"Deleted dataset '{dataset_id}' and underlying tables.")
        except NotFound:
            logging.info(f"Dataset {dataset_id} not found.")

        self._destroy_glue_etl_resources()

    def create_glue_job_connection(self) -> None:
        credential = self._get_big_query_credential()
        connection_input = {
            "Name": self.get_glue_etl_connection_name(),
            "Description": "Athena Integration test BIGQUERY connector for ETL JOB",
            "ConnectionType": "BIGQUERY",
            "ConnectionProperties": {
                "JDBC_ENFORCE_SSL": "false",
                "KAFKA_SSL_ENABLED": "false",
                "ROLE_ARN": config_helper.get_glue_job_role_arn(),
                "PROJECT_ID": credential["project_id"],
            },
            "AuthenticationConfiguration": {
                "AuthenticationType": "CUSTOM",
                "SecretArn": self.secret_manager_client.get_secret_value(SecretId=config_helper.get_config("bigquery", "big_query_credential_name"))["ARN"]
            },
            "ValidateForComputeEnvironments": ["SPARK"]
        }
        glue_job_client.create_glue_job_connection(connection_input, self.get_connection_type_name())

    def create_database_local(self, database_name: str) -> None:
        bq_client = self._get_big_query_client()
        # big query "database name" (in bq it is called dataset). consist of projectid
        dataset_id = f"{bq_client.project}.{database_name}"

        try:
            bq_client.get_dataset(dataset_id)
            logging.info(f"BigQuery, dataset: {dataset_id} already exists")
        except NotFound:
            dataset = bigquery.Dataset(dataset_id)
            # dataset.location = "US"
            bq_client.create_dataset(dataset)
            logging.info(f"BigQuery, Created dataset {dataset_id}")

    def create_table_local(self, table_names: list) -> None:
        logging.info(f"Creating tables local: {table_names}, not supported for {self.get_connection_type_name()}")

    # same thing for create database locally
    def create_database_s3(self, database_name: str) -> None:
        self.create_database_local(database_name)

    def create_table_s3(self, table_names: list) -> None:
        bq_client = self._get_big_query_client()

        self.create_glue_job()
        project_id = self._get_big_query_credential()['project_id']
        # check if index exists, if exists skip
        for table_name in table_names:
            should_create = False
            try:
                table = bq_client.get_table(f"{project_id}.{config_helper.get_tpcds_scale_factor()}.{table_name}")  # API request
                logging.info(f"BQ Table {table_name} exists. skipping creation")
            except NotFound:
                should_create = True

            if should_create:
                # for each table we go thru for look and submit job this doesn't require public access as Glue will handle VPC connnectivity.
                logging.info(f"Uploading table: {table_name}")
                job_arguments = {
                    '--BUCKET_NAME': config_helper.get_glue_job_bucket_name(common_infra.get_account_id()),
                    '--SCALE': config_helper.get_tpcds_scale_factor_number(),
                    '--TABLE_NAME': table_name,
                    '--BQ_CONNECTION_NAME': self.get_glue_etl_connection_name(),
                    '--BQ_PROJECT_ID': project_id
                }
                run_id = glue_job_client.start_job(self.get_glue_job_name(), job_arguments)
                final_status = glue_job_client.monitor_job_run(self.get_glue_job_name(), run_id)

    def create_connector(self) -> None:
        self.construct_common_resource()
        parameters = {
            "connection-type": "BIGQUERY", #even tho it is showing as "ElasticSearch" connector in github, but we only have open search glue connection type
            "connection-properties": json.dumps(
                {
                    "spill_bucket": config_helper.get_s3_spill_bucket_name(common_infra.get_account_id()),
                    "disable_spill_encryption": "false",
                    "SecretArn": self.secret_manager_client.get_secret_value(SecretId=config_helper.get_config("bigquery", "big_query_credential_name"))["ARN"],
                    "PROJECT_ID": self._get_big_query_credential()['project_id'],
                    "description": "Athena integration test bigquery connector",

                }
            )
        }
        self._create_athena_prod_connector(parameters)

    def create_glue_job(self) -> None:
        # check if job exists, if not create job upload script
        if not glue_job_client.is_glue_job_exists(self.get_glue_job_name()):
            # Create a glue connection for ETL
            self.create_glue_job_connection()
            # upload job script to the bucket
            logging.info(f"Uploading script, job: {self.get_glue_job_name()}, script:{self.get_glue_script_name()}")
            glue_job_client.upload_glue_job_script(self.get_glue_script_name())

            logging.info(f"createing job: {self.get_glue_job_name()}")
            # OpenSearch only allow new versions hence using 5.0
            self._do_create_glue_job_with_parameter(glue_version= "5.0")

    # Big query credential format is different hence we need to explicitly create one (and manually create one)
    def _get_big_query_credential(self):
        client = aws_client_factory.get_aws_client("secretsmanager")
        secret_name = config_helper.get_config("bigquery", "big_query_credential_name")
        response = client.get_secret_value(SecretId=secret_name)
        secret = response['SecretString']
        return json.loads(secret)

    def _get_big_query_client(self):
        credential = self._get_big_query_credential()
        credentials = service_account.Credentials.from_service_account_info(credential)

        return bigquery.Client(credentials=credentials, project=credential["project_id"])
