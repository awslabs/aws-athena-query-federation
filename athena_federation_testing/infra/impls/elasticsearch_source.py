import json, logging
import importlib.resources as resources

from opensearchpy import OpenSearch

from athena_federation_testing.util.client import cfn_client, rds_client, aws_client_factory,glue_job_client
from athena_federation_testing.util import config_helper
from botocore.exceptions import ClientError
from athena_federation_testing.infra import common_infra
from athena_federation_testing.infra.federated_source import Federated_Source
from athena_federation_testing.resources.infra import cfn_template

resources.files(cfn_template).joinpath("config.properties")

class ElasticSearch_Federated_Source(Federated_Source, connection_type_name = "elasticsearch"):
    aws_opensearch_client = aws_client_factory.get_aws_client('opensearch')
    port = 443

    def get_glue_job_name(self):
        return "athena_integ_s3_to_elasticsearch"

    def get_glue_script_name(self):
        return "s3_to_elasticsearch.py"

    def get_glue_etl_connection_name(self):
        return "athena_integ_s3_to_elasticsearch_glue_connection"

    def construct_infra(self, parameter=None) -> None:
        credential = self._get_rds_credential_from_secret_manager()
        parameter = [
            {
                'ParameterKey': 'DBUsername',
                'ParameterValue': credential['username'],
            },
            {
                'ParameterKey': 'DBpwd',
                'ParameterValue': credential['password']
            },
            {
                'ParameterKey': 'DomainAccessARN',
                'ParameterValue': config_helper.get_config("elasticsearch", "admin_role_arn"),
            }
        ]

        super().construct_infra(parameter)

    def create_glue_job_connection(self) -> None:
        spark_properties = {
            "secretId": config_helper.get_config("common", "rds_secret_name"),
            "opensearch.nodes": f"https://{self._get_open_search_host()}",
            "opensearch.port": "443",
            "opensearch.aws.sigv4.region": config_helper.get_region(),
            "opensearch.nodes.wan.only": "true",
            "opensearch.aws.sigv4.enabled": "false"
        }

        # SparkProperties must be as a string not nested json object
        connection_input = {
            "Name": self.get_glue_etl_connection_name(),
            "Description": "Athena Integration test elasticsearch connector for ETL JOB",
            "ConnectionType": "OPENSEARCH",
            "ConnectionProperties": {
                 "SparkProperties": json.dumps(spark_properties)
            }
        }
        glue_job_client.create_glue_job_connection(connection_input, self.get_connection_type_name())

    def create_database_local(self, database_name: str) -> None:
        logging.info(f"Creating database_name local: {database_name}, not supported for {self.get_connection_type_name()}")

    def create_table_local(self, table_names: list) -> None:
        logging.info(f"Creating tables local: {table_names}, not supported for {self.get_connection_type_name()}")

    def create_database_s3(self, database_name: str) -> None:
        logging.info(f"Creating database_name local: {database_name}, not supported for {self.get_connection_type_name()}")

    def create_table_s3(self, table_names: list) -> None:
        credential = self._get_rds_credential_from_secret_manager()
        os_client = OpenSearch(
            hosts=[{'host': self._get_open_search_host(), 'port': self.port}],
            http_auth=(credential['username'], credential['password']),
            use_ssl=True,
            verify_certs=True
        )

        self.create_glue_job()

        # check if index exists, if exists skip
        for original_table_name in table_names:
            table_name = self._get_es_table_name(original_table_name)

            if os_client.indices.exists(index=table_name):
                logging.info(f"Index '{table_name}' exists , skipping creation")
            else:
                # for each table we go thru for look and submit job this doesn't require public access as Glue will handle VPC connnectivity.
                logging.info(f"Uploading table: {table_name}")
                job_arguments = {
                    '--BUCKET_NAME': config_helper.get_glue_job_bucket_name(common_infra.get_account_id()),
                    '--SCALE': config_helper.get_tpcds_scale_factor_number(),
                    '--TABLE_NAME': original_table_name,
                    '--ES_CONNECTION_NAME': self.get_glue_etl_connection_name()
                }
                run_id = glue_job_client.start_job(self.get_glue_job_name(), job_arguments)
                final_status = glue_job_client.monitor_job_run(self.get_glue_job_name(), run_id)


    def create_connector(self) -> None:
        self.construct_common_resource()
        parameters = {
            "connection-type": "OPENSEARCH", #even tho it is showing as "ElasticSearch" connector in github, but we only have open search glue connection type
            "connection-properties": json.dumps(
                {
                    "spill_bucket": config_helper.get_s3_spill_bucket_name(common_infra.get_account_id()),
                    "disable_spill_encryption": "false",
                    "SecretArn": self.get_secrete_arn(),
                    "domain_endpoint": f"tpcds=https://{self._get_open_search_host()}",
                    "description": "Athena integration test es/os connector"
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

    def _create_open_search_client(self):
        credential = self._get_rds_credential_from_secret_manager()

        client = OpenSearch(
            hosts=[{'host': self._get_open_search_host(), 'port': self.port}],
            http_auth=(credential['username'], credential['password']),
            use_ssl=True,
            verify_certs=True
        )

        return client

    def _get_open_search_host(self):
        response = self.aws_opensearch_client.describe_domain(DomainName=config_helper.get_config("elasticsearch", "domain_name"))
        return response['DomainStatus']['Endpoint']

    def _get_es_table_name(self, table_name):
        return f'{config_helper.get_tpcds_scale_factor()}_{table_name}'