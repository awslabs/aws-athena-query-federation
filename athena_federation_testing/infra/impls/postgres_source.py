import pymysql, json, logging, time

from athena_federation_testing.util.client import cfn_client, rds_client, aws_client_factory, glue_job_client
from athena_federation_testing.util import config_helper
from athena_federation_testing.util.tpcds import tpcds_reader
from botocore.exceptions import ClientError


from sqlalchemy import create_engine, text
from athena_federation_testing.infra import common_infra
from athena_federation_testing.infra.impls.jdbc_source import JDBC_Source
import importlib.resources as resources
from athena_federation_testing.resources.infra import cfn_template

resources.files(cfn_template).joinpath("config.properties")



class Postgres_Federated_Source(JDBC_Source, connection_type_name="postgresql", is_rds_source=True):

    def get_glue_job_name(self):
        return "athena_integ_s3_to_postgres"

    def get_glue_script_name(self):
        return "s3_to_postgresql.py"

    def get_glue_etl_connection_name(self):
        return "athena_integ_s3_to_postgresql_glue_connection"

    def create_glue_job_connection(self) -> None:
        connection_input = {
            "Name": self.get_glue_etl_connection_name(),
            "Description": "Athena Integration test POSTGRESQL connector for ETL JOB",
            "ConnectionType": "POSTGRESQL",
            "ConnectionProperties": {
                "HOST": rds_client.get_rds_db_host(
                    config_helper.get_rds_database_identifier_name(self.get_connection_type_name())),
                "ROLE_ARN": config_helper.get_glue_job_role_arn(),
                "DATABASE": "database",
                "PORT": "5432",
                "KAFKA_SSL_ENABLED": "false"
            },
            "PhysicalConnectionRequirements": {
                "SubnetId": self.get_subnet_1_id(),
                "SecurityGroupIdList": [
                    self.get_security_group_id()
                ],
                'AvailabilityZone': self.get_subnet_1_net_availability_zone()
            },
            "AuthenticationConfiguration": {
                "AuthenticationType": "BASIC",
                "SecretArn": self.get_secrete_arn()
            },
            "ValidateForComputeEnvironments": ["SPARK"]
        }
        glue_job_client.create_glue_job_connection(connection_input, self.get_connection_type_name())

    def create_jdbc_engine(self, with_db: bool = True):
        credential = self._get_rds_credential_from_secret_manager()

        return create_engine(f"postgresql+psycopg2://{credential['username']}:{credential['password']}"
                                 f"@{rds_client.get_rds_db_host(config_helper.get_rds_database_identifier_name(self.get_connection_type_name()))}:5432/postgres")

    # aws database level = schema in postgress
    def create_database_local(self, database_name: str) -> None:
        engine = self.create_jdbc_engine(False)
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {database_name}"))

    def start_glue_job(self, table_names: list):
        credential = self._get_rds_credential_from_secret_manager()
        host_name = rds_client.get_rds_db_host(
            config_helper.get_rds_database_identifier_name(self.get_connection_type_name()))
        job_arguments = {
            '--BUCKET_NAME': config_helper.get_glue_job_bucket_name(common_infra.get_account_id()),
            '--SCALE': config_helper.get_tpcds_scale_factor_number(),
            '--TABLE_LIST': ','.join(table_names),
            '--POSTGRESQL_URL': f'jdbc:postgresql://{host_name}:5432/postgres?currentSchema={config_helper.get_tpcds_scale_factor()}',
            '--POSTGRESQL_USER': credential['username'],
            '--POSTGRESQL_PWD': credential['password']
        }
        run_id = glue_job_client.start_job(self.get_glue_job_name(), job_arguments)
        final_status = glue_job_client.monitor_job_run(self.get_glue_job_name(), run_id)

    def create_connector(self):
        self.construct_common_resource()

        parameters = {
            "connection-type": self.get_connection_type_name().upper(),  # glue connection must be in upper
            "connection-properties": json.dumps(
                {
                    "spill_bucket": config_helper.get_s3_spill_bucket_name(common_infra.get_account_id()),
                    "disable_spill_encryption": "false",
                    "SecretArn": self.get_secrete_arn(),
                    "host": rds_client.get_rds_db_host(
                        config_helper.get_rds_database_identifier_name(self.get_connection_type_name())),
                    "SecurityGroupIdList": [self.get_security_group_id()],
                    "SubnetId": self.get_subnet_1_id(),
                    "database": "postgres",
                    "port": 5432,
                    "description": "Athena integration test PostGreSQL connector",
                }
            )
        }
        self._create_athena_prod_connector(parameters)