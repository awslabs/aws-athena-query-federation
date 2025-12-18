import json, logging, snowflake.connector
import importlib.resources as resources

from athena_federation_testing.util.client import aws_client_factory, glue_job_client
from athena_federation_testing.util import config_helper
from athena_federation_testing.infra import common_infra
from athena_federation_testing.resources.infra import cfn_template
from athena_federation_testing.infra.impls.jdbc_source import JDBC_Source
from athena_federation_testing.util.tpcds import tpcds_reader
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

resources.files(cfn_template).joinpath("config.properties")

class Snowflake_Federated_Source(JDBC_Source, connection_type_name = "snowflake", is_rds_source=False):

    def get_glue_job_name(self):
        return "athena_integ_s3_to_snowflake"

    def get_glue_script_name(self):
        return "s3_to_snowflake.py"

    def get_glue_etl_connection_name(self):
        return "athena_integ_s3_to_snowflake_glue_connection"

    def construct_infra(self, parameter=None) -> None:
        logging.info(
            "SNOWFLAKE_SOURCE does not support creating infra, please created manually before executing framework")

    def destroy_infra(self):
        # We only delete what is the current select scale factor to avoid accidental deletion for rest of prefix start with*
        engine = self.create_jdbc_engine(False)
        with engine.connect() as con:
            for table_name in tpcds_reader.get_data_tables():
                try:
                    result = con.execute(
                        text(f"DROP TABLE IF EXISTS \"{config_helper.get_tpcds_scale_factor()}\".\"{table_name}\""))
                    result = con.execute(
                        text(f"DROP TABLE IF EXISTS \"{config_helper.get_tpcds_scale_factor()}\".{table_name}_upper"))
                    logging.info(f"Snowflake DROP TABLE IF EXISTS {table_name}")
                except ProgrammingError as e:
                    logging.info(e)
            con.execute(text(f"DROP SCHEMA IF EXISTS \"{config_helper.get_tpcds_scale_factor()}\""))
            logging.info(f"Snowflake DROP SCHEMA IF EXISTS \"{config_helper.get_tpcds_scale_factor()}\"")

        self._destroy_glue_etl_resources()

    def create_glue_job_connection(self) -> None:
        credential = self._get_snowflake_credential()

        connection_input = {
            "Name": self.get_glue_etl_connection_name(),
            "Description": "Athena Integration test SNOWFLAKE connector for ETL JOB",
            "ConnectionType": "SNOWFLAKE",
            "ConnectionProperties": {
                "JDBC_ENFORCE_SSL": "false",
                "ROLE_ARN": config_helper.get_glue_job_role_arn(),
                "DATABASE": credential["db"],
                "PORT": "443",
                "WAREHOUSE": credential["warehouse"],
                "HOST": credential["host_name"],
                "KAFKA_SSL_ENABLED": "false"
            },
            "AuthenticationConfiguration": {
                "AuthenticationType": "BASIC",
                "SecretArn": self.secret_manager_client.get_secret_value(
                    SecretId=config_helper.get_config(self.get_connection_type_name(), "credential_secret_name"))["ARN"]
            },
            "ValidateForComputeEnvironments": ["SPARK"]
        }
        glue_job_client.create_glue_job_connection(connection_input, self.get_connection_type_name())

    def create_jdbc_engine(self, with_db: bool = True):
        credential = self._get_snowflake_credential()

        user = credential["username"]
        password = credential["password"]
        account = credential["account_name"]
        warehouse = credential["warehouse"]
        database = credential["db"]

        if with_db:
            return create_engine(
                f"snowflake://{user}:{password}@{account}/{database}/{config_helper.get_tpcds_scale_factor()}?warehouse={warehouse}")
        else:
            return create_engine(
                f"snowflake://{user}:{password}@{account}/{database}?warehouse={warehouse}")

    def create_database_local(self, database_name: str) -> None:
        engine = self.create_jdbc_engine(False)
        with engine.connect() as con:
            con.execute(text(f"CREATE SCHEMA IF NOT EXISTS \"{config_helper.get_tpcds_scale_factor()}\""))
            logging.info(
                f"✅ Data source:{self.get_connection_type_name()}, Schema name:'{config_helper.get_tpcds_scale_factor()}' checked/created.")

    # same thing for create database locally
    def create_database_s3(self, database_name: str) -> None:
        self.create_database_local(database_name)

    def create_table_s3(self, table_names: list) -> None:
        self.create_glue_job()
        # check if index exists, if exists skip
        engine = self.create_jdbc_engine(True)
        for table_name in table_names:
            with engine.connect() as con:
                result = con.execute(
                    text(f"SHOW TABLES LIKE '{table_name}' in \"{config_helper.get_tpcds_scale_factor()}\""))

            row_table = result.fetchall()
            if row_table:
                logging.info(f"Skipping table: {table_name}, row_table:{row_table}")
            else:
                # for each table we go thru for look and submit job this doesn't require public access as Glue will handle VPC connnectivity.
                credential = self._get_snowflake_credential()
                logging.info(f"Uploading table: {table_name}")
                job_arguments = {
                    '--BUCKET_NAME': config_helper.get_glue_job_bucket_name(common_infra.get_account_id()),
                    '--SCALE': config_helper.get_tpcds_scale_factor_number(),
                    '--TABLE_NAME': table_name,
                    '--SF_CONNECTION_NAME': self.get_glue_etl_connection_name(),
                    '--SF_DATABASE_NAME':credential["db"]
                }
                run_id = glue_job_client.start_job(self.get_glue_job_name(), job_arguments)
                final_status = glue_job_client.monitor_job_run(self.get_glue_job_name(), run_id)

                # Due to Glue ETL job can't define schema, it will automatically set to upper case, hence use CTAS to use to lower case all entities
                with engine.connect() as con:
                    result = con.execute(
                        text(f"DESCRIBE TABLE \"{config_helper.get_tpcds_scale_factor()}\".{table_name}_upper"))
                    columns_info = result.fetchall()
                    # change column to lower cased
                    columns = [f'{col[0]} as "{col[0].lower()}"' for col in columns_info]  # wrap in double quotes
                    # CTAS query
                    columns_str = ", ".join(columns)
                    ctas_query = f'CREATE TABLE \"{config_helper.get_tpcds_scale_factor()}\".\"{table_name}\" AS SELECT {columns_str} FROM \"{config_helper.get_tpcds_scale_factor()}\".{table_name}_upper'
                    con.execute(text(ctas_query))

    def create_connector(self) -> None:
        self.construct_common_resource()
        credential = self._get_snowflake_credential()
        parameters = {
            "connection-type": "SNOWFLAKE",
            "connection-properties": json.dumps(
                {
                    "spill_bucket": config_helper.get_s3_spill_bucket_name(common_infra.get_account_id()),
                    "disable_spill_encryption": "false",
                    "SecretArn": self.secret_manager_client.get_secret_value(SecretId=config_helper.get_config(self.get_connection_type_name(), "credential_secret_name"))["ARN"],
                    "description": "Athena integration test snowflake connector",
                    "WAREHOUSE": credential["warehouse"],
                    "HOST": credential["host_name"],
                    "DATABASE": credential["db"],
                    "PORT": "443"
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
            # Snowflake only support with latest version
            self._do_create_glue_job_with_parameter(glue_version= "5.0")

    # Required manual creation for snowflake, hence need another credential
    def _get_snowflake_credential(self):
        client = aws_client_factory.get_aws_client("secretsmanager")
        secret_name = config_helper.get_config(self.get_connection_type_name(), "credential_secret_name")
        response = client.get_secret_value(SecretId=secret_name)
        secret = response['SecretString']
        return json.loads(secret)

    def _get_snowflake_conn(self):
        credential = self._get_snowflake_credential()
        return snowflake.connector.connect(
            user=credential["username"],
            password=credential["password"],
            account=credential["account_name"],
            warehouse=credential["warehouse"],
            database=credential["db"]
        )