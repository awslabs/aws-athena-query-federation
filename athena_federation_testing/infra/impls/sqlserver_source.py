import pymysql, json, logging, time, urllib

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


class SQLServer_Federated_Source(JDBC_Source, connection_type_name="sqlserver", is_rds_source=True):
    def get_glue_job_name(self):
        return "athena_integ_s3_to_sqlserver"

    def get_glue_script_name(self):
        return "s3_to_sqlserver.py"

    def get_glue_etl_connection_name(self):
        return "athena_integ_s3_to_sqlserver_glue_connection"

    def create_glue_job_connection(self) -> None:
        connection_input = {
            "Name": self.get_glue_etl_connection_name(),
            "Description": "Athena Integration test MySQL connector for ETL JOB",
            "ConnectionType": "SQLSERVER",
            "ConnectionProperties": {
                "HOST": rds_client.get_rds_db_host(
                    config_helper.get_rds_database_identifier_name(self.get_connection_type_name())),
                "ROLE_ARN": config_helper.get_glue_job_role_arn(),
                "DATABASE": "database",
                "PORT": "1433",
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

    def create_jdbc_engine(self, with_db: bool=True):
        credential = self._get_rds_credential_from_secret_manager()
        host = f"mssql+pymssql://{credential['username']}:{credential['password']}"f"@{rds_client.get_rds_db_host(config_helper.get_rds_database_identifier_name(self.get_connection_type_name()))}:1433"
        if with_db:
            host += "/tpcds"

        return create_engine(host, isolation_level="AUTOCOMMIT")

    def create_database_local(self, database_name: str) -> None:
        # below will require direct access, hence we do it separate.
        # We don't want to open public access (which could result in ticket) for too long hence just dropping table and create again.
        try:
            self._enable_rds_public_ccess()
            # print("put this back: self._enable_rds_public_ccess()")
            # DB might still busy, sleep for 10 sec
            time.sleep(10)
            credential = self._get_rds_credential_from_secret_manager()
            engine = self.create_jdbc_engine(False)
            with engine.connect() as conn:
                conn = conn.execution_options(isolation_level="AUTOCOMMIT")
                # SQLSERVER has 3 level, create database first(catalog level) with tpcds
                # SQL server doesn't support create schema if not exists
                conn.execute(text(f"""
                       IF NOT EXISTS (
                           SELECT name FROM sys.databases WHERE name = :dbname
                       )
                       BEGIN
                           EXEC('CREATE DATABASE [tpcds];')
                           EXEC('USE tpcds; GRANT CONTROL ON DATABASE::tpcds TO {credential['username']};')
                       END
                   """), {"dbname": "tpcds"})

            # using engine with default database as `tpcds`
            engine = self.create_jdbc_engine(True)
            with engine.connect() as conn:
                # Create Schema which use to represent the scale factor
                conn.execute(text(f"""
                                IF NOT EXISTS (
                                    SELECT schema_name
                                    FROM information_schema.schemata
                                    WHERE schema_name = :schema_name
                                )
                                BEGIN
                                    EXEC('CREATE SCHEMA [{database_name}];');
                                    GRANT CONTROL ON SCHEMA::{database_name} TO {credential['username']};
                                    ALTER USER {credential['username']} WITH DEFAULT_SCHEMA = {database_name};
                                END
                            """), {"schema_name": database_name})

            logging.info(f"schema: {database_name} create if not exists completed")

        except Exception as e:
            logging.error(f"🚨create_database_local failed!: {e}")
            raise
        finally:
            self._disable_rds_public_ccess()
            # print("put this back: self._disable_rds_public_ccess()")

    # Override create table, as dropping table then crete see some weird issue...
    # Given the behavior for glue etl is replacing hence not dropping table
    def create_table_s3(self, table_names: list) -> None:
        self.create_database_local(config_helper.get_tpcds_scale_factor())

        # Create a glue connection for ETL
        self.create_glue_job()
        self.start_glue_job(table_names)

    def start_glue_job(self, table_names: list):
        credential = self._get_rds_credential_from_secret_manager()
        host_name = rds_client.get_rds_db_host(
            config_helper.get_rds_database_identifier_name(self.get_connection_type_name()))
        job_arguments = {
            '--BUCKET_NAME': config_helper.get_glue_job_bucket_name(common_infra.get_account_id()),
            '--SCALE': config_helper.get_tpcds_scale_factor_number(),
            '--TABLE_LIST': ','.join(table_names),
            '--SQLSERVER_URL': f'jdbc:sqlserver://{host_name}:1433/tpcds',
            '--SQLSERVER_USER': credential['username'],
            '--SQLSERVER_PWD': credential['password']
        }
        run_id = glue_job_client.start_job(self.get_glue_job_name(), job_arguments)
        final_status = glue_job_client.monitor_job_run(self.get_glue_job_name(), run_id)

    def create_connector(self) -> None:
        self.construct_common_resource()  # delete this line later

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
                    "database": "tpcds",
                    "port": 1433,
                    "description": "Athena Integrration test SQLSERVER connector",
                }
            )
        }
        self._create_athena_prod_connector(parameters)
