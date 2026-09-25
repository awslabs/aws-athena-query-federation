import pymysql, json, logging, time, oracledb,sys

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

# for SQLAlchemy 1.4 ref:https://stackoverflow.com/questions/74093231/nosuchmoduleerror-cant-load-plugin-sqlalchemy-dialectsoracle-oracledb
oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb

class Oracle_Federated_Source(JDBC_Source, connection_type_name="oracle", is_rds_source=True):
    def get_glue_job_name(self):
        return "athena_integ_s3_to_oracle"

    def get_glue_script_name(self):
        return "s3_to_oracle.py"

    def get_glue_etl_connection_name(self):
        return "athena_integ_s3_to_oracle_glue_connection"

    def create_database_local(self, database_name: str) -> None:
        engine = self.create_jdbc_engine(False)
        credential = self._get_rds_credential_from_secret_manager()
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            # Oracle has 3 level, create database first(catalog level) with tpcds
            # then create schema name based on scale factor
            # Given user = schema in Oracle, we are actually creating an User here
            sql = f"""
                DECLARE
                  v_count INTEGER;
                BEGIN
                  SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = :schemaname;

                  IF v_count = 0 THEN
                    EXECUTE IMMEDIATE 'CREATE USER "{database_name}" IDENTIFIED BY "{credential['password']}" ';
                    EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO "{database_name}"';
                  END IF;
                END;
                """
            conn.execute(text(sql), {"schemaname": database_name})
            logging.info(f"Schema/User '{database_name}' Created...")
            # Grant newly created user on write
            sql = f"""alter user "{database_name}" quota unlimited on USERS """
            logging.info(f"sql executed: {sql}")
            conn.execute(text(sql))

    # Override method as oracle has different syntax
    def create_table_s3(self, table_names: list) -> None:
        # below will require direct access.
        # We don't want to open public access (which could result in ticket) for too long hence just dropping table and create again.
        try:
            self._enable_rds_public_ccess()
            # DB might still busy, sleep for 10 sec
            time.sleep(10)
            self.create_database_local(config_helper.get_tpcds_scale_factor())
            # Create a connection with scale factor, given user = schema in oracle
            engine = self.create_jdbc_engine_with_schema(config_helper.get_tpcds_scale_factor())
            with engine.connect() as con:
                # if table exists, drop it as we will create a new one
                for table_name in table_names:
                    sql = f"""
                        BEGIN
                            EXECUTE IMMEDIATE 'DROP TABLE "{table_name}" ';
                        EXCEPTION
                            WHEN OTHERS THEN
                              IF SQLCODE != -942 THEN
                                 RAISE;
                              END IF;
                        END; \
                          """
                    con.execute(text(sql))
                    logging.info(f"Query:{sql} succceeded")

                    # This is the Glue ETL table, glue can always create table in upper case only, hence dropping the table in original form
                    sql = f"""
                        BEGIN
                            EXECUTE IMMEDIATE 'DROP TABLE {table_name}_upper';
                        EXCEPTION
                            WHEN OTHERS THEN
                              IF SQLCODE != -942 THEN
                                 RAISE;
                              END IF;
                        END; \
                          """
                    con.execute(text(sql))
                    logging.info(f"Query:{sql} succceeded")

                # Create a glue ETL
                self.create_glue_job()
                self.start_glue_job(table_names)
                # We do a CTAS from original upper case table to lower case
                for table_name in table_names:
                    sql = f"""CREATE TABLE "{table_name}" AS SELECT * FROM {table_name.upper()}_UPPER"""
                    con.execute(text(sql))


        except Exception as e:
            logging.error(f"🚨Create_data failed!: {e}")
            raise
        finally:
            self._disable_rds_public_ccess()

    def create_glue_job_connection(self) -> None:
        connection_input = {
            "Name": self.get_glue_etl_connection_name(),
            "Description": "Athena Integration test MySQL connector for ETL JOB",
            "ConnectionType": "ORACLE",
            "ConnectionProperties": {
                "HOST": rds_client.get_rds_db_host(
                    config_helper.get_rds_database_identifier_name(self.get_connection_type_name())),
                "ROLE_ARN": config_helper.get_glue_job_role_arn(),
                "DATABASE": "tpcds",
                "PORT": "1521",
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
        host = f"oracle://{credential['username']}:{credential['password']}"f"@{rds_client.get_rds_db_host(config_helper.get_rds_database_identifier_name(self.get_connection_type_name()))}:1521/tpcds"
        return create_engine(host)

    def create_jdbc_engine_with_schema(self, schema_name):
        credential = self._get_rds_credential_from_secret_manager()
        host = f"oracle://\"{schema_name}\":{credential['password']}"f"@{rds_client.get_rds_db_host(config_helper.get_rds_database_identifier_name(self.get_connection_type_name()))}:1521/tpcds"
        return create_engine(host)

    def start_glue_job(self, table_names: list):
        credential = self._get_rds_credential_from_secret_manager()
        host_name = rds_client.get_rds_db_host(
            config_helper.get_rds_database_identifier_name(self.get_connection_type_name()))
        job_arguments = {
            '--BUCKET_NAME': config_helper.get_glue_job_bucket_name(common_infra.get_account_id()),
            '--SCALE': config_helper.get_tpcds_scale_factor_number(),
            '--ORACLE_URL': f'jdbc:oracle:thin:@//{host_name}:1521/tpcds',
            '--ORACLE_USER': f"\"{config_helper.get_tpcds_scale_factor()}\"",
            '--ORACLE_PWD': credential['password'],
            '--TABLE_LIST': ','.join(table_names)
        }
        run_id = glue_job_client.start_job(self.get_glue_job_name(), job_arguments)
        final_status = glue_job_client.monitor_job_run(self.get_glue_job_name(), run_id)


    def create_connector(self) -> None:
        self.construct_common_resource()

        parameters = {
            "connection-type": self.get_connection_type_name().upper(),  # glue connection must be in upper
            "connection-properties": json.dumps(
                {
                    "spill_bucket": config_helper.get_s3_spill_bucket_name(common_infra.get_account_id()),
                    "disable_spill_encryption": "false",
                    "SecretArn": self.get_secrete_arn(),
                    "host": rds_client.get_rds_db_host(config_helper.get_rds_database_identifier_name(self.get_connection_type_name())),
                    "SecurityGroupIdList": [self.get_security_group_id()],
                    "SubnetId": self.get_subnet_1_id(),
                    "database": "tpcds",
                    "port": 1521,
                    "description": "Athena Integrration test Oracle connector",
                }
            )
        }
        self._create_athena_prod_connector(parameters)