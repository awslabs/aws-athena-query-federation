import pymysql, json, logging, time,boto3

from athena_federation_testing.util.client import cfn_client, rds_client, aws_client_factory, glue_job_client
from athena_federation_testing.util import config_helper
from athena_federation_testing.util.tpcds import tpcds_reader
from botocore.exceptions import ClientError
from sqlalchemy.engine.url import URL

from sqlalchemy import create_engine, text
from athena_federation_testing.infra import common_infra
from athena_federation_testing.infra.impls.jdbc_source import JDBC_Source
import importlib.resources as resources
from athena_federation_testing.resources.infra import cfn_template

resources.files(cfn_template).joinpath("config.properties")

redshift_client = aws_client_factory.get_aws_client('redshift')
terminal_statuses = {'available','stopped'}


class Redshift_Federated_Source(JDBC_Source, connection_type_name="redshift", is_rds_source=True):

    def _enable_rds_public_ccess(self):
        logging.info("Enabling Redshift Public Access")
        try:
            response = redshift_client.modify_cluster(
                ClusterIdentifier=config_helper.get_rds_database_identifier_name(self.get_connection_type_name()),
                PubliclyAccessible=True
            )
            print(f"enable response:{response}")
            self._wait_redshift_cluster_available(True)
        except ClientError as e:
            if "already publicly accessible" in e.response['Error']['Message']:
                logging.info("Cluster already publicly accessible")
            else:
                raise
        logging.info("Enabled Redshift Public Access")

    def _disable_rds_public_ccess(self):
        logging.info("Disabling Redshift Public Access")
        response = redshift_client.modify_cluster(
            ClusterIdentifier=config_helper.get_rds_database_identifier_name(self.get_connection_type_name()),
            PubliclyAccessible=False
        )
        logging.info("Disabled Redshift Public Access")

        self._wait_redshift_cluster_available(False)

    def _wait_redshift_cluster_available(self, is_enable: bool):
        timeout_seconds = 300
        polling_interval = 10
        start_time = time.time()
        redshift_cluster_name=config_helper.get_rds_database_identifier_name(self.get_connection_type_name())
        while True:
            response = redshift_client.describe_clusters(ClusterIdentifier=redshift_cluster_name)
            db_status = response['Clusters'][0]['ClusterStatus']
            public_access_setting = response['Clusters'][0]['PubliclyAccessible']

            if db_status in terminal_statuses and public_access_setting == is_enable:
                break

            if time.time() - start_time > timeout_seconds:
                logging.error("Timeout reached: DB instance did not become available within 5 minutes.")
                break

            logging.info(
                f"⏳ Modifying database config still in progress, identifier:{redshift_cluster_name} current setting:{public_access_setting}, target setting:{is_enable}, db status:{db_status}... waiting {polling_interval} seconds.")
            time.sleep(polling_interval)

        logging.info(f'✅ Database config updated, identifier:{redshift_cluster_name}')

    def get_glue_job_name(self):
        return "athena_integ_s3_to_redshift"

    def get_glue_script_name(self):
        return "s3_to_redshift.py"

    def get_glue_etl_connection_name(self):
        return "athena_integ_s3_to_redshift_glue_connection"

    def create_glue_job_connection(self) -> None:
        connection_input = {
            "Name": self.get_glue_etl_connection_name(),
            "Description": "Athena Integration test Redshift connector for ETL JOB",
            "ConnectionType": "REDSHIFT",
            "ConnectionProperties": {
                "HOST": self.get_redshift_host(),
                "ROLE_ARN": config_helper.get_glue_job_role_arn(),
                "DATABASE": "database",
                "PORT": "5439",
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

        url = URL.create(
            drivername='redshift+redshift_connector',  # indicate redshift_connector driver and dialect will be used
            host=self.get_redshift_host(),  # Amazon Redshift host
            port=5439,  # Amazon Redshift port
            database='tpcds',  #We created database(same level as catalog for redshift as `DBName: tpcds` in cfn template, hence hard coded it
            username=credential['username'],  # Amazon Redshift username
            password=credential['password']  # Amazon Redshift password
        )

        return create_engine(url)

    # Redshift middle layer is called schema, hence creating a schema
    def create_database_local(self, database_name: str) -> None:
        engine = self.create_jdbc_engine(False)
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")

            # Check if DB exists
            result = conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS tpcds.{database_name}"))
            logging.info(f"CREATE SCHEMA IF NOT EXISTS tpcds.{database_name} completed")

    def start_glue_job(self, table_names: list):
        credential = self._get_rds_credential_from_secret_manager()
        host_name = self.get_redshift_host()
        job_arguments = {
            '--BUCKET_NAME': config_helper.get_glue_job_bucket_name(common_infra.get_account_id()),
            '--SCALE': config_helper.get_tpcds_scale_factor_number(),
            '--TABLE_LIST': ','.join(table_names),
            '--REDSHIFT_URL': f'jdbc:redshift://{host_name}:5439/tpcds',
            '--REDSHIFT_USER': credential['username'],
            '--REDSHIFT_PWD': credential['password'],
            '--TempDir': f's3://{config_helper.get_s3_spill_bucket_name(common_infra.get_account_id())}/redshift_temp'
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
                    "host": self.get_redshift_host(),
                    "SecurityGroupIdList": [self.get_security_group_id()],
                    "SubnetId": self.get_subnet_1_id(),
                    "database": 'tpcds',
                    "port": 5439,
                    "description": "Athena Integrration test Redshift connector",
                }
            )
        }
        self._create_athena_prod_connector(parameters)


    def get_redshift_host(self):
        response = redshift_client.describe_clusters(
            ClusterIdentifier=config_helper.get_rds_database_identifier_name("redshift")
        )
        return response['Clusters'][0]['Endpoint']['Address']
