import json, logging
import importlib.resources as resources
from athena_federation_testing.util.client import cfn_client, rds_client, aws_client_factory,glue_job_client
from athena_federation_testing.util import config_helper
from botocore.exceptions import ClientError
from athena_federation_testing.infra import common_infra
from athena_federation_testing.infra.federated_source import Federated_Source
from athena_federation_testing.resources.infra import cfn_template

resources.files(cfn_template).joinpath("config.properties")

# glue_job_name = "athena_integ_s3_to_docdb"
# glue_job_glue_connection_name = "athena_integ_s3_to_docdb_glue_connection"
# s3_to_docdb_script = "s3_to_docdb.py"

class DOCDB_Federated_Source(Federated_Source, connection_type_name = "documentdb", athena_federation_sdk_folder_name = "docdb"):
    def get_glue_job_name(self):
        return "athena_integ_s3_to_docdb"

    def get_glue_script_name(self):
        return "s3_to_docdb.py"

    def get_glue_etl_connection_name(self):
        return "athena_integ_s3_to_docdb_glue_connection"

    def create_glue_job_connection(self) -> None:
        connection_input = {
            "Name": self.get_glue_etl_connection_name(),
            "Description": "Athena Integration test docdb connector for ETL JOB",
            "ConnectionType": "DOCUMENTDB",
            "ConnectionProperties": {
                "HOST": rds_client.get_rds_db_host(
                    config_helper.get_rds_database_identifier_name(self.get_connection_type_name())),
                "ROLE_ARN": config_helper.get_glue_job_role_arn(),
                "PORT": "27017",
                "KAFKA_SSL_ENABLED": "false",
                "ENFORCE_SSL": "false",
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

    def create_database_local(self, database_name: str) -> None:
        print(f"Creating database: {database_name}, not supported for {self.get_connection_type_name()}")
        pass

    def create_table_local(self, table_names: list) -> None:
        print(f"Creating tables: {table_names}, not supported for {self.get_connection_type_name()}")

    def create_database_s3(self, database_name: str) -> None:
        # instead of creating table individually, just batch create db with create talbe to avoid open public access for too long
        pass

    def create_table_s3(self, table_names: list) -> None:
        # below will require direct access, hence we do it separate.
        # We don't want to open public access (which could result in ticket) for too long hence just dropping table and create again.
        credential = self._get_rds_credential_from_secret_manager()
        # check if job exists, if not create job upload script
        self.create_glue_job()

        host_name = rds_client.get_db_cluster(config_helper.get_rds_database_clsuter_name(self.get_connection_type_name()))

        # for each table we go thru for look and submit job this doesn't require public access as Glue will handle VPC connnectivity.
        logging.info(f"Uploading table: {table_names}")
        url = f'mongodb://{host_name}:27017/?ssl=true'
        job_arguments = {
            '--BUCKET_NAME': config_helper.get_glue_job_bucket_name(common_infra.get_account_id()),
            '--SCALE': config_helper.get_tpcds_scale_factor_number(),
            '--TABLE_LIST': ','.join(table_names),
            '--DOCDB_URL': url,
            '--DOCDB_USER': credential['username'],
            '--DOCDB_PWD': credential['password']
        }
        run_id = glue_job_client.start_job(self.get_glue_job_name(), job_arguments)
        final_status = glue_job_client.monitor_job_run(self.get_glue_job_name(), run_id)

    def create_connector(self) -> None:
        self.construct_common_resource() #delete this line later
        # common_infra.create_secret_for_rds_if_not_exist(), decide to change it required
        parameters = {
            "connection-type": self.get_connection_type_name().upper(), #glue connection must be in upper
            "connection-properties": json.dumps(
                {
                    "spill_bucket": config_helper.get_s3_spill_bucket_name(common_infra.get_account_id()),
                    "disable_spill_encryption": "false",
                    "SecretArn": self.get_secrete_arn(),
                    "host":rds_client.get_rds_db_host(config_helper.get_rds_database_identifier_name(self.get_connection_type_name())),
                    "SecurityGroupIdList":[self.get_security_group_id()],
                    "SubnetId":self.get_subnet_1_id(),
                    "port": 27017,
                    "description": "Athena Integrration test MySQL connector",
                    "JDBC_PARAMS": "ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
                }
            )
        }
        self._create_athena_prod_connector(parameters)

        #todo update connector with local version