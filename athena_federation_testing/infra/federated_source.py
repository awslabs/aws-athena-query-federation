from abc import ABC, abstractmethod
import time, subprocess, botocore.exceptions, json
from athena_federation_testing.util import config_helper
from athena_federation_testing.util.tpcds import tpcds_reader
from athena_federation_testing.util.client import rds_client, lakeformation_client,aws_client_factory, cfn_client, glue_job_client
from athena_federation_testing.infra import common_infra
from enum import Enum
import importlib.resources as resources
from athena_federation_testing.resources.infra import cfn_template

resources.files(cfn_template).joinpath("config.properties")
import logging

class Federated_Source(ABC):
    '''
    The `data_sources/__init__.py` imports will cause the registry to be populated.
    So when you add a new datasource, make sure you add the corresponding
    import in `data_sources/__init__.py` as well.
    '''
    registry = {}

    athena_catalog_terminal_statuses = ['CREATE_COMPLETE', 'CREATE_FAILED', 'CREATE_FAILED_CLEANUP_COMPLETE', 'CREATE_FAILED_CLEANUP_FAILED', 'DELETE_COMPLETE', 'DELETE_FAILED']
    athena_recourse_creation_prefix = 'athenafederatedcatalog_'
    glue_connection_arn_template = "arn:aws:glue:{}:{}:connection/{}"
    ecr_custom_repo_prefix = 'athena-federation-integ-repository-'

    timeout_seconds = 300
    polling_interval = 10

    athena_client = aws_client_factory.get_aws_client('athena')
    glue_client = aws_client_factory.get_aws_client('glue')

    cf_client = aws_client_factory.get_aws_client('cloudformation')
    secret_manager_client = aws_client_factory.get_aws_client('secretsmanager')
    ec2_client = aws_client_factory.get_aws_client('ec2')
    ecr_client = aws_client_factory.get_aws_client('ecr')
    lambda_client = aws_client_factory.get_aws_client('lambda')

    class LoadDataMode(Enum):
        LOCAL = "LOCAL"
        S3 = 's3'

    def __init_subclass__(cls, connection_type_name, athena_federation_sdk_folder_name=None, is_rds_source=False):
        print(f'registering {connection_type_name}')
        if connection_type_name is None:
            raise ValueError("connection_type_name must be provided")
        Federated_Source.registry[connection_type_name] = cls

        cls.connection_type_name = connection_type_name
        if athena_federation_sdk_folder_name is None:
            cls.athena_federation_sdk_folder_name = connection_type_name
        else:
            cls.athena_federation_sdk_folder_name = athena_federation_sdk_folder_name

        cls.is_rds_source = is_rds_source

    # Create federated source based on connection name
    @classmethod
    def create_federated_source(cls, connection_type_name, *args, **kwargs):
        if connection_type_name not in cls.registry:
            raise ValueError(f"Unknown connection type: {connection_type_name}")
        return cls.registry[connection_type_name](*args, **kwargs)

    @abstractmethod
    def create_database_local(self, database_name: str) -> str:
        pass

    @abstractmethod
    def create_table_local(self, table_name: list) -> None:
        pass

    @abstractmethod
    def create_database_s3(self, database_name: str) -> None:
        pass

    @abstractmethod
    def create_table_s3(self, table_name: list) -> None:
        pass

    @abstractmethod
    def create_connector(self) -> None:
        pass

    @abstractmethod
    def get_glue_job_name(self):
        pass

    @abstractmethod
    def get_glue_script_name(self):
        pass

    @abstractmethod
    def get_glue_etl_connection_name(self):
        pass

    @abstractmethod
    def create_glue_job_connection(self) -> None:
        pass

    def create_glue_job(self) -> None:
        # check if job exists, if not create job upload script
        if not glue_job_client.is_glue_job_exists(self.get_glue_job_name()):
            # Create a glue connection for ETL
            self.create_glue_job_connection()
            # upload job script to the bucket
            logging.info(f"Uploading script, job: {self.get_glue_job_name()}, script:{self.get_glue_script_name()}")
            glue_job_client.upload_glue_job_script(self.get_glue_script_name())

            logging.info(f"createing job: {self.get_glue_job_name()}")
            self._do_create_glue_job_with_parameter()

    def _do_create_glue_job_with_parameter(self, glue_version = "3.0"):
        try:
            # Create Glue ETL Job
            job_args = {
                "Name": self.get_glue_job_name(),
                "Role": config_helper.get_glue_job_role_arn(),
                "Command": {
                    "Name": "glueetl",
                    "ScriptLocation": f's3://{config_helper.get_glue_job_bucket_name(common_infra.get_account_id())}/scripts/{self.get_glue_script_name()}',
                    "PythonVersion": "3"
                },
                "DefaultArguments": {
                    "--job-language": "python"
                },
                "GlueVersion": glue_version,
                "MaxRetries": 0,
                "Timeout": 2880,  # 48 hours
                "NumberOfWorkers": 5,
                "WorkerType": "G.2X",
                "ExecutionProperty": {
                    "MaxConcurrentRuns": 2
                }
            }

            # Attach Glue connection to glue ETL job for those who required a glue connection.
            # we do this by checking if glue connectio is not null, if not null we append glue connection.
            if self.get_glue_etl_connection_name() is not None:
                job_args["Connections"] = {
                    "Connections": [self.get_glue_etl_connection_name()]
                }
            aws_client_factory.get_aws_client("glue").create_job(**job_args)

        except Exception as e:
            logging.error(f"Error creating/starting job: {str(e)}")
            raise e

    def construct_infra(self, parameter=None) -> None:
        self.construct_common_resource()
        common_infra.construct_vpc_stack_if_not_exist()
        stack_name = config_helper.get_database_stack_name(self.get_connection_type_name())
        if cfn_client.is_cfn_exists(stack_name):
            logging.info(f'ℹ️ Stack name:{stack_name} exists, skipping creation')
            return
        yaml = resources.files(cfn_template).joinpath(f'{self.get_connection_type_name()}.yaml')

        credential = self._get_rds_credential_from_secret_manager()
        if parameter is None:
            parameter = [
                {
                    'ParameterKey': 'DBUsername',
                    'ParameterValue': credential['username'],
                },
                {
                    'ParameterKey': 'DBpwd',
                    'ParameterValue': credential['password']
                }
            ]

        cfn_client.construct_cfn_resource(yaml, stack_name, parameter)

    def destroy_infra(self) -> None:
        self._destroy_glue_etl_resources()
        response = self.cf_client.delete_stack(
            StackName=config_helper.get_database_stack_name(self.get_connection_type_name()))
        logging.info(
            f'Started deleting {self.get_connection_type_name()} stack: {config_helper.get_database_stack_name(self.get_connection_type_name())}. response:{response}')

    def _destroy_glue_etl_resources(self):
        if self.get_glue_etl_connection_name() is not None:
            try:
                logging.info(f'Deleting Glue ETL connection name:{self.get_glue_etl_connection_name()}.')
                self.glue_client.delete_connection(ConnectionName=f"{self.get_glue_etl_connection_name()}")
                logging.info(f'✅ Successfully deleted Glue ETL connection arn:{self.get_glue_etl_connection_name()}.')
            except Exception as e:
                logging.error(f'🚨 Deleting Failed!, Glue ETL connection name:{self.get_glue_etl_connection_name()}, exception: {e}')

        if self.get_glue_job_name() is not None:
            try:
                logging.info(f'Deleting Glue ETL job name:{self.get_glue_job_name()}.')
                self.glue_client.delete_job(JobName=f"{self.get_glue_job_name()}",)
                logging.info(f'✅ Successfully deleted Glue ETL job name:{self.get_glue_job_name()}.')
            except Exception as e:
                    logging.error(f'🚨 Deleting Failed!, Glue ETL job name:{self.get_glue_job_name()}, exception: {e}')

    def load_data(self) -> None:
        load_mode = Federated_Source.LoadDataMode[config_helper.get_load_data_model()]
        logging.info(f"load data mode: {load_mode}")
        if load_mode == Federated_Source.LoadDataMode.LOCAL:
            if int(config_helper.get_tpcds_scale_factor_number()) > 1:
                raise Exception(f"Load_data local only supports scale factor '1'")
            self.load_data_local()
        elif load_mode == Federated_Source.LoadDataMode.S3:
            self.load_data_s3()

    def load_data_local(self) -> None:
        try:
            self.create_database_local(config_helper.get_tpcds_scale_factor())
            self.create_table_local(tpcds_reader.get_data_tables())
        except Exception as e:
            logging.error(f"🚨Create_data failed! for local mode: {e}")

    def load_data_s3(self) -> None:
        self.create_database_s3(config_helper.get_tpcds_scale_factor())
        self.create_table_s3(tpcds_reader.get_data_tables())

    def get_connection_type_name(self) -> str:
        return type(self).connection_type_name

    def get_athena_federation_sdk_folder_name(self) -> str:
        return type(self).athena_federation_sdk_folder_name

    def is_rds(self) -> bool:
        return type(self).is_rds_source

    def get_ecr_repository_name(self) -> str:
        return f"{self.ecr_custom_repo_prefix}{self.get_connection_type_name()}".lower()

    def update_connector_with_local_repo(self) -> None:
        self._build_connector_jars()
        self._ecr_set_up()
        self._build_connector_image()
        self._update_lambda_image()
        self._ecr_clean_up()

    def construct_common_resource(self):
        common_infra.construct_s3_spill_bucket_if_not_exist()

    def migrate_athena_catalog_to_glue_catalog_with_lake_formation(self) -> None:
        logging.info(f"migrate_athena_catalog_to_glue_catalog_with_lake_formation")
        glue_catalog_name = config_helper.get_glue_catalog_name(self.get_connection_type_name())
        athena_catalog_name = config_helper.get_athena_catalog_name(self.get_connection_type_name())
        # Check if check glue catalog exist
        try:
            glue_catalog = self.glue_client.get_catalog(CatalogId=glue_catalog_name)
            if 'FederatedCatalog' in glue_catalog['Catalog'] and 'ConnectionName' in glue_catalog['Catalog']['FederatedCatalog']:
                logging.info(f'ℹ️Catalog {glue_catalog_name} found in glue, skipping migration.')
            else:
                raise ValueError(f"Glue Catalog:{glue_catalog_name} exists and it is not FederatedCatalog.")
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'EntityNotFoundException':
                logging.info(f'ℹ️Catalog {glue_catalog_name} not found in glue, migrating from Athena Catalog to Glue...')

        # Check if Athena Catalog exists=
        get_catalog_response = self.athena_client.get_data_catalog(Name=athena_catalog_name)

        if get_catalog_response['DataCatalog']['Type'] == 'FEDERATED' and get_catalog_response['DataCatalog']['Status'] != 'CREATE_COMPLETE':
            msg = f'❌ Athena catalog not ready to migrate ... name:{athena_catalog_name} status: {get_catalog_response["DataCatalog"]["Status"]}'
            logging.error(msg)
            raise RuntimeError(msg)

        if get_catalog_response['DataCatalog']['Type'] == 'FEDERATED':
            glue_connection_arn = get_catalog_response['DataCatalog']['Parameters']['connection-arn']
        elif get_catalog_response['DataCatalog']['Type'] == 'LAMBDA':
            catalog_arn = f"arn:aws:athena:{config_helper.get_region()}:{common_infra.get_account_id()}:datacatalog/{athena_catalog_name}"
            tags_response = self.athena_client.list_tags_for_resource(ResourceARN=catalog_arn)
            glue_connection_arn = next((tag['Value'] for tag in tags_response['Tags'] if tag['Key'] == 'connection-arn'), None)
            if not glue_connection_arn:
                raise ValueError(f"Tag 'connection-arn' not found for LAMBDA catalog {athena_catalog_name}")


        lakeformation_client.register_glue_connection(glue_connection_arn,config_helper.get_lake_formation_admin_role_arn())

        logging.info(f'✅ LakeFormation registered resource... resource_arn:{glue_connection_arn}')

        catalog_parameter = {
            'Description': "athena federation integration catalog",
            'FederatedCatalog': {
                'Identifier': '*',
                'ConnectionName':f"{self.athena_recourse_creation_prefix}{athena_catalog_name}"
            },
            'CreateTableDefaultPermissions':[],
            'CreateDatabaseDefaultPermissions':[]
        }
        response = self.glue_client.create_catalog(
            Name=glue_catalog_name,
            CatalogInput=catalog_parameter
        )

        self.athena_client.delete_data_catalog(Name=athena_catalog_name, DeleteCatalogOnly=True)
        logging.info(f'✅ Athena Catalog delete catalog only deleted... name:{athena_catalog_name}')
        logging.info(f'✅ Glue Federated catalog created. catalog_name:{glue_catalog_name}')

    def migrate_glue_catalog_to_athena_catalog(self) -> None:
        logging.info(f"migrate_glue_catalog_to_athena_catalog")
        glue_catalog_name = config_helper.get_glue_catalog_name(self.get_connection_type_name())
        athena_catalog_name = config_helper.get_athena_catalog_name(self.get_connection_type_name())
        # Check if check glue catalog exist
        try:
            glue_catalog = self.glue_client.get_catalog(CatalogId=glue_catalog_name)
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'EntityNotFoundException':
                raise ValueError(f"Glue Catalog:{glue_catalog_name} does NOT exists. Nothing to migrate")

        # Check if Athena Catalog exists
        try:
            athena_catalog_response = self.athena_client.get_data_catalog(Name=athena_catalog_name)
            if athena_catalog_response['DataCatalog']['Status'] != 'DELETE_COMPLETE':
                msg = f'❌ Athena catalog exists and not in DELETE_COMPLETE status ... name:{athena_catalog_name} status: {athena_catalog_response["DataCatalog"]["Status"]}'
                logging.error(msg)
                raise RuntimeError(msg)
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] != 'InvalidRequestException' or 'not found' not in error.response['Error']['Message']:
                raise error
            logging.info(f'ℹ️ Athena catalog {athena_catalog_name} does not exist, proceeding with migration.')

        connection_name = glue_catalog['Catalog']['FederatedCatalog']['ConnectionName']
        glue_connection = self.glue_client.get_connection(Name=connection_name)
        lambda_arn = glue_connection['Connection']['AthenaProperties']['lambda_function_arn']
        glue_connection_arn = self.glue_connection_arn_template.format(config_helper.get_region(), common_infra.get_account_id(), connection_name)

        lakeformation_client.deregister_resource(glue_connection_arn)
        logging.info(f'✅ LakeFormation deregistered resource... resource_arn:{glue_connection_arn}')
        
        self.glue_client.delete_catalog(CatalogId=glue_catalog_name)
        logging.info(f'✅ Glue catalog deleted... name:{glue_catalog_name}')
        
        self.athena_client.create_data_catalog(Name=athena_catalog_name, Type='LAMBDA', Parameters={'function': lambda_arn}, Tags=[{'Key': 'connection-arn', 'Value': glue_connection_arn}])
        logging.info(f'✅ Athena catalog created... name:{athena_catalog_name} lambda_arn:{lambda_arn}')

    def delete_resources(self) -> None:
        athena_catalog_name = config_helper.get_athena_catalog_name(self.get_connection_type_name())
        glue_catalog_name = config_helper.get_glue_catalog_name(self.get_connection_type_name())
        try:
            glue_connection_arn = self.glue_connection_arn_template.format(config_helper.get_region(), common_infra.get_account_id(), f"{self.athena_recourse_creation_prefix}{athena_catalog_name}")
            logging.info(f'Unregistering DataLakeResource, arn:{glue_connection_arn}.')
            lakeformation_client.deregister_resource(glue_connection_arn)
            logging.info(f'✅ Successfully unregistering DataLakeResource arn:{glue_connection_arn}.')
        except Exception as e:
            logging.error(f'🚨 Unregistering DataLakeResource failed, connection_arn:{glue_connection_arn}, exception: {e}')

        try:
            logging.info(f'Deleting Glue catalog name:{glue_catalog_name}.')
            self.glue_client.delete_catalog(CatalogId=glue_catalog_name)
            logging.info(f'✅ Successfully deleted Glue catalog name:{glue_catalog_name}.')
        except Exception as e:
            logging.error(f'🚨 Delete glue catalog name failed, name:{glue_catalog_name}, exception: {e}')

        try:
            logging.info(f'Deleting Glue connection name:{self.athena_recourse_creation_prefix}{athena_catalog_name}.')
            self.glue_client.delete_connection(ConnectionName=f"{self.athena_recourse_creation_prefix}{athena_catalog_name}")
            logging.info(f'✅ Successfully deleted Glue connection arn:{glue_connection_arn}.')
        except Exception as e:
            logging.error(f'🚨 Deleting Failed!, Glue connection name:{self.athena_recourse_creation_prefix}{athena_catalog_name}, exception: {e}')

        try:
            lambda_stack_name = f"{self.athena_recourse_creation_prefix}{athena_catalog_name}".replace("_", "-")
            logging.info(f'Deleting Cloudformation Lambda stack name:{lambda_stack_name}')
            self.cf_client.delete_stack(StackName=lambda_stack_name)
            logging.info(f'✅ Successfully deleted Cloudformation Lambda stack name:{lambda_stack_name}.')
        except Exception as e:
            logging.error(f'🚨 Deleting Failed!, Cloudformation Lambda stack name:{lambda_stack_name}, exception: {e}')

        try:
            logging.info(f'Deleting Athena catalog name:{athena_catalog_name}')
            self.athena_client.delete_data_catalog(Name=athena_catalog_name, DeleteCatalogOnly=True)
            logging.info(f'✅ Successfully deleted Athena catalog name:{athena_catalog_name}.')
        except botocore.exceptions.ClientError as error:
            if error.response['AthenaErrorCode'] == 'DATACATALOG_NOT_FOUND' :
                logging.info(f'ℹ️Catalog {athena_catalog_name} already deleted in Athena, skipping deletion.')
            else:
                logging.error(f'🚨 Deleting Failed!, deleted Athena catalog name:{athena_catalog_name}, exception: {error}')
        except Exception as e:
            logging.error(f'🚨 Deleting Failed!, deleted Athena catalog name:{athena_catalog_name}, exception: {e}')

    def get_secrete_arn(self):
        secret_response = self.secret_manager_client.get_secret_value(SecretId=config_helper.get_config("common", "rds_secret_name"))
        return secret_response['ARN']

    def get_security_group_id(self):
        response = self.ec2_client.describe_security_groups(Filters=[{'Name': 'tag:Name', 'Values': [config_helper.get_security_group_name()]}])
        return response['SecurityGroups'][0]['GroupId']

    def get_subnet_1_id(self):
        response = self.ec2_client.describe_subnets(Filters=[{'Name': 'tag:Name', 'Values': [config_helper.get_subnet1_name()]}])
        return response['Subnets'][0]['SubnetId']

    def get_subnet_1_net_availability_zone(self):
        response = self.ec2_client.describe_subnets(Filters=[{'Name': 'tag:Name', 'Values': [config_helper.get_subnet1_name()]}])
        return response['Subnets'][0]['AvailabilityZone']

    def _build_connector_jars(self) -> None:
        command = [
            "mvn",
            "clean",
            "install",
            "-q",
            "-pl", f"athena-{self.get_athena_federation_sdk_folder_name()}",
            "-am",
            "-DskipTests",
            "-Dcheckstyle.skip=true",
            "-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN",
            "--no-transfer-progress"
        ]
        try:
            logging.info(f"cmd:{command}, cwd:{config_helper.get_repo_root()}")
            subprocess.run(command, check=True, cwd=config_helper.get_repo_root())
            logging.info("✅ Maven build completed successfully.")
        except subprocess.CalledProcessError as e:
            logging.error(f"❌ Maven build failed with error:{e}")
            raise e

    # Get the login password from AWS and created ecr repo
    def _ecr_set_up(self):
        try:
            self.ecr_client.create_repository(repositoryName=self.get_ecr_repository_name())
            logging.info(f"ECR repository '{self.get_ecr_repository_name()}' created successfully.")
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'RepositoryAlreadyExistsException':
                logging.info(f"ECR repository '{self.get_ecr_repository_name()}' already exists. Skipping creation.")
            else:
                raise error
        try:
            #aws ecr create-repository --repository-name athena-federation-repository-$CONNECTOR_NAME --region us-east-1
            subprocess.run(["aws", "ecr", "create-repository", "--repository-name", self.get_ecr_repository_name()])
            password = subprocess.check_output(["aws", "ecr", "get-login-password", "--region", config_helper.get_region()],text=True).strip()
            logging.info("ECR get Password completed successfully.")
            registry = f"{common_infra.get_account_id()}.dkr.ecr.{config_helper.get_region()}.amazonaws.com"
            subprocess.run(["docker", "login", "--username", "AWS", "--password-stdin", registry], text=True, input=password, check=True)
            logging.info(f"ECR logged in to {registry} successfully.")
        except subprocess.CalledProcessError as e:
            logging.error("ECR/Docker login failed:", e)
            raise e

    def _ecr_clean_up(self):
        try:
            ecr_repository_name = f"{self.ecr_custom_repo_prefix}{self.get_connection_type_name()}"
            self.ecr_client.delete_repository(repositoryName=ecr_repository_name, force=True)
            logging.info(f"✅ ECR repository '{ecr_repository_name}' delete successfully.")
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'RepositoryNotFoundException':
                logging.info(f"ECR repository '{ecr_repository_name}' does not exist. Skipping deletion.")
            else:
                raise error

    def _build_connector_image(self):
        try:
            # build the image
            ecr_repository_name = self.get_ecr_repository_name()
            subprocess.run(["docker", "build","--provenance", "false", "--platform", "linux/amd64" ,"-t", ecr_repository_name,
                            f"{config_helper.get_repo_root()}/athena-{self.get_athena_federation_sdk_folder_name()}"],check=True)
            logging.info("✅ Docker build completed successfully.")
            # tag the image
            registry = f"{common_infra.get_account_id()}.dkr.ecr.{config_helper.get_region()}.amazonaws.com"
            subprocess.run(["docker", "tag", f"{ecr_repository_name}:latest", f"{registry}/{ecr_repository_name}:latest"], check=True)
            logging.info("✅ Docker tag completed successfully.")
            # push the image
            subprocess.run(["docker", "push", f"{registry}/{ecr_repository_name}:latest"], check=True)
            logging.info("✅ Docker push completed successfully.")
        except subprocess.CalledProcessError as e:
            logging.error("ECR/Docker login failed:", e)
            raise e

    def _update_lambda_image(self):
        function_name = f'{self.athena_recourse_creation_prefix}{config_helper.get_athena_catalog_name(self.get_connection_type_name())}'
        ecr_image_uri = f"{common_infra.get_account_id()}.dkr.ecr.{config_helper.get_region()}.amazonaws.com/{self.get_ecr_repository_name()}:latest"

        # Update the function code
        response = self.lambda_client.update_function_code(FunctionName=function_name,ImageUri=ecr_image_uri)

        start_time = time.time()
        while True:
            get_funct_response = self.lambda_client.get_function(FunctionName=function_name)
            lambda_status = get_funct_response['Configuration']['LastUpdateStatus']

            if lambda_status in ['Successful', 'Failed']:
                break

            if time.time() - start_time > self.timeout_seconds:
                logging.error("Timeout reached: update lambda function image did no complete within 5 minutes.")
                break

            logging.debug(f"⏳ Update lambda function imag still in progress. Name:{function_name}, status: {lambda_status}... waiting:{self.polling_interval} seconds")
            time.sleep(self.polling_interval)

        if lambda_status == 'Successful':
            logging.info('✅ Update lambda function completed')
        else:
            logging.error(f'❌ Update lambda function failed... name:{function_name}')

    def _enable_rds_public_ccess(self):
        rds_client.update_public_access(config_helper.get_rds_database_identifier_name(self.get_connection_type_name()), True)

    def _disable_rds_public_ccess(self):
        rds_client.update_public_access(config_helper.get_rds_database_identifier_name(self.get_connection_type_name()), False)

    def _create_athena_prod_connector(self, parameter) -> None:
        #cherck if it already has athena or glue catalog.
        catalog_name = config_helper.get_athena_catalog_name(self.get_connection_type_name())
        create_data_catalog_response = self.athena_client.create_data_catalog(Name=catalog_name, Type='FEDERATED', Parameters=parameter, Tags=[
            {
                'Key': 'Athena_Federation_Integ_Test',
                'Value': 'Athena_Federation_Integ_Test'
            },
        ])

        if create_data_catalog_response['DataCatalog']['Status'] == 'CREATE_FAILED':
            raise Exception(f"Catalog {catalog_name}, failed to create. Error: {create_data_catalog_response['DataCatalog']['Error']}")

        logging.info(f'Create athena connector response: {create_data_catalog_response}')

        self._get_athena_connector_status(catalog_name)

    def _get_athena_connector_status(self, catalog_name: str) -> bool:
        start_time = time.time()
        while True:
            get_catalog_response = self.athena_client.get_data_catalog(Name=catalog_name)
            catalog_status = get_catalog_response['DataCatalog']['Status']

            if catalog_status in self.athena_catalog_terminal_statuses:
                break

            if time.time() - start_time > self.timeout_seconds:
                logging.error("Timeout reached: Athena Catalog and connector creation did no complete within 5 minutes.")
                break

            logging.info(f"⏳ Create athena connector still in progress.Name:{catalog_name}, status: {catalog_status}... waiting:{self.polling_interval} seconds")
            time.sleep(self.polling_interval)


        if catalog_status == 'CREATE_COMPLETE':
            logging.info(f'✅ Create athena connector successful... name:{catalog_name} status: {catalog_status}')
        else:
            raise RuntimeError(f'❌ Create athena connector failed... name:{catalog_name}, response:{get_catalog_response}')

    def _get_rds_credential_from_secret_manager(self):
        client = aws_client_factory.get_aws_client("secretsmanager")
        secret_name = config_helper.get_config("common", "rds_secret_name")
        response = client.get_secret_value(SecretId=secret_name)
        secret = response['SecretString']
        return json.loads(secret)
