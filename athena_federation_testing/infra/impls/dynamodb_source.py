import logging

import boto3, json
import pandas as pd
import awswrangler as wr

from athena_federation_testing.util import config_helper
from athena_federation_testing.util.tpcds import tpcds_reader
from athena_federation_testing.infra.federated_source import Federated_Source
from athena_federation_testing.infra import common_infra
from botocore.exceptions import ClientError
from decimal import Decimal
from datetime import datetime, date
from botocore.config import Config
import botocore.config

from athena_federation_testing.util.client import glue_job_client,aws_client_factory


class DynamoDB_Federated_Source(Federated_Source, connection_type_name = "dynamodb"):
    ddb_config = Config(
        retries={
            'max_attempts': 1,
            'mode': 'standard'
        },
        max_pool_connections=50
    )
    wr.config.botocore_config = botocore.config.Config(
        retries={"max_attempts": 1},
        max_pool_connections=50)
    dynamodb = boto3.client('dynamodb', region_name=config_helper.get_region(), config = ddb_config)

    def get_glue_job_name(self):
        return "athena_integ_s3_to_ddb"

    def get_glue_script_name(self):
        return "s3_to_ddb.py"

    def get_glue_etl_connection_name(self):
        return None

    # For DynamoDB, glue ETL does not require glue connection
    def create_glue_job_connection(self):
        pass

    def construct_infra(self):
        self.construct_common_resource()
        logging.info(f'ℹ️ DynamoDB does not need construct_infra, skipping.....')

    def destroy_infra(self):
        for original_table_name in tpcds_reader.get_data_tables():
            table_name = self._get_dynamodb_table_name(original_table_name)
            try:
                response = self.dynamodb.delete_table(TableName=table_name)
                logging.info(f"Deletion initiated for table: {table_name}")
                waiter = self.dynamodb.get_waiter('table_not_exists')
                waiter.wait(
                    TableName=table_name,
                    WaiterConfig={'Delay': 10, 'MaxAttempts': 20}
                )

            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    logging.info(f"Table {table_name} does not exist.")
                    print(f'Table {table_name} does not exist.')
                else:
                    raise
            except Exception as e:
                logging.info(f"Error deleting table, name:{table_name}, exception: {e}")
                print(f'Error deleting table, name:{table_name}, exception: {e}')


    def create_database_local(self, database_name: str) -> None:
        logging.info(f'ℹ️ DynamoDB does not support create database, , skipping.....')

    def create_table_local(self, table_names: list) -> None:
        for original_table_name in table_names:
            table_name = self._get_dynamodb_table_name(original_table_name)
            if self._is_table_exists(table_name):
                logging.info(f'ℹ️ DynamoDB table exists:{table_name}, skipping. creation')
                continue

            self._create_dynamodb_table(table_name)
            # read data with orignal_table_name
            df = tpcds_reader.load_tpcds_data_parquet_from_resources(original_table_name)
            df = self._refresh_data_for_dynamo(df, original_table_name)
            logging.info(f"Data source:{self.get_connection_type_name()}, table:{table_name} Starting.")
            wr.dynamodb.put_df(df=df, table_name=table_name,
                               boto3_session=boto3.Session(region_name=config_helper.get_region()))
            logging.info(f"✅ Data source:{self.get_connection_type_name()}, table:{table_name} checked/created.")

    def create_database_s3(self, database_name: str) -> None:
        logging.info(f'ℹ️ DynamoDB does not support create database, , skipping.....')

    def create_table_s3(self, table_names: list) -> None:
        self.create_glue_job()

        for original_table_name in table_names:
            if self._is_table_exists(self._get_dynamodb_table_name(original_table_name)):
                logging.info(f'ℹ️ DynamoDB table exists:{self._get_dynamodb_table_name(original_table_name)}, skipping. creation')
                continue

            self._create_dynamodb_table(original_table_name)

            job_arguments = {
                '--additional-python-modules': 'pandas==1.4.0',
                '--BUCKET_NAME': config_helper.get_glue_job_bucket_name(common_infra.get_account_id()),
                '--SCALE': config_helper.get_tpcds_scale_factor_number(),
                '--TABLE_NAME': original_table_name
            }
            logging.info(f"Start loading table:{self._get_dynamodb_table_name(original_table_name)}")
            run_id = glue_job_client.start_job(self.get_glue_job_name(), job_arguments)
            final_status = glue_job_client.monitor_job_run(self.get_glue_job_name(), run_id)

    def create_connector(self) -> None:
        self.construct_common_resource()
        parameters = {
            "connection-type": self.get_connection_type_name().upper(), #glue connection must be in upper
            "connection-properties": json.dumps(
                {
                    "spill_bucket": config_helper.get_s3_spill_bucket_name(common_infra.get_account_id()),
                    "disable_spill_encryption": "false",
                    "description": "Athena Integrration test DYNAMODB connector",
                }
            )
        }
        self._create_athena_prod_connector(parameters)

    # Create a dynamodb table first before inserting table.
    def _create_dynamodb_table(self, table_name: str) -> None:
        ddb_table_name = self._get_dynamodb_table_name(table_name)
        logging.info(f"creating.. ddb table: {ddb_table_name}")

        attribute_def = [{'AttributeName': tpcds_reader.get_tpcds_data_primary_key(table_name),'AttributeType': 'N' if tpcds_reader.get_tpcds_data_primary_key_type(table_name) == "int" else 'S'}]
        key_schema = [{'AttributeName': tpcds_reader.get_tpcds_data_primary_key(table_name),'KeyType': 'HASH'}]
        write_capacity = 2000
        if table_name.endswith('catalog_sales'):
            attribute_def.append({'AttributeName': tpcds_reader.get_tpcds_data_sort_key(table_name),'AttributeType': 'N' if tpcds_reader.get_tpcds_data_sort_key_type(table_name) == "int" else 'S'})
            key_schema.append({'AttributeName': tpcds_reader.get_tpcds_data_sort_key(table_name),'KeyType': 'RANGE'})
            write_capacity = 10000

        response = self.dynamodb.create_table(
            TableName=ddb_table_name,
            AttributeDefinitions=attribute_def,
            KeySchema=key_schema,
            BillingMode='PROVISIONED',
            ProvisionedThroughput={
                'ReadCapacityUnits': 800,
                'WriteCapacityUnits': write_capacity  # <-- Set your custom write capacity here
            },
            TableClass='STANDARD',
        )

        waiter = self.dynamodb.get_waiter('table_exists')
        waiter.wait(
            TableName=ddb_table_name,
            WaiterConfig={'Delay': 10, 'MaxAttempts': 20}
        )

    def _is_table_exists(self, table_name):
        try:
            self.dynamodb.describe_table(TableName=table_name)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return False
            else:
                raise

    # This is for local dataframe
    def _refresh_data_for_dynamo(self, df, table_name):
        # fill secondary key incase null
        sort_key = tpcds_reader.get_tpcds_data_sort_key(table_name)
        sort_key_type = tpcds_reader.get_tpcds_data_sort_key_type(table_name)

        if sort_key_type == "int":
            df[sort_key] = df[sort_key].fillna(0)
        else:
            df[sort_key] = df[sort_key].fillna("")


        for column in df.columns:
            # Convert float columns, ddb not supported
            if df[column].dtype in ['float64', 'float32']:
                df[column] = df[column].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else None)

            # Convert datetime, date columns as ddb not supported
            elif df[column].dtype == 'datetime64[ns]':
                df[column] = df[column].dt.strftime('%Y-%m-%d %H:%M:%S')

            elif df[column].dtype == 'object':
                df[column] = df[column].apply(
                    lambda x: x.strftime('%Y-%m-%d') if isinstance(x, (datetime, date)) else x
                )
        return df

    # Due to dynamodb has no `database` concept, we need to append database name to table anem
    def _get_dynamodb_table_name(self, table_name):
        return f'{config_helper.get_tpcds_scale_factor()}_{table_name}'