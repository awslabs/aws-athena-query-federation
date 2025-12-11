from abc import ABC
from typing import Any

import time, botocore.exceptions
import importlib.resources as pkg_resources
from athena_federation_testing.util import config_helper
from athena_federation_testing.util.tpcds import tpcds_reader
from athena_federation_testing.util.client import lakeformation_client, aws_client_factory
from athena_federation_testing import resources
import pandas as pd
from athena_federation_testing.infra import common_infra

import logging

class Federated_Testing(ABC):
    registry = {}
    athena_client = aws_client_factory.get_aws_client("athena")
    sts_client = aws_client_factory.get_aws_client("sts")
    glue_client = aws_client_factory.get_aws_client("glue")
    lakeformation_client = aws_client_factory.get_aws_client("lakeformation")
    s3_client = aws_client_factory.get_aws_client("s3")

    timeout_seconds = 1800 # 30 min default query timeout
    polling_interval = 10
    def __init_subclass__(cls, connection_type_name):
        logging.info(f"Creating federated test, type: {connection_type_name}")
        cls.connection_type_name = connection_type_name
        Federated_Testing.registry[connection_type_name] = cls

    @classmethod
    def create_federated_source(cls, connection_type_name, *args, **kwargs):
        if connection_type_name not in cls.registry:
            # Majority of test are the same unless special condition like Dynamodb(no schema), ES
            # Hence we are just going to create a subclass at runtime and use the connection_type_name to perform query(for catalog name etc...)
            new_subclass = type(f"Federated_{connection_type_name}", (Federated_Testing,), {},
                                connection_type_name=connection_type_name)
            return new_subclass(*args, **kwargs)
        return cls.registry[connection_type_name](*args, **kwargs)

    def execute_athena_metadata_test(self):
        catalog_name = config_helper.get_athena_catalog_name(self.get_connection_type_name())

        rows = []
        rows.append({"Test": "Athena:GetDataCatalog",
                     "State": "SUCCEEDED" if self._is_athena_catalog_exists(catalog_name) else "FAILED",
                     "Description": "Get Athena Datacatalog, check if status = completed"
                     })
        db_list = self.athena_client.list_databases(CatalogName=catalog_name)
        rows.append({"Test": "Athena:ListDatabase",
                     "State": "SUCCEEDED" if len(db_list['DatabaseList']) > 0 else "FAILED",
                     "Description": "List database created (no pagination)"
                    })

        table_list=self.athena_client.list_table_metadata(CatalogName=catalog_name, DatabaseName=self.get_database_name())
        actual_name_set = {table["Name"] for table in table_list["TableMetadataList"]}
        contains_all_tables= set(self.get_tables_name()).issubset(actual_name_set)
        rows.append({"Test": "Athena:ListTables",
                     "State": "SUCCEEDED" if contains_all_tables else "FAILED",
                     "Description": "List tables verify all table exists"})

        table_name = next(iter(actual_name_set), None)
        response = self.athena_client.get_table_metadata(
            CatalogName=catalog_name,
            DatabaseName=self.get_database_name(),
            TableName=table_name
        )
        rows.append({"Test": "Athena:GetTableMetadata",
                     "State": "SUCCEEDED" if response["TableMetadata"]["Name"] == table_name else "FAILED",
                     "Description": "GetTable verify Get-Table_metadata works"})
        logging.info(f"execute_athena_metadata_test completed for {catalog_name}")

        return pd.DataFrame(rows)

    def grant_permission_on_glue_catalog(self):
        lakeformation_client.grant_catalog_all_permission(config_helper.get_glue_catalog_name(self.get_connection_type_name()), self.get_account_arn())
        lakeformation_client.grant_database_all_permission(config_helper.get_glue_catalog_name(self.get_connection_type_name()), self.get_database_name(), self.get_account_arn())
        lakeformation_client.grant_tables_all_permission(
            config_helper.get_glue_catalog_name(self.get_connection_type_name()), self.get_database_name(),
            self.get_account_arn())

    def execute_glue_federation_metadata_test(self) -> None:
        glue_catalog_name = config_helper.get_glue_catalog_name(self.get_connection_type_name())
        rows = []
        rows.append({"Test": "Glue:GetDataCatalog",
                     "State": "SUCCEEDED" if self._is_glue_catalog_exists(config_helper.get_glue_catalog_name(self.get_connection_type_name())) else "FAILED",
                     "Description": "Get Glue Datacatalog"
                     })

        glue_database_response = self.glue_client.get_databases(CatalogId = glue_catalog_name)

        rows.append({"Test": "Glue:GetDatabases",
                     "State": "SUCCEEDED" if len(glue_database_response['DatabaseList']) > 0 else "FAILED",
                     "Description": "List database created (no pagination)"
                     })

        glue_tables_response = self.glue_client.get_tables(CatalogId = glue_catalog_name, DatabaseName = self.get_database_name())
        actual_name_set = {table["Name"] for table in glue_tables_response["TableList"]}
        contains_all_tables= set(self.get_tables_name()).issubset(actual_name_set)

        rows.append({"Test": "Glue:getTables",
                     "State": "SUCCEEDED" if contains_all_tables else "FAILED",
                     "Description": "List tables verify all table exists"})

        logging.info(f"execute_glue_federation_metadata_test completed for {glue_catalog_name}")
        return pd.DataFrame(rows)

    def execute_athena_simple_select_test(self):
        logging.info(f"execute_athena_simple_select_test started for {self.get_connection_type_name()}")
        self._is_athena_catalog_exists(config_helper.get_athena_catalog_name(self.get_connection_type_name()))
        queries = self._get_simple_select_query(use_athena_catalog=True)
        return self._execute_simple_select_test(queries, True)

    def execute_glue_federation_simple_select_test(self):
        self._is_glue_catalog_exists(config_helper.get_glue_catalog_name(self.get_connection_type_name()))
        queries = self._get_simple_select_query(use_athena_catalog=False)
        return self._execute_simple_select_test(queries, False)

    def _execute_simple_select_test(self, queries : list[str], use_athena_catalog):
        rows = []
        for query in queries:
            query_response = self._execute_athena_query(query)
            if query_response['QueryExecution']['Status']['State'] == 'SUCCEEDED':
                rows.append({"QueryExecutionId": query_response["QueryExecution"]["QueryExecutionId"],
                             "Description": "Execute simple select test",
                             "State": query_response["QueryExecution"]["Status"]["State"],
                             "Query": query_response["QueryExecution"]["Query"],
                             "IsAthenaCatalog": use_athena_catalog,
                             "EngineExecutionTimeInMillis": query_response["QueryExecution"]["Statistics"][
                                 "EngineExecutionTimeInMillis"],
                             "QueryPlanningTimeInMillis": query_response.get("QueryExecution", {}).get("Statistics",
                                                                                                       {}).get(
                                 "QueryPlanningTimeInMillis", ""),  # not sure why this somtimes empty
                             "DataScannedInBytes": query_response["QueryExecution"]["Statistics"]["DataScannedInBytes"],
                             "ResultOutputBytes": self._get_result_output_bytes(query_response)
                             })
            else:
                rows.append({"QueryExecutionId": query_response["QueryExecution"]["QueryExecutionId"],
                             "Description":"Execute simple select test",
                             "State": query_response["QueryExecution"]["Status"]["State"],
                             "Query": query_response["QueryExecution"]["Query"],
                             "IsAthenaCatalog": use_athena_catalog,
                             "ErrorMessage": query_response["QueryExecution"]["Status"]["AthenaError"]["ErrorMessage"]
                             })
        return pd.DataFrame(rows)

    def execute_athena_predicate_select_test(self):
        logging.info(f"execute_athena_predicate_select_test started for {self.get_connection_type_name()}")
        self._is_athena_catalog_exists(config_helper.get_athena_catalog_name(self.get_connection_type_name()))
        return self._execute_predicate_test(use_athena_catalog=True)

    def execute_glue_federation_predicate_select_test(self):
        self._is_glue_catalog_exists(config_helper.get_glue_catalog_name(self.get_connection_type_name()))
        return self._execute_predicate_test(use_athena_catalog=False)

    def _execute_predicate_test(self, use_athena_catalog):
        df = pd.DataFrame()

        # Baseline with simple select
        base_tuple = self._get_predicate_base_query(use_athena_catalog=use_athena_catalog)
        df = pd.concat([df, self._execute_predicate_query(base_tuple, use_athena_catalog)])

        # select with primary key where clause
        simple_predicate = self._get_predicate_select_query(use_athena_catalog=use_athena_catalog)
        df = pd.concat([df, self._execute_predicate_query(simple_predicate, use_athena_catalog)], ignore_index=True)

        limit_queries = self._get_limit_pushdown_select_query(use_athena_catalog=use_athena_catalog)
        df = pd.concat([df, self._execute_predicate_query(limit_queries, use_athena_catalog)], ignore_index=True)

        complex_predicate = self._get_complex_pushdown_select_query(use_athena_catalog=use_athena_catalog)
        df = pd.concat([df, self._execute_predicate_query(complex_predicate, use_athena_catalog)], ignore_index=True)

        select_count = self._get_select_count_query(use_athena_catalog=use_athena_catalog)
        df = pd.concat([df, self._execute_predicate_query(select_count, use_athena_catalog)], ignore_index=True)

        column_project = self._get_column_project_query(use_athena_catalog=use_athena_catalog)
        df = pd.concat([df, self._execute_predicate_query(column_project, use_athena_catalog)], ignore_index=True)

        # #
        dynamic_filter_query = self._get_dynamic_filter_query(use_athena_catalog=use_athena_catalog)
        df = pd.concat([df, self._execute_predicate_query(dynamic_filter_query, use_athena_catalog)], ignore_index=True)

        return df

    def _execute_predicate_query(self, queries_input: list[dict], use_athena_catalog):
        rows = []
        for i, query_input in enumerate(queries_input):
            try:
                query_response = self._execute_athena_query(query_input["Query"])
                if query_response['QueryExecution']['Status']['State'] == 'SUCCEEDED':
                    rows.append({"QueryExecutionId":query_response["QueryExecution"]["QueryExecutionId"],
                               "Description": query_input["Description"],
                               "State":query_response["QueryExecution"]["Status"]["State"],
                               "Query":query_response["QueryExecution"]["Query"],
                               "TableName":query_input["TableName"],
                               "IsAthenaCatalog": use_athena_catalog,
                               "EngineExecutionTimeInMillis":query_response["QueryExecution"]["Statistics"]["EngineExecutionTimeInMillis"],
                               "QueryPlanningTimeInMillis": query_response.get("QueryExecution", {}).get("Statistics", {}).get("QueryPlanningTimeInMillis", ""), #not sure why this somtimes empty
                               "DataScannedInBytes":query_response["QueryExecution"]["Statistics"]["DataScannedInBytes"],
                               "ResultOutputBytes": self._get_result_output_bytes(query_response)
                               })
                else:
                    rows.append({"QueryExecutionId": query_response["QueryExecution"]["QueryExecutionId"],
                                 "Description": query_input["Description"],
                                 "State": query_response["QueryExecution"]["Status"]["State"],
                                 "Query": query_response["QueryExecution"]["Query"],
                                 "TableName": query_input["TableName"],
                                 "IsAthenaCatalog": use_athena_catalog,
                                 "ErrorMessage": query_response["QueryExecution"]["Status"]["AthenaError"]["ErrorMessage"]
                                 })
            except Exception as e:
                logging.error(f"Error executing query: {query_input}")
                rows.append({"QueryExecutionId": "Failed at API level: N/A",
                             "Description": query_input["Description"],
                             "State": "FAILED",
                             "Query": query_input["Query"],
                             "TableName": query_input["TableName"],
                             "IsAthenaCatalog": use_athena_catalog,
                             "ErrorMessage": f'{e.response["AthenaErrorCode"]}, {e.response["Error"]["Message"]}'
                             })

        return pd.DataFrame(rows)

    def get_connection_type_name(self) -> str:
        return type(self).connection_type_name

    def get_database_name(self) -> str:
        return config_helper.get_tpcds_scale_factor()

    def get_tables_name(self) -> list[str]:
        if not self.get_table_prefix():
            return tpcds_reader.TEST_SELECT_TABLES

        return [f"{self.get_table_prefix()}{table}" for table in tpcds_reader.TEST_SELECT_TABLES]

    def get_table_prefix(self) -> str:
        return ""

    def get_account_arn(self):
        response = self.sts_client.get_caller_identity()
        arn = response['Arn']
        if ":assumed-role/" in arn:
            parts = arn.split(":assumed-role/")
            account_id = response["Account"]
            role_name = parts[1].split("/")[0]
            iam_role_arn = f"arn:aws:iam::{account_id}:role/{role_name}"
        else:
            iam_role_arn = arn  # If it's already a normal role/user ARN
        return iam_role_arn

    def _get_simple_select_query(self, use_athena_catalog:bool) -> list[str]:
        catalog_name = config_helper.get_athena_catalog_name(
            self.get_connection_type_name()) if use_athena_catalog is True else config_helper.get_glue_catalog_name(self.get_connection_type_name())
        return [
            query
            for table_name in self.get_tables_name()
            for query in [
                f'SELECT * FROM {catalog_name}.{self.get_database_name()}.{table_name} LIMIT 1',
                f'SELECT * FROM {catalog_name}.{self.get_database_name()}.{table_name}'
            ]
        ]

    def _get_predicate_base_query(self, use_athena_catalog:bool) -> list[dict]:
        catalog_name = config_helper.get_athena_catalog_name(
            self.get_connection_type_name()) if use_athena_catalog is True else config_helper.get_glue_catalog_name(
            self.get_connection_type_name())
        queries = []
        for table_name in self.get_tables_name():
            queries.append({
                "Query":f'SELECT * FROM {catalog_name}.{self.get_database_name()}.{table_name}',
                "TableName":table_name,
                "Description":"Base Query",
            })
        return queries

    def _get_predicate_select_query(self, use_athena_catalog:bool) -> list[dict]:
        catalog_name = config_helper.get_athena_catalog_name(
            self.get_connection_type_name()) if use_athena_catalog is True else config_helper.get_glue_catalog_name(
            self.get_connection_type_name())
        queries=[]
        for table_name in self.get_tables_name():
            queries.append({
                "Query": f'SELECT * FROM {catalog_name}.{self.get_database_name()}.{table_name} where {tpcds_reader.get_tpcds_data_primary_key_predicate(table_name)}',
                "TableName": table_name,
                "Description": "Primary Key predicate",
            })
        return queries

    def _get_limit_pushdown_select_query(self, use_athena_catalog:bool) -> list[dict]:
        catalog_name = config_helper.get_athena_catalog_name(
            self.get_connection_type_name()) if use_athena_catalog is True else config_helper.get_glue_catalog_name(
            self.get_connection_type_name())
        queries=[]
        for table_name in self.get_tables_name():
            queries.append({
                "Query": f'SELECT * FROM {catalog_name}.{self.get_database_name()}.{table_name} LIMIT 1',
                "TableName": table_name,
                "Description": "limit pushdown",
            })
        return queries

    def _get_complex_pushdown_select_query(self, use_athena_catalog:bool) -> list[dict]:
        catalog_name = config_helper.get_athena_catalog_name(
            self.get_connection_type_name()) if use_athena_catalog is True else config_helper.get_glue_catalog_name(
            self.get_connection_type_name())
        queries=[]
        for table_name in self.get_tables_name():
            queries.append({
                "Query": f'SELECT * FROM {catalog_name}.{self.get_database_name()}.{table_name} where {tpcds_reader.get_tpcds_data_complex_predicate(table_name)}',
                "TableName": table_name,
                "Description": "Complex Predicate",
            })
        return queries

    def _get_select_count_query(self, use_athena_catalog:bool) -> list[dict]:
        catalog_name = config_helper.get_athena_catalog_name(
            self.get_connection_type_name()) if use_athena_catalog is True else config_helper.get_glue_catalog_name(
            self.get_connection_type_name())
        queries=[]
        for table_name in self.get_tables_name():
            queries.append({
                "Query": f'SELECT count(*) FROM {catalog_name}.{self.get_database_name()}.{table_name} ',
                "TableName": table_name,
                "Description": "select count",
            })
        return queries

    def _get_dynamic_filter_query(self, use_athena_catalog:bool) -> list[dict]:
        catalog_name = config_helper.get_athena_catalog_name(
            self.get_connection_type_name()) if use_athena_catalog is True else config_helper.get_glue_catalog_name(
            self.get_connection_type_name())
        resource_path = pkg_resources.files(resources).joinpath(f'tpcds_data/query/')

        queries = []
        for sql_file in resource_path.iterdir():
            with pkg_resources.as_file(sql_file) as path:
                with open(path, 'r', encoding='utf-8') as f:
                    sql_query = f.read()
                    sql_query = sql_query.replace('{catalog_place_holder}', catalog_name)
                    sql_query = sql_query.replace('{database_place_holder}', self.get_database_name())
                    sql_query = sql_query.replace('{table_prefix}', self.get_table_prefix())
                    queries.append({
                        "Query": sql_query,
                        "TableName": path,
                        "Description": f"tpcds query: {sql_file.name}",
                    })

        return queries

    def _get_column_project_query(self, use_athena_catalog:bool) -> list[dict]:
        catalog_name = config_helper.get_athena_catalog_name(
            self.get_connection_type_name()) if use_athena_catalog is True else config_helper.get_glue_catalog_name(
            self.get_connection_type_name())
        queries = []

        queries.append({
            "Query":f'SELECT c_customer_sk FROM {catalog_name}.{self.get_database_name()}.{self.get_table_prefix()}customer',
            "TableName":f"{self.get_table_prefix()}customer",
            "Description":"Column projection",
        })
        return queries

    def _is_athena_catalog_exists(self, catalog_name:str)-> bool:
        get_catalog_response = self.athena_client.get_data_catalog(Name=catalog_name)
        
        if get_catalog_response['DataCatalog']['Type'] == 'LAMBDA':
            return True
        
        catalog_status = get_catalog_response['DataCatalog']['Status']
        if catalog_status != 'CREATE_COMPLETE':
            raise RuntimeError(f"Catalog {catalog_name} does not exist in Athena Catalog.")

        return True

    def _is_glue_catalog_exists(self, glue_catalog_name:str)-> bool:
        try:
            glue_catalog = self.glue_client.get_catalog(CatalogId=glue_catalog_name)
            if 'FederatedCatalog' in glue_catalog['Catalog'] and 'ConnectionName' in glue_catalog['Catalog'][
                'FederatedCatalog']:
                logging.info(f'ℹ️Catalog {glue_catalog_name} found in glue, skipping migration.')
            else:
                raise ValueError(f"Glue Catalog:{glue_catalog_name} exists and it is not FederatedCatalog.")
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'EntityNotFoundException':
                logging.info(
                    f'ℹ️Catalog {glue_catalog_name} not found in glue, migrating from Athena Catalog to Glue...')
                raise RuntimeError(f"Catalog {glue_catalog_name} does not exist in Glue Data Catalog.")

        return True

    def _execute_athena_query(self, query_string:str):
        result_path = f"s3://{config_helper.get_s3_result_bucket_name(common_infra.get_account_id())}/result"

        start_query_execution_response = self.athena_client.start_query_execution(
            QueryString=query_string,
            ResultConfiguration={
                'OutputLocation': result_path
            }
        )

        return self._wait_for_athena_query(start_query_execution_response['QueryExecutionId'])

    def _wait_for_athena_query(self, query_execution_id):
        start_time = time.time()
        while True:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = response['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break

            if time.time() - start_time > self.timeout_seconds:
                logging.error("Timeout reached: update lambda function image did no complete within 5 minutes.")
                break

            time.sleep(self.polling_interval)
        return response
        #TODO consider adding failure here document

    def _get_result_output_bytes(self, query_response):
        try:
            output_location = query_response.get('QueryExecution', {}).get('ResultConfiguration', {}).get('OutputLocation', '')
            if not output_location:
                return 0
            
            bucket = output_location.split('/')[2]
            key = '/'.join(output_location.split('/')[3:])
            
            response = self.s3_client.head_object(Bucket=bucket, Key=key)
            return response.get('ContentLength', 0)
        except Exception as e:
            logging.warning(f"Failed to get result output bytes: {e}")
            return 0



    #todo add capabilities here then invoke corresponding test.


class DynamoDB_Federated_Testing(Federated_Testing, connection_type_name = "dynamodb"):
    def get_database_name(self) -> str:
        return 'default'
    def get_table_prefix(self) -> str:
        return f"tpcds{config_helper.get_tpcds_scale_factor_number()}_"
    def _get_dynamic_filter_query(self, use_athena_catalog:bool) -> list[dict]:
        return []

class ElasticSearch_Testing(Federated_Testing, connection_type_name = "elasticsearch"):
    def get_database_name(self) -> str:
        return 'tpcds'
    def get_table_prefix(self) -> str:
        return f"tpcds{config_helper.get_tpcds_scale_factor_number()}_"
    def _get_dynamic_filter_query(self, use_athena_catalog:bool) -> list[dict]:
        return []

class DocumentDB_Testing(Federated_Testing, connection_type_name = "documentdb"):
    def _get_dynamic_filter_query(self, use_athena_catalog:bool) -> list[dict]:
        return []
