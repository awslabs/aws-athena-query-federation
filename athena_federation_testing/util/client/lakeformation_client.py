import logging


import time
from botocore.exceptions import ClientError
from pathlib import Path
from athena_federation_testing.util import config_helper
from athena_federation_testing.util.client import aws_client_factory

lakeformation = aws_client_factory.get_aws_client("lakeformation")
lf_ops_sleep_daly = 5

def register_glue_connection(ResourceARN, RoleARN):
    lakeformation.register_resource(
        ResourceArn=ResourceARN,
        RoleArn=RoleARN,
        WithFederation=True,
        UseServiceLinkedRole=False,
        HybridAccessEnabled=False,
    )
def grant_catalog_all_permission(catalog_name, role_arn):
    logging.info(f'Granting permission for {catalog_name} catalog...')
    lakeformation.grant_permissions(
        Principal={
            'DataLakePrincipalIdentifier': role_arn
        },
        Resource={
            'Catalog': {
                'Id': catalog_name
            }
        },
        Permissions=[
            'ALL'
        ]
    )
    time.sleep(lf_ops_sleep_daly)
    logging.info(f'Granted permission for {catalog_name} catalog...')

def grant_database_all_permission(catalog_name, db_name, role_arn):
    logging.info(f'Granting permission for {db_name} database...')
    lakeformation.grant_permissions(
        Principal={
            'DataLakePrincipalIdentifier': role_arn
        },
        Resource={
            'Database': {
                'CatalogId': catalog_name,
                'Name': db_name
            }
        },
        Permissions=[
            'ALL'
        ]
    )
    time.sleep(lf_ops_sleep_daly)
    logging.info(f'Granted permission for {db_name} database...')

def grant_tables_all_permission(catalog_name, db_name, role_arn):
    logging.info(f'Granting permission for {db_name} all tables...')
    lakeformation.grant_permissions(
        Principal={
            'DataLakePrincipalIdentifier': role_arn
        },
        Resource={
            'Table': {
                'CatalogId': catalog_name,
                'DatabaseName': db_name,
                'TableWildcard': {}
            }
        },
        Permissions=[
            'SELECT'
        ]
    )
    time.sleep(lf_ops_sleep_daly)
    logging.info(f'Granted permission for {db_name} all tables...')

def deregister_resource(glue_connection_arn):
    lakeformation.deregister_resource(ResourceArn=glue_connection_arn)
