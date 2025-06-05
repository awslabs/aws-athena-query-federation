import sys
import boto3
import time
import logging
import json
from awsglue.utils import getResolvedOptions


# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Enable boto3 debug logging
boto3.set_stream_logger('botocore', logging.DEBUG)

redshift_data = boto3.client('redshift-data')

args = getResolvedOptions(sys.argv,
                          ['cluster_identifier',
                           'secret_arn',
                           'database_name'])

def execute_statement(sql):
    logger.info(f"Executing SQL: {sql}")
    try:
        response = redshift_data.execute_statement(
            ClusterIdentifier=args['cluster_identifier'],
            Database=args['database_name'],
            SecretArn=args['secret_arn'],
            Sql=sql
        )
        logger.debug(f"Execute statement response: {json.dumps(response, default=str)}")

        statement_id = response['Id']
        logger.info(f"Statement ID: {statement_id}")

        # Wait for the query to complete
        while True:
            status = redshift_data.describe_statement(Id=statement_id)
            logger.debug(f"Describe statement response: {json.dumps(status, default=str)}")

            if status['Status'] in ['FINISHED', 'FAILED', 'ABORTED']:
                logger.info(f"Statement {statement_id} finished with status: {status['Status']}")
                break
            logger.debug(f"Statement {statement_id} still running. Current status: {status['Status']}")
            time.sleep(0.5)

        if status['Status'] != 'FINISHED':
            raise Exception(f"Query failed: {json.dumps(status, default=str)}")

        return status
    except Exception as e:
        logger.error(f"Error executing statement: {str(e)}", exc_info=True)
        raise

# Your SQL statements
statements = [
    'CREATE SCHEMA "camelCaseTest"',
    'CREATE TABLE "camelCaseTest"."camelCase" (ID int)',
    'INSERT INTO "camelCaseTest"."camelCase" VALUES (5)',
    'CREATE TABLE "camelCaseTest"."UPPERCASE" (ID int)',
    'INSERT INTO "camelCaseTest"."UPPERCASE" VALUES (7)',
    'CREATE SCHEMA "UPPERCASETEST"',
    'CREATE TABLE "UPPERCASETEST"."camelCase" (ID int)',
    'INSERT INTO "UPPERCASETEST"."camelCase" VALUES (4)',
    'CREATE TABLE "UPPERCASETEST"."UPPERCASE" (ID int)',
    'INSERT INTO "UPPERCASETEST"."UPPERCASE" VALUES (6)',
    'CREATE MATERIALIZED VIEW "UPPERCASETEST"."camelCaseView" AS SELECT * FROM "camelCaseTest"."camelCase"'
]

for statement in statements:
    execute_statement(statement)

print("All statements executed successfully")
