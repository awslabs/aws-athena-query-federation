import sys
import boto3
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['db_arn', 'secret_arn'])

client = boto3.client('rds-data')

def execute_statement(sql):
    response = client.execute_statement(
        resourceArn=args['db_arn'],
        secretArn=args['secret_arn'],
        database='test',
        sql=sql
    )
    return response

execute_statement('CREATE SCHEMA "camelCaseTest"')
execute_statement('CREATE TABLE "camelCaseTest"."camelCase" (ID int)')
execute_statement('INSERT INTO "camelCaseTest"."camelCase" VALUES (5)')
execute_statement('CREATE TABLE "camelCaseTest"."UPPERCASE" (ID int)')
execute_statement('INSERT INTO "camelCaseTest"."UPPERCASE" VALUES (7)')

execute_statement('CREATE SCHEMA "UPPERCASETEST"')
execute_statement('CREATE TABLE "UPPERCASETEST"."camelCase" (ID int)')
execute_statement('INSERT INTO "UPPERCASETEST"."camelCase" VALUES (4)')
execute_statement('CREATE TABLE "UPPERCASETEST"."UPPERCASE" (ID int)')
execute_statement('INSERT INTO "UPPERCASETEST"."UPPERCASE" VALUES (6)')

execute_statement('CREATE MATERIALIZED VIEW "UPPERCASETEST"."camelCaseView" AS SELECT * FROM "camelCaseTest"."camelCase"')
