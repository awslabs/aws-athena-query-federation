import sys
from awsglue.utils import getResolvedOptions
import pg

args = getResolvedOptions(sys.argv,
                          ['db_url',
                           'username',
                           'password'])

connection = pg.DB( host=args['db_url'], user=args['username'], passwd=args['password'], dbname='test')

connection.query('CREATE SCHEMA "camelCaseTest"')
connection.query('CREATE TABLE "camelCaseTest"."camelCase" (ID int)')
connection.query('INSERT INTO "camelCaseTest"."camelCase" VALUES (5)')
connection.query('CREATE TABLE "camelCaseTest"."UPPERCASE" (ID int)')
connection.query('INSERT INTO "camelCaseTest"."UPPERCASE" VALUES (7)')

connection.query('CREATE SCHEMA "UPPERCASETEST"')
connection.query('CREATE TABLE "UPPERCASETEST"."camelCase" (ID int)')
connection.query('INSERT INTO "UPPERCASETEST"."camelCase" VALUES (4)')
connection.query('CREATE TABLE "UPPERCASETEST"."UPPERCASE" (ID int)')
connection.query('INSERT INTO "UPPERCASETEST"."UPPERCASE" VALUES (6)')

connection.query('CREATE MATERIALIZED VIEW "UPPERCASETEST"."camelCaseView" AS SELECT * FROM "camelCaseTest"."camelCase"')