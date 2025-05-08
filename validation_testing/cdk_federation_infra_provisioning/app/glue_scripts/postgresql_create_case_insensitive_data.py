import sys
from awsglue.utils import getResolvedOptions
import psycopg2

args = getResolvedOptions(sys.argv,
                          ['db_url',
                           'username',
                           'password'])

connection = psycopg2.connect(host=args['db_url'], user=args['username'], password=args['password'], dbname='test')
cursor = connection.cursor()

cursor.execute('CREATE SCHEMA "camelCaseTest"')
cursor.execute('CREATE TABLE "camelCaseTest"."camelCase" (ID int)')
cursor.execute('INSERT INTO "camelCaseTest"."camelCase" VALUES (5)')
cursor.execute('CREATE TABLE "camelCaseTest"."UPPERCASE" (ID int)')
cursor.execute('INSERT INTO "camelCaseTest"."UPPERCASE" VALUES (7)')

cursor.execute('CREATE SCHEMA "UPPERCASETEST"')
cursor.execute('CREATE TABLE "UPPERCASETEST"."camelCase" (ID int)')
cursor.execute('INSERT INTO "UPPERCASETEST"."camelCase" VALUES (4)')
cursor.execute('CREATE TABLE "UPPERCASETEST"."UPPERCASE" (ID int)')
cursor.execute('INSERT INTO "UPPERCASETEST"."UPPERCASE" VALUES (6)')

cursor.execute('CREATE MATERIALIZED VIEW "UPPERCASETEST"."camelCaseView" AS SELECT * FROM "camelCaseTest"."camelCase"')

connection.commit()
cursor.close()
connection.close()
