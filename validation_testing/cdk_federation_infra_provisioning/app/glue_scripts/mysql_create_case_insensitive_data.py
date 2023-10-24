import sys
import pymysql
import pymysql.cursors
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv,
                          ['db_url',
                           'username',
                           'password'])

connection = pymysql.connect(host=args['db_url'], user=args['username'], password=args['password'])
cursor = connection.cursor()

cursor.execute('CREATE DATABASE camelCaseTest')
cursor.execute('USE camelCaseTest')
cursor.execute('CREATE TABLE camelCase (ID int)')
cursor.execute('INSERT INTO camelCase VALUES (5)')
cursor.execute('CREATE TABLE UPPERCASE (ID int)')
cursor.execute('INSERT INTO UPPERCASE VALUES (7)')

cursor.execute('CREATE DATABASE UPPERCASETEST')
cursor.execute('USE UPPERCASETEST')
cursor.execute('CREATE TABLE camelCase (ID int)')
cursor.execute('INSERT INTO camelCase VALUES (4)')
cursor.execute('CREATE TABLE UPPERCASE (ID int)')
cursor.execute('INSERT INTO UPPERCASE VALUES (6)')

