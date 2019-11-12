# Amazon Athena Lambda Jdbc Connector

This connector enables Amazon Athena to access your SQL database or RDS instance(s) using JDBC driver. 

Following databases are supported:

1. MySql
2. PostGreSql
3. Redshift

See `com.amazonaws.connectors.athena.jdbc.connectio.JdbcConnectionFactory.DatabaseEngine` for latest database types supported.

# Terms

* **Database Instance:** Any instance of a database deployed on premises, EC2 or using RDS.
* **Database type:** Could be one of mysql, postgres, redshift.
* **Handler:** A Lambda handler accessing your database instance(s). Could be metadata or a record handler.
* **Metadata Handler:** A Lambda handler that retrieves metadata from your database instance(s).
* **Record Handler:** A Lambda handler that retrieves data records from your database instance(s). 
* **Property/Parameter:** A database property used by handlers to extract database information for connection. These are set as Lambda environment variables.
* **Connection String:** Used to establish connection to a database instance.
* **Catalog:** Athena Catalog. This is not a Glue Catalog. Must be used to prefix `connection_string` property.

# Usage

## Parameters 

Jdbc Connector supports several configuration parameters using Lambda environment variables. Each parameter should be prefixed with a database instance name (any unique string) except spill s3 bucket and prefix.


## Connection String:

Connection string is used to connect to a database instance.

We support following format:

`${db_type}://<jdbc_connection_string>`

```
db_type                 One of following, mysql, postgres, redshift.
jdbc_connection_string  Connection string for a database type. For example, MySql connection String: jdbc:mysql://host1:33060/database
```


## Multiplexing handler parameters

Multiplexer provides a way to connect to multiple database instances of any type using a single Lambda function. Requests are routed depending on catalog name. Use following classes in Lambda for using multiplexer.

|DB Type|MetadataHandler|RecordHandler|
|---	|---	|---	|
|All supported database types|com.amazonaws.connectors.athena.jdbc.MultiplexingJdbcMetadataHandler|com.amazonaws.connectors.athena.jdbc.MultiplexingJdbcRecordHandler|

**Parameters:**

```
<catalog>_connection_string     Database instance connection string. One of two types specified above. Required.
default                         Default connection string. Required. This will be used when a catalog is not recognized.
```

Example properties for a Mux Lambda function that supports three database instances, mysql1, mysql2 and postgres1:

|Property|Value|
|---|---|
|mysql_catalog1_connection_string	|mysql://jdbc:mysql://mysql1.host:3306/default?${Test/RDS/PostGres1}|
|	|	|
|	|	|
|mysql_catalog2_connection_string	|mysql://jdbc:mysql://mysql2.host:3333/default?user=sample2&password=sample2|
|	|	|
|	|	|
|postgres_catalog3_connection_string	|postgres://jdbc:postgresql://postgres1.host:5432/default?${Test/RDS/PostGres1}|

JDBC Connector supports substitution of any string enclosed like ${SecretName} with username and password retrieved from AWS Secrets Manager. Example, `mysql://jdbc:mysql://mysql1.host:3306/default?...&${Test/RDS/PostGres1}&...` will be replaced to `mysql://jdbc:mysql://mysql1.host:3306/default?...&user=sample2&password=sample2&...` and secret name `Test/RDS/PostGres1` will be used to retrieve secrets.

## Database specific handler parameters

Database specific metadata and record handlers can also be used to connect to a database instance. These are currently capable of connecting to a single database instance.

|DB Type|MetadataHandler|RecordHandler|
|---|---|---|
|MySql|com.amazonaws.connectors.athena.jdbc.mysql.MySqlMetadataHandler|com.amazonaws.connectors.athena.jdbc.mysql.MySqlRecordHandler|
|PostGreSql|com.amazonaws.connectors.athena.jdbc.postgresql.PostGreSqlMetadataHandler|com.amazonaws.connectors.athena.jdbc.postgresql.PostGreSqlRecordHandler|
|Redshift|com.amazonaws.connectors.athena.jdbc.postgresql.PostGreSqlMetadataHandler|com.amazonaws.connectors.athena.jdbc.postgresql.PostGreSqlRecordHandler|

**Parameters:**

```
default                         Default connection string. Required. This will be used when a catalog is not recognized.
```

These handlers support one database instance. Must provide `default` parameter, everything else is ignored. 

**Example property for a single MySql instance supported by a Lambda function:**

| Property | Value |
| --- | --- |
| catalog1_connection_string | mysql://mysql1.host:3306/default?secret=Test/RDS/MySql1 |

## Common parameters

### Spill parameters:

All database instances accessed using Lambda spill to the same location

```
spill_bucket                Bucket name for spill. Required.
spill_prefix                Spill bucket key prefix. Required.
```

What is spilled?

# Data types support

|Jdbc|Arrow|
| ---|---|
|Boolean|Bit|
|Integer|Tiny|
|Short|Smallint|
|Integer|Int|
|Long|Bigint|
|float|Float4|
|Double|Float8|
|Date|DateDay|
|Timestamp|DateMilli|
|String|Varchar|
|Bytes|Varbinary|
|BigDecimal|Decimal|

See respective database documentation for conversion between JDBC and database types.

# Secrets

We support two ways to input database user name and password:

1. **AWS Secrets Manager:** The name of the secret in AWS Secrets Manager can be embedded in JDBC connection string, which is used to replace with `username` and `password` in secret value. Support is tightly integrated for AWS RDS database instances. When using AWS RDS, we highly recommend using AWS Secrets Manager, including credential rotation. If your database is not using AWS RDS, store credentials as JSON in the following format `{“username”: “${username}”, “password”: “${password}”}.`. 
2. **Basic Auth:** Username and password can be specified in the JDBC connection string.

# Partitions and Splits
### MySql
A partition is represented by a single partition column of type varchar. We leverage partitions defined on a MySql table, and this column contains partition names. For a table that does not have partition names, * is returned which single partition. A partition is equivalent to a split.

|Name|Type|Description
|---|---|---|
|partition_name|Varchar|Named partition in MySql. E.g. p0|

 
### PostGreSql & Redshift
A partition is represented by two partition columns of type varchar. We leverage partitions as child tables defined on a PostGres table, and these columns contain child schema and child table information. For a table that does not have partition names, * is returned which single partition. A partition is equivalent to a split.

|Name|Type|Description
|---|---|---|
|partition_schema|Varchar|Child table schema name|
|partition_name|Varchar|Child table name|

In case of Redshift partition_schema and partition_name will always be "*".

# JDBC Driver Versions
Current supported versions of JDBC Driver:

|Database Type|Version|
|---|---|
|MySql|8.0.17|
|PostGreSql|42.2.8|
|Redshift|1.2.34.1058|

# Limitations
* Write DDL operations are not supported. Athena can only do read operations currently. Athena assumes tables and relevant entities already exist in database instance. 
* In Mux setup, spill bucket and prefix is shared across all database instances.
* Any relevant Lambda Limits. See Lambda documentation.
