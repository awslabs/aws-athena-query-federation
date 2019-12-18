# Amazon Athena Lambda Jdbc Connector

This connector enables Amazon Athena to access your SQL database or RDS instance(s) using JDBC driver. 

**To enable this Preview feature you need to create an Athena workgroup named AmazonAthenaPreviewFunctionality and run any queries attempting to federate to this connector, use a UDF, or SageMaker inference from that workgroup.**

Following databases are supported:

1. MySql
2. PostGreSql
3. Redshift

# Terms

* **Database Instance:** Any instance of a database deployed on premises, EC2 or using RDS.
* **Database type:** Could be one of mysql, postgres, redshift.
* **Handler:** A Lambda handler accessing your database instance(s). Could be metadata or a record handler.
* **Metadata Handler:** A Lambda handler that retrieves metadata from your database instance(s).
* **Record Handler:** A Lambda handler that retrieves data records from your database instance(s).
* **Composite Handler:** A Lambda handler that retrieves metadata and data records from your database instance(s). This is recommended to be set as lambda function handler. 
* **Property/Parameter:** A database property used by handlers to extract database information for connection. These are set as Lambda environment variables.
* **Connection String:** Used to establish connection to a database instance.
* **Catalog:** Athena Catalog. This is not a Glue Catalog. Must be used to prefix `connection_string` property.

# Usage

## Parameters 

Jdbc Connector supports several configuration parameters using Lambda environment variables.

### Connection String:

Connection string is used to connect to a database instance. Following format is supported:

`${db_type}://${jdbc_connection_string}`

```
db_type                     One of following, mysql, postgres, redshift.
jdbc_connection_string      Connection string for a database type. For example, MySql connection String: jdbc:mysql://host1:33060/database
```

### Multiplexing handler parameters

Multiplexer provides a way to connect to multiple database instances of any type using a single Lambda function. Requests are routed depending on catalog name. Use following classes in Lambda for using multiplexer.

|Handler|Class|
|---	|---	|
|Composite Handler|com.amazonaws.connectors.athena.jdbc.MultiplexingJdbcCompositeHandler|
|Metadata Handler|com.amazonaws.connectors.athena.jdbc.MultiplexingJdbcMetadataHandler|
|Record Handler|com.amazonaws.connectors.athena.jdbc.MultiplexingJdbcRecordHandler|


**Parameters:**

```
${catalog}_connection_string    Database instance connection string. One of two types specified above. Required.
default                         Default connection string. Required. This will be used when catalog is `lambda:${AWS_LAMBDA_FUNCTION_NAME}`.
```

Example properties for a Mux Lambda function that supports four database instances, redshift(default), mysql1, mysql2 and postgres1:

|Property|Value|
|---|---|
|default|redshift://jdbc:redshift://redshift1.host:5439/dev?user=sample2&password=sample2|
|	|	|
|mysql_catalog1_connection_string|mysql://jdbc:mysql://mysql1.host:3306/default?${Test/RDS/PostGres1}|
|	|	|
|mysql_catalog2_connection_string|mysql://jdbc:mysql://mysql2.host:3333/default?user=sample2&password=sample2|
|	|	|
|postgres_catalog3_connection_string|postgres://jdbc:postgresql://postgres1.host:5432/default?${Test/RDS/PostGres1}|

JDBC Connector supports substitution of any string enclosed like *${SecretName}* with *username* and *password* retrieved from AWS Secrets Manager. Example: 

```
mysql://jdbc:mysql://mysql1.host:3306/default?...&${Test/RDS/PostGres1}&...
```

will be modified to:

```
mysql://jdbc:mysql://mysql1.host:3306/default?...&user=sample2&password=sample2&...
```

Secret Name `Test/RDS/PostGres1` will be used to retrieve secrets.

Currently supported databases recognize `user` and `password` JDBC properties.

### Database specific handler parameters

Database specific metadata and record handlers can also be used to connect to a database instance. These are currently capable of connecting to a single database instance.

|DB Type|Handler|Class|
|---|---|---|
| |Composite Handler|com.amazonaws.connectors.athena.jdbc.mysql.MySqlCompositeHandler|
|MySql|Metadata Handler|com.amazonaws.connectors.athena.jdbc.mysql.MySqlMetadataHandler|
| |Record Handler|com.amazonaws.connectors.athena.jdbc.mysql.MySqlRecordHandler|
|	|	|
| |Composite Handler|com.amazonaws.connectors.athena.jdbc.postgresql.PostGreSqlCompositeHandler|
|PostGreSql|Metadata Handler|com.amazonaws.connectors.athena.jdbc.postgresql.PostGreSqlMetadataHandler|
| |Record Handler|com.amazonaws.connectors.athena.jdbc.postgresql.PostGreSqlRecordHandler|
|	|	|
| |Composite Handler|com.amazonaws.connectors.athena.jdbc.postgresql.PostGreSqlRecordHandler|
|Redshift|Metadata Handler|com.amazonaws.connectors.athena.jdbc.postgresql.PostGreSqlMetadataHandler|
| |Record Handler|com.amazonaws.connectors.athena.jdbc.postgresql.PostGreSqlRecordHandler|

**Parameters:**

```
default         Default connection string. Required. This will be used when a catalog is not recognized.
```

These handlers support one database instance and must provide `default` connection string parameter. All other connection strings are ignored. 

**Example property for a single MySql instance supported by a Lambda function:**

|Property|Value|
|---|---|
|default|mysql://mysql1.host:3306/default?secret=Test/RDS/MySql1|

### Spill parameters:

Lambda SDK may spill data to S3. All database instances accessed using a single Lambda spill to the same location.

```
spill_bucket    Spill bucket name. Required.
spill_prefix    Spill bucket key prefix. Required.
```

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
|String|Varchar|
|Bytes|Varbinary|
|BigDecimal|Decimal|

See respective database documentation for conversion between JDBC and database types.

# Secrets

We support two ways to input database user name and password:

1. **AWS Secrets Manager:** The name of the secret in AWS Secrets Manager can be embedded in JDBC connection string, which is used to replace with `username` and `password` values from Secret. Support is tightly integrated for AWS RDS database instances. When using AWS RDS, we highly recommend using AWS Secrets Manager, including credential rotation. If your database is not using AWS RDS, store credentials as JSON in the following format `{“username”: “${username}”, “password”: “${password}”}.`.
2. **Connection String:** Username and password can be specified as properties in the JDBC connection string.

# Partitions and Splits
### MySql
A partition is represented by a single partition column of type varchar. We leverage partitions defined on a MySql table, and this column contains partition names. For a table that does not have partition names, * is returned which is equivalent to a single partition. A partition is equivalent to a split.

|Name|Type|Description
|---|---|---|
|partition_name|Varchar|Named partition in MySql. E.g. p0|

 
### PostGreSql & Redshift
A partition is represented by two partition columns of type varchar. We leverage partitions as child tables defined on a PostGres table, and these columns contain child schema and child table information. For a table that does not have partition names, * is returned which is equivalent to a single partition. A partition is equivalent to a split.

|Name|Type|Description
|---|---|---|
|partition_schema|Varchar|Child table schema name|
|partition_name|Varchar|Child table name|

**Note:** In case of Redshift partition_schema and partition_name will always be '*'. It does not support external partitions. Performance with huge datasets is slow.

### Deploying The Connector

To use the Amazon Athena HBase Connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-jdbc dir, run `mvn clean install`.
3. From the athena-jdbc dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-jdbc` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

# JDBC Driver Versions

For latest version information see [pom.xml](./pom.xml).

# Limitations
* Write DDL operations are not supported.
* In Mux setup, spill bucket and prefix is shared across all database instances.
* Any relevant Lambda Limits. See Lambda documentation.
* Redshift does not support external partitions so all data will be retrieved every time.

# Performance tuning

MySql and PostGreSql support native partitions. Athena's lambda connector can retrieve data from these partitions in parallel. We highly recommend native partitioning for retrieving huge datasets with uniform partition distribution.
