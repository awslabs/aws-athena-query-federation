# Amazon Athena Lambda Snowflake Connector

This connector enables Amazon Athena to access your Snowflake SQL database or RDS instance(s) using JDBC driver.

**Athena Federated Queries are now enabled as GA in us-east-1, us-east-2, us-west-2, eu-west-1, ap-northeast-1, ap-south-1, us-west-1, ap-southeast-1, ap-southeast-2, eu-west-2, ap-northeast-2, eu-west-3, ca-central-1, sa-east-1, and eu-central-1. To use this feature, upgrade your engine version to Athena V2 in your workgroup settings. Check documentation here for more details: https://docs.aws.amazon.com/athena/latest/ug/engine-versions.html.**

By using this connector, you acknowledge the inclusion of third party components, a list of which can be found in [`pom.xml`](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-snowflake/pom.xml) and agree to the terms in their respective licenses, provided in [`LICENSE.txt`](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-snowflake/LICENSE.txt).

# Terms

* **Database Instance:** Any instance of a database deployed on premises, EC2 or using RDS.
* **Handler:** A Lambda handler accessing your database instance(s). Could be metadata or a record handler.
* **Metadata Handler:** A Lambda handler that retrieves metadata from your database instance(s).
* **Record Handler:** A Lambda handler that retrieves data records from your database instance(s).
* **Composite Handler:** A Lambda handler that retrieves metadata and data records from your database instance(s). This is recommended to be set as lambda function handler.
* **Multiplexing Handler:** a Lambda handler that can accept and use multiple different database connections.
* **Property/Parameter:** A database property used by handlers to extract database information for connection. These are set as Lambda environment variables.
* **Connection String:** Used to establish connection to a database instance.
* **Catalog:** Athena Catalog. This is not a Glue Catalog. Must be used to prefix `connection_string` property.

# Usage

## Parameters

The Snowflake Connector supports several configuration parameters using Lambda environment variables.

### Connection String:

A JDBC Connection string is used to connect to a database instance. Following format is supported: `snowflake://${jdbc_connection_string}`.

### Multiplexing handler parameters

Multiplexer provides a way to connect to multiple database instances using a single Lambda function. Requests are routed depending on catalog name. Use following classes in Lambda for using multiplexer.

|Handler|Class|
|---	|---	|
|Composite Handler|SnowflakeMuxCompositeHandler|
|Metadata Handler|SnowflakeMuxCompositeHandler|
|Record Handler|SnowflakeMuxCompositeHandler|


**Parameters:**

```
${catalog}_connection_string    Database instance connection string. One of two types specified above. Required.
                                Example: If the catalog as registered with Athena is snowflakecatalog then the environment variable name should be snowflakecatalog_connection_string

default                         Default connection string. Required. This will be used when catalog is `lambda:${AWS_LAMBDA_FUNCTION_NAME}`.
```

Example properties for a Snowflake Mux Lambda function that supports two database instances snowflake1(default) and snowflake2:

|Property|Value|
|---|---|
|default|```snowflake://jdbc:snowflake://snowflake1.host:port/?warehouse=warehousename&db=db1&schema=schema1&${Test/RDS/Snowflake1}```|
|	|	|
|snowflake_catalog1_connection_string|```snowflake://jdbc:snowflake://snowflake1.host:port/?warehouse=warehousename&db=db1&schema=schema1${Test/RDS/Snowflake1}```|
|	|	|
|snowflake_catalog2_connection_string|```snowflake://jdbc:snowflake://snowflake2.host:port/?warehouse=warehousename&db=db1&schema=schema1&user=sample2&password=sample2```|

Snowflake Connector supports substitution of any string enclosed like *${SecretName}* with *username* and *password* retrieved from AWS Secrets Manager. Example:

```
snowflake://jdbc:snowflake://snowflake1.host:port/?warehouse=warehousename&db=db1&schema=schema1${Test/RDS/Snowflake1}&...
```

will be modified to:

```
snowflake://jdbc:snowflake://snowflake2.host:port/?warehouse=warehousename&db=db1&schema=schema1&user=sample2&password=sample2&...
```

Secret Name `Test/RDS/Snowflake1` will be used to retrieve secrets.

Currently Snowflake recognizes `user` and `password` JDBC properties.It will take username and password like username/password without any key `user` and `password`

### Single connection handler parameters

Single connection metadata and record handlers can also be used to connect to a single Snowflake instance.
```
Composite Handler    SnowflakeCompositeHandler
Metadata Handler     SnowflakeMetadataHandler
Record Handler       SnowflakeRecordHandler
```

**Parameters:**

```
default         Default connection string. Required. This will be used when a catalog is not recognized.
```

These handlers support one database instance and must provide `default` connection string parameter. All other connection strings are ignored.

**Example property for a single Snowflake instance supported by a Lambda function:**

|Property|Value|
|---|---|
|default|```snowflake://jdbc:snowflake://snowflake1.host:port/?secret=Test/RDS/Snowflake1```|

### Spill parameters:

Lambda SDK may spill data to S3. All database instances accessed using a single Lambda spill to the same location.

```
spill_bucket    Spill bucket name. Required.
spill_prefix    Spill bucket key prefix. Required.
```

### PageCount parameter
Limits the number of records per partition

The default value is 500000

### PartitionLimit parameter
Limits the number of partitions. A large number may cause a time-out issue. Please reset to a lower value if you encounter a time-out error

The default value is 10

# Data types support

|Jdbc|Arrow|
Limit on number of partitions. A large number may cause time-out issue during running the query. Please reset to a lower value if you encounter a time-out error.

Default partition limit is 10

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
|ARRAY|List|

See Snowflake documentation for conversion between JDBC and database types.

# Data Types Conversion
In order to make the source (Snowflake) and Athena data types compatible, there are number of data types conversion which are being included in the Snowflake connector. This is in addition to JDBC ARROW conversion which are not covered. The purpose of these conversions is to make sure the mismatches on the type of data types have been addressed so that queries do get executed successfully. The details of the data types which have been converted are as below:

|Source Data Type (Snowflake)| Converted Data Type (Athena) |
| ---|------------------------------|
|TIMESTAMP| TIMESTAMPMILLI               |
|DATE| TIMESTAMPMILLI               |
|INTEGER| INT                          |
|DECIMAL| BIGINT                       |
|TIMESTAMP_NTZ| TIMESTAMPMILLI               |

In addition, all the unsupported data types are getting converted to VARCHAR.

# Secrets

We support two ways to input database username and password:

1. **AWS Secrets Manager:** The name of the secret in AWS Secrets Manager can be embedded in JDBC connection string, which is used to replace with `username` and `password` values from Secret. Support is tightly integrated for AWS RDS database instances. When using AWS RDS, we highly recommend using AWS Secrets Manager, including credential rotation. If your database is not using AWS RDS, store credentials as JSON in the following format `{“username”: “${username}”, “password”: “${password}”}.`. To use the Athena Federated Query feature with AWS Secrets Manager, the VPC connected to your Lambda function should have [internet access](https://aws.amazon.com/premiumsupport/knowledge-center/internet-access-lambda-function/) or a [VPC endpoint](https://docs.aws.amazon.com/secretsmanager/latest/userguide/vpc-endpoint-overview.html#vpc-endpoint-create) to connect to Secrets Manager.
2. **Connection String:** Username and password can be specified as properties in the JDBC connection string.

# Partitions and Splits

A partition is represented by a single partition column of type varchar. We have customized partition logic for snowflake at athena layer for parallel processing. A partition is equivalent to a split. Snowflake automatically determines the most efficient compression algorithm for the columns in each micro-partition.


|Name|Type|Description|
|---|---|---|
|partition|varchar|custom partition in athena. E.g. p-limit-3000-offset-0,p-limit-3000-offset-3000,p-limit-3000-offset-6000|

# Running Integration Tests

The integration tests in this module are designed to run without the prior need for deploying the connector. Nevertheless, the integration tests will not run straight out-of-the-box. Certain build-dependencies are required for them to execute correctly.
For build commands and step-by-step instructions on building and running the integration tests see the [Running Integration Tests](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-integ-test/README.md#running-integration-tests) README section in the **athena-federation-integ-test** module.

In addition to the build-dependencies, certain test configuration attributes must also be provided in the connector's [test-config.json](./etc/test-config.json) JSON file.
For additional information about the test configuration file, see the [Test Configuration](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-integ-test/README.md#test-configuration) README section in the **athena-federation-integ-test** module.

Once all prerequisites have been satisfied, the integration tests can be executed by specifying the following command: `mvn failsafe:integration-test` from the connector's root directory.

_**_ Snowflake integration Test suite will not create any Snowflake RDS instances and Database Schemas, Instead it will use existing Snowflake instance.


# Deploying The Connector

To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the **athena-federation-sdk** dir, run `mvn clean install` if you haven't already.
2. From the **athena-federation-integ-test** dir, run `mvn clean install` if you haven't already
   (**Note: failure to follow this step will result in compilation errors**).
3. From the **athena-jdbc** dir, run `mvn clean install`.
4. From the **athena-snowflake** dir, run `mvn clean install`.
5. From the **athena-snowflake** dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-snowflake` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

# JDBC Driver Versions

For latest version information see [pom.xml](./pom.xml).

# Limitations
* Write DDL operations are not supported.
* In Mux setup, spill bucket and prefix is shared across all database instances.
* Any relevant Lambda Limits. See Lambda documentation.
* In Snowflake, object names are case sensitive and this implicitly states that 2 different tables can have same name in lower and upper case i.e. 1. EMPLOYEE and 2. employee. In AFQ, the Schema/Table names are pushed to the lambda function in lower case.
  In order to handle this issue and as a work around for this specific scenario, users are expected to provide query hints to retrieve the data from the tables where the name is case sensitive. Query hints can be avoided for rest of the scenarios.
  Here are the sample queries with query hints.
  	SELECT * FROM "lambda:athenasnowflake".SYSTEM."MY_TABLE@schemaCase=upper&tableCase=upper”
  	SELECT * FROM "lambda:athenasnowflake".SYSTEM."MY_TABLE@schemaCase=upper&tableCase=lower”
* Current version does not support snowflake views. It will be added in next upgrade.

# Performance tuning

Suggeted to use filters in queries for optimized performance.In addition,We highly recommend native partitioning for retrieving huge datasets with uniform partition distribution.
