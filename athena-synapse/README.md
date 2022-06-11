# Amazon Athena Lambda Synapse Connector

This connector enables Amazon Athena to access your Azure Synapse database.

**Athena Federated Queries are now enabled as GA in us-east-1, us-east-2, us-west-2, eu-west-1, ap-northeast-1, ap-south-1, us-west-1, ap-southeast-1, ap-southeast-2, eu-west-2, ap-northeast-2, eu-west-3, ca-central-1, sa-east-1, and eu-central-1. To use this feature, upgrade your engine version to Athena V2 in your workgroup settings. Check documentation here for more details: https://docs.aws.amazon.com/athena/latest/ug/engine-versions.html.**

By using this connector, you acknowledge the inclusion of third party components, a list of which can be found in [`pom.xml`](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-synapse/pom.xml) and agree to the terms in their respective licenses, provided in [`LICENSE.txt`](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-synapse/LICENSE.txt).

# Terms

* **Database Instance:** Any instance of a database deployed on premises, EC2 or using RDS.
* **Handler:** A Lambda handler accessing your database instance(s). Could be metadata or a record handler.
* **Metadata Handler:** A Lambda handler that retrieves metadata from your database instance(s).
* **Record Handler:** A Lambda handler that retrieves data records from your database instance(s).
* **Composite Handler:** A Lambda handler that retrieves metadata and data records from your database instance(s). This is recommended to be set as lambda function handler.
* **Multiplexing Handler:** a Lambda handler that can accept and use multiple different database connections.
* **Property/Parameter:** A database property used by handlers to extract database information for connection. These are set as Lambda environment variables.
* **Connection- String:** Used to establish connection to a database instance.
* **Catalog:** Athena Catalog. This is not a Glue Catalog. Must be used to prefix `connection_string` property.

# Usage

## Parameters

The Synapse Connector supports several configuration parameters using Lambda environment variables.

### Connection String:

A JDBC Connection string is used to connect to a database instance. Following format is supported:

`synapse://${jdbc_connection_string}`

### Multiplexing handler parameters

Multiplexer provides a way to connect to multiple database instances using a single Lambda function. Requests are routed depending on catalog name. Use following classes in Lambda for using multiplexer.

| Handler           | Class                       |
|-------------------|-----------------------------|
| Composite Handler | SynapseMuxCompositeHandler  |
| Metadata Handler  | SynapseMuxMetadataHandler   |
| Record Handler    | SynapseMuxRecordHandler     |


**Parameters:**

```
${catalog}_connection_string    Database instance connection string. One of two types specified above. Required.
                                Example: If the catalog as registered with Athena is synapsecatalog then the environment variable name should be synapsecatalog_connection_string
                                
default                         Default connection string. Required. This will be used when catalog is `lambda:${AWS_LAMBDA_FUNCTION_NAME}`.
```

Example properties for a Synapse Mux Lambda function that supports two database instances, synapse1(default) and synapse2:

| Property                          | Value                                                                                           |
|-----------------------------------|-------------------------------------------------------------------------------------------------|
| default                           | synapse://jdbc:sqlserver://synapse1.hostname:port;databaseName=<database_name>;${secret1_name}  |
| synapsecatalog1_connection_string | synapse://jdbc:sqlserver://synapse1.hostname:port;databaseName=<database_name>;${secret1_name}  |
| synapsecatalog2_connection_string | synapse://jdbc:sqlserver://synapse2.hostname:port;databaseName=<database_name>;${secret2_name}  |

Synapse Connector supports substitution of any string enclosed like *${secret1_name}* with *username* and *password* retrieved from AWS Secrets Manager. Example:

```
synapse://jdbc:sqlserver://hostname:port;datbaseName=<database_name>;${secret_name}
```

will be modified to:

```
synapse://jdbc:sqlserver://hostname:port;datbaseName=<database_name>;user=<user>;password=<password>
```

Secret Name `secret_name` will be used to retrieve secrets.

### Single connection handler parameters

Single connection metadata and record handlers can also be used to connect to a single Synapse instance.
```
Composite Handler    SynapseCompositeHandler
Metadata Handler     SynapseMetadataHandler
Record Handler       SynapseRecordHandler
```

**Parameters:**

```
default         Default connection string. Required. This will be used when a catalog is not recognized.
```

These handlers support one database instance and must provide `default` connection string parameter. All other connection strings are ignored.

**Example property for a single Synapse instance supported by a Lambda function:**

| Property | Value                                                                               |
|----------|-------------------------------------------------------------------------------------|
| default  | synapse://jdbc:sqlserver://hostname:port;datbaseName=<database_name>;${secret_name} |

### Spill parameters:

Lambda SDK may spill data to S3. All database instances accessed using a single Lambda spill to the same location.

```
spill_bucket    Spill bucket name. Required.
spill_prefix    Spill bucket key prefix. Required.
spill_put_request_headers    JSON encoded map of request headers and values for the s3 putObject request used for spilling. Example: `{"x-amz-server-side-encryption" : "AES256"}`. For more possible headers see: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
```

# Data types support

| Synapse         |  Arrow            |
|-----------------|-------------------|
| bit             | TINYINT           |
| tinyint         | SMALLINT          |
| smallint        | SMALLINT          |
| int             | INT               |
| bigint          | BIGINT            |
| decimal         | DECIMAL           |
| numeric         | FLOAT8            |
| smallmoney      | FLOAT8            |
| money           | DECIMAL           |
| float[24]       | FLOAT4            |
| float[53]       | FLOAT8            |
| real            | FLOAT4            |
| datetime        | Date(MILLISECOND) |
| datetime2       | Date(MILLISECOND) |
| smalldatetime   | Date(MILLISECOND) |
| date            | Date(DAY)         |
| time            | VARCHAR           |
| datetimeoffset  | Date(MILLISECOND) |
| char[n]         | VARCHAR           |
| varchar[n/max]  | VARCHAR           |
| nchar[n]        | VARCHAR           |
| nvarchar[n/max] | VARCHAR           |

# Secrets

We support two ways to input database username and password:

1. **AWS Secrets Manager:** The name of the secret in AWS Secrets Manager can be embedded in JDBC connection string, which is used to replace with `user` and `password` values from Secret. Support is tightly integrated for AWS RDS database instances. When using AWS RDS, we highly recommend using AWS Secrets Manager, including credential rotation. If your database is not using AWS RDS, store credentials as JSON in the following format `{“username”: “${username}”, “password”: “${password}”}.`. To use the Athena Federated Query feature with AWS Secrets Manager, the VPC connected to your Lambda function should have [internet access](https://aws.amazon.com/premiumsupport/knowledge-center/internet-access-lambda-function/) or a [VPC endpoint](https://docs.aws.amazon.com/secretsmanager/latest/userguide/vpc-endpoint-overview.html#vpc-endpoint-create) to connect to Secrets Manager.
2. **Connection String:** Username and password can be specified as properties in the JDBC connection string.

# Partitions and Splits

Synapse supports Range Partitioning. A partition is represented by a single partition column of type varchar. Partitioning strategy is implemented by extracting the partition column and partition range from Synapse metadata tables. So the splits are created by custom queries using these range values.

# Running Integration Tests

The integration tests in this module are designed to run without the prior need for deploying the connector. Nevertheless, the integration tests will not run straight out-of-the-box. Certain build-dependencies are required for them to execute correctly.
For build commands and step-by-step instructions on building and running the integration tests see the [Running Integration Tests](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-integ-test/README.md#running-integration-tests) README section in the **athena-federation-integ-test** module.

In addition to the build-dependencies, certain test configuration attributes must also be provided in the connector's [test-config.json](./etc/test-config.json) JSON file.
For additional information about the test configuration file, see the [Test Configuration](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-integ-test/README.md#test-configuration) README section in the **athena-federation-integ-test** module.

Once all prerequisites have been satisfied, the integration tests can be executed by specifying the following command: `mvn failsafe:integration-test` from the connector's root directory.
**Synapse integration Test suite will not create any Synapse instances and Database Schemas, Instead it will use existing Azure Synapse instance which is specified in json config file

# Deploying The Connector

To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the **athena-federation-sdk** dir, run `mvn clean install` if you haven't already.
2. From the **athena-federation-integ-test** dir, run `mvn clean install` if you haven't already
   (**Note: failure to follow this step will result in compilation errors**).
3. From the **athena-jdbc** dir, run `mvn clean install`.
4. From the **athena-synapse** dir, run `mvn clean install`.
5. From the **athena-synapse** dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-synapse` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

# JDBC Driver Versions

For latest version information see [pom.xml](./pom.xml).

# Limitations

* Write DDL operations are not supported.
* casting to appropriate data type in filter condition is needed for Date and Timestamp datatypes.
* Use <= or >= for searching negative values of type Real and Float.
* binary, varbinary, image, rowversion datatypes are not supported, there is data discrepancy between Synapse data and data rendered in athena.
* In Mux setup, spill bucket and prefix is shared across all database instances.
* Any relevant Lambda Limits. See Lambda documentation.
