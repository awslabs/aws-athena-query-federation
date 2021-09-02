# Amazon Athena Redis Connector

This connector enables Amazon Athena to communicate with your Redis instance(s), making your Redis data accessible via SQL. 

**Athena Federated Queries are now enabled as GA in us-east-1, us-east-2, us-west-2, eu-west-1, ap-northeast-1, ap-south-1, us-west-1, ap-southeast-1, ap-southeast-2, eu-west-2, ap-northeast-2, eu-west-3, ca-central-1, sa-east-1, and eu-central-1. To use this feature, upgrade your engine version to Athena V2 in your workgroup settings. Check documentation here for more details: https://docs.aws.amazon.com/athena/latest/ug/engine-versions.html.**

Unlike traditional relational data stores, Redis does not have the concept of a table or a column. Instead, Redis offers key-value access patterns where the key is essentially a 'string' and the value is one of: string, z-set, hmap. The Athena Redis Connector allows you to configure virtual tables using the Glue Data Catalog for schema and special table properties to tell the Athena Redis Connector how to map your Redis key-values into a table. You can read more on this below in the 'Setting Up Tables Section'.


## Usage

### Parameters

The Athena Redis Connector exposes several configuration options via Lambda environment variables. More detail on the available parameters can be found below.

1. **spill_bucket** - When the data returned by your Lambda function exceeds Lambda’s limits, this is the bucket that the data will be written to for Athena to read the excess from. (e.g. my_bucket)
2. **spill_prefix** - (Optional) Defaults to sub-folder in your bucket called 'athena-federation-spill'. Used in conjunction with spill_bucket, this is the path within the above bucket that large responses are spilled to. You should configure an S3 lifecycle on this location to delete old spills after X days/Hours.
3. **kms_key_id** - (Optional) By default any data that is spilled to S3 is encrypted using AES-GCM and a randomly generated key. Setting a KMS Key ID allows your Lambda function to use KMS for key generation for a stronger source of encryption keys. (e.g. a7e63k4b-8loc-40db-a2a1-4d0en2cd8331)
4. **disable_spill_encryption** - (Optional) Defaults to False so that any data that is spilled to S3 is encrypted using AES-GCM either with a randomly generated key or using KMS to generate keys. Setting this to false will disable spill encryption. You may wish to disable this for improved performance, especially if your spill location in S3 uses S3 Server Side Encryption. (e.g. True or False)
5. **glue_catalog** - (Optional) Can be used to target a cross-account Glue catalog. By default the connector will attempt to get metadata from its own Glue account.

### Setting Up Databases & Tables

To enable a Glue Table for use with Redis, you can set the following properties on the Table. redis-endpoint , redis-value-type, and one of redis-keys-zset or redis-key-prefix. Also note that any Glue database which may contain redis tables should have "redis-db-flag" somewhere in the URI property of the Database. You can set this from the Glue Console by editing the database.

1. **redis-endpoint** - (required) The hostname:port:password of the redis server that data for this table should come from. (e.g. athena-federation-demo.cache.amazonaws.com:6379) Alternatively, you can store the endpoint or part of the endpoint in AWS Secrets Manager by using ${secret_name} as the table property value.*
2. **redis-keys-zset** - (required if not using # 3) A comma separated list of keys whose value is a zset. Each of the values in the zset is then treated as a key that is part of this table. You must set either this or redis-key-prefix. (e.g. active-orders,pending-orders)
3. **redis-key-prefix** - (required if not using # 2)  A comma separated list of key prefixes to scan for values that should be part of this table. You must set either this or redis-keys-zset on the table. (e.g. accounts-*,acct-)
4. **redis-value-type** - (required) Defines how the value for the keys defined by either redis-key-prefix or redis-keys-zset will be mapped to your table. literal maps to a single column. zset also maps to a single column but each key can essentially store N rows. hash allows for each key to be a row with multiple columns. (e.g. hash or literal or zset)
5. **redis-ssl-flag** - (optional) Defaults to False, setting this to True will create a redis connection with SSL/TLS. (e.g. True or False)
6. **redis-cluster-flag** - (optional) Defaults to False, setting this to True will enable support for clustered Redis instances. (e.g. True or False)

*To use the Athena Federated Query feature with AWS Secrets Manager, the VPC connected to your Lambda function should have [internet access](https://aws.amazon.com/premiumsupport/knowledge-center/internet-access-lambda-function/) or a [VPC endpoint](https://docs.aws.amazon.com/secretsmanager/latest/userguide/vpc-endpoint-overview.html#vpc-endpoint-create) to connect to Secrets Manager.

### Data Types

All Redis values are retrieved as the basic String data type. From there they are converted to one of the below Apache Arrow data types used by the Athena Query Federation SDK based on how you've defined your table(s) in Glue's DataCatalog.

|Glue DataType|Apache Arrow Type|
|-------------|-----------------|
|int|INT|
|string|VARCHAR|
|bigint|BIGINT|
|double|FLOAT8|
|float|FLOAT4|
|smallint|SMALLINT|
|tinyint|TINYINT|
|boolean|BIT|
|binary|VARBINARY|

### Required Permissions

Review the "Policies" section of the athena-redis.yaml file for full details on the IAM Policies required by this connector. A brief summary is below.

1. S3 Write Access - In order to successfully handle large queries, the connector requires write access to a location in S3. 
2. SecretsManager Read Access - If you choose to store redis-endpoint details in SecretsManager you will need to grant the connector access to those secrets.
3. Glue Data Catalog - Since Redis does not have a meta-data store, the connector requires Read-Only access to Glue's DataCatalog for obtaining Redis key to table/column mappings. 
4. VPC Access - In order to connect to your VPC for the purposes of communicating with your Redis instance(s), the connector needs the ability to attach/detach an interface to the VPC.
5. CloudWatch Logs - This is a somewhat implicit permission when deploying a Lambda function but it needs access to cloudwatch logs for storing logs.
1. Athena GetQueryExecution - The connector uses this access to fast-fail when the upstream Athena query has terminated.

### Running Integration Tests

The integration tests in this module are designed to run without the prior need for deploying the connector. Nevertheless,
the integration tests will not run straight out-of-the-box. Certain build-dependencies are required for them to execute correctly.
For build commands and step-by-step instructions on building and running the integration tests see the
[Running Integration Tests](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-integ-test/README.md#running-integration-tests) README section in the **athena-federation-integ-test** module.

In addition to the build-dependencies, certain test configuration attributes must also be provided in the connector's [test-config.json](./etc/test-config.json) JSON file.
For additional information about the test configuration file, see the [Test Configuration](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-integ-test/README.md#test-configuration) README section in the **athena-federation-integ-test** module.

Once all prerequisites have been satisfied, the integration tests can be executed by specifying the following command: `mvn failsafe:integration-test` from the connector's root directory.

### Deploying The Connector

To use the Amazon Athena Redis Connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-redis dir, run `mvn clean install`.
3. From the athena-redis dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-redis` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

## Performance

The Athena Redis Connector will attempt to parallelize queries against your Redis instance depending on the type of table you've defined (zset keys vs. prefix keys). Predicate Pushdown is performed within the Lambda function.

## License

This project is licensed under the Apache-2.0 License.
