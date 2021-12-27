# Amazon Athena DynamoDB Connector

This connector enables Amazon Athena to communicate with DynamoDB, making your tables accessible via SQL. 

**Athena Federated Queries are now enabled as GA in us-east-1, us-east-2, us-west-2, eu-west-1, ap-northeast-1, ap-south-1, us-west-1, ap-southeast-1, ap-southeast-2, eu-west-2, ap-northeast-2, eu-west-3, ca-central-1, sa-east-1, and eu-central-1. To use this feature, upgrade your engine version to Athena V2 in your workgroup settings. Check documentation here for more details: https://docs.aws.amazon.com/athena/latest/ug/engine-versions.html.**

## Usage

### Parameters

The Athena DynamoDB Connector exposes several configuration options via Lambda environment variables. More detail on the available parameters can be found below.

1. **SpillBucket** - When the data returned by your Lambda function exceeds Lambda’s limits, this is the bucket that the data will be written to for Athena to read the excess from. (e.g. my_bucket)
2. **SpillPrefix** - (Optional) Defaults to sub-folder in your bucket called 'athena-federation-spill'. Used in conjunction with spill_bucket, this is the path within the above bucket that large
responses are spilled to. You should configure an S3 lifecycle on this location to delete old spills after X days/Hours.
3. **KMSKeyId** - (Optional) By default any data that is spilled to S3 is encrypted using AES-GCM and a randomly generated key. Setting a KMS Key ID allows your Lambda function to use KMS for key
generation for a stronger source of encryption keys. (e.g. a7e63k4b-8loc-40db-a2a1-4d0en2cd8331)
4. **DisableSpillEncryption** - (Optional) Defaults to False so that any data that is spilled to S3 is encrypted using AES-GCM either with a randomly generated key or using KMS to generate keys.
Setting this to false will disable spill encryption. You may wish to disable this for improved performance, especially if your spill location in S3 uses S3 Server Side Encryption. (e.g. True or False)
5. **disable_glue** - (Optional) If present, with any value except false, the connector will no longer attempt to retrieve supplemental metadata from Glue.
6. **glue_catalog** - (Optional) Can be used to target a cross-account Glue catalog. By default the connector will attempt to get metadata from its own Glue account.
7. **LambdaMemory** - (Optional) Memory is the amount of memory available to your Lambda function at runtime. Set a value between 128 MB and 10240 MB
8. **LambdaTimeout** - (Optional)  Amount of time that the Lambda function handler can run for an invocation. Set a value between 1 second and 900 seconds

### Setting Up Databases & Tables in Glue

To enable a Glue Table for use with DynamoDB, you simply need to have a table that matches any DynamoDB Table that you'd like to supply supplemental metadata for (instead of relying on the DynamoDB
Connector's limited ability to infer schema). You can enable a Glue table to be used for supplemental metadata by setting one of the below table properties from the Glue Console when editing the Table in
question.  These properties are automatically set if you use Glue's DynamoDB Crawler.  The only other thing you need to do is ensure you use the appropriate data types when defining manually or validate
the columns and types that the Crawler discovered.

1. **dynamodb** - String indicating that the table can be used for supplemental meta-data by the Athena DynamoDB Connector. This string can be in any one of the following places:
    1. in the table properties/parameters under a field called "classification" (exact match).
    2. in the table's storage descriptor's location field (substring match).
    3. in the table's storage descriptor's parameters under a field called "classification" (exact match).
2. **dynamo-db-flag** - String indicating that the *database* contains tables used for supplemental meta-data by the Athena DynamoDB Connector.  This is required for any Glue databases other than "default"
and is useful for filtering out irrelevant databases in accounts that have lots of them.  This string should be in the Location URI of the Glue Database (substring match).
3. **sourceTable** - Optional table property/parameter that defines the source table name in DynamoDB.  Use this if Glue table naming rules prevent you from creating a Glue table with the same name as
your DynamoDB table (e.g. capital letters are not permitted in Glue table names but are permitted in DynamoDB table names).
4. **columnMapping** - Optional table property/parameter that define column name mappings.  Use this if Glue column naming rules prevent you from creating a Glue table with the same column names as
your DynamoDB table (e.g. capital letters are not permitted in Glue column names but are permitted in DynamoDB column names).  This is expected to be in the format `col1=Col1,col2=Col2`.
5. **defaultTimeZone** - Optional table property/parameter for timezone that will be applied to date/datetime values without explicit timezone. To avoid any discrepancy between the data source default timezone and athena's session timezone, it is good practice to set this value.
6. **datetimeFormatMapping** - Optional table property/parameter that defines the date/datetime format to be used to parse the raw DynamoDB string in a particular column that is of Glue type `date` or `timestamp`. If not provided, the format will inferred using [various ISO-8601 format](https://commons.apache.org/proper/commons-lang/apidocs/org/apache/commons/lang3/time/DateFormatUtils.html). If the date/datetime format cannot be inferred or if the raw string fails to parse, then the value will be omitted from the result. The mapping is expected to be in the format `col1=someformat1,col2=someformat2`. Some examples of the date/datetime formats are `yyyyMMdd'T'HHmmss`, `ddMMyyyy'T'HH:mm:ss`. If your column is of date/datetime value without timezone, and you wish to use the column in the `WHERE` clause, you need to set this optional property for that column.


### Required Permissions

Review the "Policies" section of the athena-dynamodb.yaml file for full details on the IAM Policies required by this connector. A brief summary is below.

1. DynamoDB Read Access - The connector uses the DescribeTable, ListSchemas, ListTables, Query, and Scan APIs.
2. S3 Write Access - In order to successfully handle large queries, the connector requires write access to a location in S3.
3. Glue Data Catalog - Since DynamoDB does not have a meta-data store, the connector requires Read-Only access to Glue's DataCatalog for supplemental table schema information.
4. CloudWatch Logs - This is a somewhat implicit permission when deploying a Lambda function but it needs access to cloudwatch logs for storing logs.
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

To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from
source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the **athena-federation-sdk** dir, run `mvn clean install` if you haven't already.
2. From the **athena-federation-integ-test** dir, run `mvn clean install` if you haven't already
   (**Note: failure to follow this step will result in compilation errors**).
3. From the **athena-dynamodb** dir, run `mvn clean install`.
4. From the **athena-dynamodb** dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-dynamodb` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command
is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the
connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

## Performance

The Athena DynamoDB Connector does support parallel scans and will attempt to push down predicates as part of its DynamoDB queries.  A hash key predicate with X distinct values will result in X Query
calls to DynamoDB.  All other predicate scenarios will results in Y number of Scan calls where Y is heuristically determined based on the size of your table and its provisioned throughput.

## Costs

The costs for use of this solution depends on the underlying AWS resources being used. Pay special attention to [DynamoDB pricing](https://aws.amazon.com/dynamodb/pricing/) since queries using scans can consume a large number of RCU's.

