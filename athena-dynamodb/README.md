# Amazon Athena Cloudwatch Connector

This connector enables Amazon Athena to communicate with DynamoDB, making your tables accessible via SQL. 

## Usage

### Parameters

The Athena DynamoDB Connector exposes several configuration options via Lambda environment variables. More detail on the available parameters can be found below.

1. **spill_bucket** - When the data returned by your Lambda function exceeds Lambda’s limits, this is the bucket that the data will be written to for Athena to read the excess from. (e.g. my_bucket)
2. **spill_prefix** - (Optional) Defaults to sub-folder in your bucket called 'athena-federation-spill'. Used in conjunction with spill_bucket, this is the path within the above bucket that large
responses are spilled to. You should configure an S3 lifecycle on this location to delete old spills after X days/Hours.
3. **kms_key_id** - (Optional) By default any data that is spilled to S3 is encrypted using AES-GCM and a randomly generated key. Setting a KMS Key ID allows your Lambda function to use KMS for key
generation for a stronger source of encryption keys. (e.g. a7e63k4b-8loc-40db-a2a1-4d0en2cd8331)
4. **disable_spill_encryption** - (Optional) Defaults to False so that any data that is spilled to S3 is encrypted using AES-GMC either with a randomly generated key or using KMS to generate keys.
Setting this to false will disable spill encryption. You may wish to disable this for improved performance, especially if your spill location in S3 uses S3 Server Side Encryption. (e.g. True or False)
5. **disable_glue** - (Optional) If set to false, the connector will no longer attempt to retrieve supplemental metadata from Glue.
6. **glue_catalog** - (Optional) Can be used to target a cross-account Glue catalog. By default the connector will attempt to get metadata from its own Glue account.

### Required Permissions

Review the "Policies" section of the athena-dynamodb.yaml file for full details on the IAM Policies required by this connector. A brief summary is below.

1. DynamoDB Read Access - The connector uses the DescribeTable, ListSchemas, ListTables, Query, and Scan APIs.
2. S3 Write Access - In order to successfully handle large queries, the connector requires write access to a location in S3.
3. Glue Data Catalog - Since DynamoDB does not have a meta-data store, the connector requires Read-Only access to Glue's DataCatalog for supplemental table schema information.
4. CloudWatch Logs - This is a somewhat implicit permission when deploying a Lambda function but it needs access to cloudwatch logs for storing logs.

### Deploying The Connector

To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from
source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-dynamodb dir, run `mvn clean install`.
3. From the athena-dynamodb dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-dynamodb` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command
is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the
connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

For a direct deployment to Lambda, you can use the below command from the athena-aws-cmdb directory. Be sure to insert your S3 Bucket and Role ARN as indicated.

```bash
aws lambda create-function \
        --function-name athena-dynamodb \
        --runtime java8 \
        --timeout 900 \
        --memory-size 3008 \
        --zip-file fileb://target/athena-dynamodb-1.0.jar \
        --handler com.amazonaws.athena.connectors.dynamodb.DynamoDBCompositeHandler \
        --role "!!!<LAMBDA_ROLE_ARN>!!!" \
        --environment Variables={spill_bucket=<!!BUCKET_NAME!!>}
```

## Performance

The Athena DynamoDB Connector does support parallel scans and will attempt to push down predicates as part of its DynamoDB queries.  A hash key predicate with X distinct values will result in X Query
calls to DynamoDB.  All other predicate scenarios will results in Y number of Scan calls where Y is heuristically determined based on the size of your table and its provisioned throughput.