# Amazon Athena Timestream Connector


This connector enables Amazon Athena to communicate with AWS Timestream, making your timeseries data accessible via Amazon Athena. 

**Athena Federated Queries are now enabled as GA in us-east-1, us-east-2, us-west-2, eu-west-1, ap-northeast-1, ap-south-1, us-west-1, ap-southeast-1, ap-southeast-2, eu-west-2, ap-northeast-2, eu-west-3, ca-central-1, sa-east-1, and eu-central-1. To use this feature, upgrade your engine version to Athena V2 in your workgroup settings. Check documentation here for more details: https://docs.aws.amazon.com/athena/latest/ug/engine-versions.html.**

## Usage

### Parameters

The Athena Timestream Connector exposes several configuration options via Lambda environment variables. More detail on the available parameters can be found below.

1. **spill_bucket** - When the data returned by your Lambda function exceeds Lambda’s limits, this is the bucket that the data will be written to for Athena to read the excess from. (e.g. my_bucket)
2. **spill_prefix** - (Optional) Defaults to sub-folder in your bucket called 'athena-federation-spill'. Used in conjunction with spill_bucket, this is the path within the above bucket that large responses are spilled to. You should configure an S3 lifecycle on this location to delete old spills after X days/Hours.
3. **kms_key_id** - (Optional) By default any data that is spilled to S3 is encrypted using AES-GCM and a randomly generated key. Setting a KMS Key ID allows your Lambda function to use KMS for key generation for a stronger source of encryption keys. (e.g. a7e63k4b-8loc-40db-a2a1-4d0en2cd8331)
4. **disable_spill_encryption** - (Optional) Defaults to False so that any data that is spilled to S3 is encrypted using AES-GMC either with a randomly generated key or using KMS to generate keys. Setting this to false will disable spill encryption. You may wish to disable this for improved performance, especially if your spill location in S3 uses S3 Server Side Encryption. (e.g. True or False)
5. **glue_catalog** - (Optional) Can be used to target a cross-account Glue catalog. By default the connector will attempt to get metadata from its own Glue account.
6. **list_tables_page_size** - Page size used for the pagination of the ListTableMetadata API (1-50, Default: 50).
   This specifies the maximum number of tables for which metadata will be retrieved by each paginated request.
   For additional information, see [ListTableMetadata Pagination](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-sdk/README.md#ListTableMetadata-Pagination)
   in the SDK's README.

### Setting Up Databases & Tables

You can optionally use AWS Glue Data Catalog as a source of supplemental metadata. When enabled, the Timestream connector will check for a matching database and table in AWS Glue which has one or more of the below properties. This is particularly useful if you want to use feature of Timestream's SQL Dialect which are not natively supported by Athena. if the matching Glue table is infact a 'view' the Athena Timestream Connector will use that SQL from the view, in conjunction with your Athena SQL Query to access your data.  

To enable a Glue Table for use with Timestream, you can set the following properties on the Table. 

1. **timestream-metadata-flag** - Flag indicating that the table can be used for supplemental meta-data by the Athena Timestream Connector. The value is unimportant as long as this key is present in the properties of the table.
2. **_view_template** - When using Glue for supplimental metadata, you can set this table property and include any arbitrary Timestream SQL as the 'view'. The Athena Timestream connector will then use this SQL, combined with the SQL from Athena, to run your query. This allows you to access feature in Timestream SQL that are not avialable in Athena's SQL dialect.
  
### Data Types

This connector presently only supports a subset of the data types available in Timestream, noteably: scalar values of varchar, double, timestamp. 

In order to query the "timeseries" data type, you will need to setup a 'view' in AWS Glue that leverages the 'CREATE_TIME_SERIES' function of Timestream. You'll also need to supply a schema for the view which uses "ARRAY<STRUCT<time:timestamp,measure_value\:\:double:double>>" as the type of any your timeseries columns. Be sure to replace "double" with the apprpopriate scalar type for your table.

Below is a sample of how you can setup such a view over a timeseries in AWS Glue.

![Example](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-timestream/docs/img/timestream_glue_example.png?raw=true)



### Required Permissions

Review the "Policies" section of the athena-redis.yaml file for full details on the IAM Policies required by this connector. A brief summary is below.

1. S3 Write Access - In order to successfully handle large queries, the connector requires write access to a location in S3. 
2. Glue Data Catalog - Since Redis does not have a meta-data store, the connector requires Read-Only access to Glue's DataCatalog for obtaining Redis key to table/column mappings. 
3. CloudWatch Logs - This is a somewhat implicit permission when deploying a Lambda function but it needs access to cloudwatch logs for storing logs.
4. Athena GetQueryExecution - The connector uses this access to fast-fail when the upstream Athena query has terminated.
5. Timestream Access - In order to run Timestream queries.

### Running Integration Tests

The integration tests in this module are designed to run without the prior need for deploying the connector. Nevertheless,
the integration tests will not run straight out-of-the-box. Certain build-dependencies are required for them to execute correctly.
For build commands and step-by-step instructions on building and running the integration tests see the
[Running Integration Tests](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-integ-test/README.md#running-integration-tests) README section in the **athena-federation-integ-test** module.

In addition to the build-dependencies, certain test configuration attributes must also be provided in the connector's [test-config.json](./etc/test-config.json) JSON file.
For additional information about the test configuration file, see the [Test Configuration](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-integ-test/README.md#test-configuration) README section in the **athena-federation-integ-test** module.

Once all prerequisites have been satisfied, the integration tests can be executed by specifying the following command: `mvn failsafe:integration-test` from the connector's root directory.

### Deploying The Connector

To use the Amazon Athena Timestream Connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-redis dir, run `mvn clean install`.
3. From the athena-redis dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-timestream` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

## Performance

The performance of this connector is currently a work in progress and is significantly (> 2x) slower than running queries from Timestream itself. We recommend limiting the data returned (not data scanned) to less than 256MB for the initial release. There are a number of unique and interesting use cases that are possible even well below the 256MB recommendation. 

## License

This project is licensed under the Apache-2.0 License.
