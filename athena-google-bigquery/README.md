# Amazon Athena Google BigQuery Connector

This connector enables Amazon Athena to communicate with BigQuery, making your BigQuery data accessible.

**Athena Federated Queries are now enabled as GA in us-east-1, us-east-2, us-west-2, eu-west-1, ap-northeast-1, ap-south-1, us-west-1, ap-southeast-1, ap-southeast-2, eu-west-2, ap-northeast-2, eu-west-3, ca-central-1, sa-east-1, and eu-central-1. To use this feature, upgrade your engine version to Athena V2 in your workgroup settings. Check documentation here for more details: https://docs.aws.amazon.com/athena/latest/ug/engine-versions.html.**

By using this connector, you acknowledge the inclusion of third party components, a list of which can be found in [`pom.xml`](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-google-bigquery/pom.xml) and agree to the terms in their respective licenses, provided in [`LICENSE.txt`](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-google-bigquery/LICENSE.txt).


### Parameters

The Athena Google BigQuery Connector exposes several configuration options via Lambda environment variables. More detail on the available parameters can be found below.

|Parameter Name|Example Value|Description|
|--------------|--------------------|------------------|
|spill_bucket|my_bucket|When the data returned by your Lambda function exceeds Lambda’s limits, this is the bucket that the data will be written to for Athena to read the excess from.|
|spill_prefix|temporary/split| (Optional) Defaults to sub-folder in your bucket called 'athena-federation-spill'. Used in conjunction with spill_bucket, this is the path within the above bucket that large responses are spilled to. You should configure an S3 lifecycle on this location to delete old spills after X days/Hours.|
|kms_key_id|a7e63k4b-8loc-40db-a2a1-4d0en2cd8331|(Optional) By default any data that is spilled to S3 is encrypted using AES-GCM and a randomly generated key. Setting a KMS Key ID allows your Lambda function to use KMS for key generation for a stronger source of encryption keys.|
|disable_spill_encryption|True or False|(Optional) Defaults to False so that any data that is spilled to S3 is encrypted using AES-GMC either with a randomly generated key or using KMS to generate keys. Setting this to false will disable spill encryption. You may wish to disable this for improved performance, especially if your spill location in S3 uses S3 Server Side Encryption.|
|gcp_project_id|semiotic-primer-1234567|The project id (not project name) that contains the datasets that this connector should read from.|
|secret_manager_gcp_creds_name|GoogleCloudPlatformCredentials|The name of the secret within AWS Secrets Manager that contains your BigQuery credentials JSON. The credentials |

# Partitions and Splits
Currently splits are not based on partitions and based on configured `concurrencyLimit` environment variable.
It will decide the page count for split and uses the limit and offset while executing the query instead of using partition value in the query.
For predicate push down queries no splits are being used since it returns the lesser results. In addition,splits strategy is applicable only
for non predicate push down queries(for select * queries). We can further increase the `concurrencyLimit` as per Google Bigquery Quota limits
configured within Google project.

# Running Integration Tests

The integration tests in this module are designed to run without the prior need for deploying the connector. Nevertheless,
the integration tests will not run straight out-of-the-box. Certain build-dependencies are required for them to execute correctly.
For build commands and step-by-step instructions on building and running the integration tests see the
[Running Integration Tests](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-integ-test/README.md#running-integration-tests) README section in the **athena-federation-integ-test** module.

In addition to the build-dependencies, certain test configuration attributes must also be provided in the connector's [test-config.json](./etc/test-config.json) JSON file.
For additional information about the test configuration file, see the [Test Configuration](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-integ-test/README.md#test-configuration) README section in the **athena-federation-integ-test** module.

Once all prerequisites have been satisfied, the integration tests can be executed by specifying the following command: `mvn failsafe:integration-test` from the connector's root directory.
** Google BigQuery integration Test suite will not create any Google Bigquery service and Datasets, Instead it will use existing Google Bigquery project and datasets.

### Deploying The Connector

To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-bigquery dir, run `mvn clean install`.
3. From the athena-bigquery dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-google-bigquery` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

## Limitations and Other Notes

* Lambda has a maximum timeout value of 15 mins. Each split executes a query on BigQuery and must finish with enough time to store the results for Athena to read. If the Lambda times out, the query will fail.
* Google BigQuery is case sensitive. We attempt to correct the case of dataset names, and table names but we do not do any case correction for project id's. This is necessary because Presto lower cases all metadata. These corrections will make many extra calls to Google BigQuery.
* Binary, and Complex data types such as Maps, Lists, Structs data types are currently not supported,
* Connector may return Google quota limit issue due to Google bigquery concurrency and quota limits. Ref [Bigquery Quotas and limits ](https://cloud.google.com/bigquery/quotas). As a best practice and in order to avoid Quota limit issues push as many constraints to Google BigQuery

## Performance

This connector will attempt to push as many constraints to Google BigQuery to decrease the number of results returned.

## License

This project is licensed under the Apache-2.0 License.