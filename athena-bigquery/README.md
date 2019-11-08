# Amazon Athena Google BigQuery Connector

This connector enables Amazon Athena to communicate with BigQuery, making your BigQuery data accessible. 

## Usage

To use the Amazon Athena Google BigQuery Connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and beploy this connector from source follow the below steps:

1. (pre-requisite) Install maven on your development machine.
2. (pre-requisite) Install the aws cli and sam tool on your development machine.
3. You will need to store your Google Service Account Key in JSON format within Secrets Manager to allow the Lambdas to call the BigQuery API. The service account will need to have permissions to execute BigQuery Queries and retrieve metadata information. 
4. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
5. From the athena-bigquery dir, run `mvn clean install`.
6. From the athena-bigquery dir, run `sam package --template-file athena-bigquery.yaml --output-template-file packaged.yaml --s3-bucket <your_lambda_source_bucket_name>`
7. Deploy your application using either Server-less Application Repository or Lambda directly. Instructions below.

For Serverless Application Repository, run `sam publish --template packaged.yaml --region <aws_region>` from the athena-bigquery directory and then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

For a direct deployment to Lambda, you can use the below command from the athena-bigquery directory. Be sure to insert your S3 Bucket an Role ARN when indicated. You also need to go to the Lambda console to configure your VPC details so that Lambda function can access Google BigQuery as well as S3.

```bash
aws lambda create-function \
        --description "Enables Amazon Athena to communicate with Google BigQuery" \
        --function-name athena-google-bigquery-connector \
        --runtime java8 \
        --timeout 900 \
        --memory-size 3008 \
        --environment Variables='{spill_bucket=<!!BUCKET_NAME!!>,spill_prefix=<!!SPILL_PREFIX!!>,gcp_project_id=<!!GCP_PROJECT_ID!!>,secret_manager_gcp_creds_name=<!!SM_GCP_CREDS_NAME!!>}' \
        --zip-file fileb://target/athena-bigquery-1.0.jar \
        --handler com.amazonaws.athena.connectors.bigquery.BigQueryCompositeHandler \
        --role "!!!<LAMBDA_ROLE_ARN>!!!" 
```

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
  
## Limitations and Other Notes

The following is a list of limitations or other notes. 
- Lambda has a maximum timeout value of 15 mins. Each split executes a query on BigQuery and must finish with enough time to store the results for Athena to read. If the Lambda times out, the query will fail.
- Google BigQuery is case sensitive. We attempt to correct the case of dataset names, and table names but we do not do any case correction for project id's. This is necessary because Presto lower cases all metadata. These corrections will make many extra calls to Google BigQuery. 
- Many data types are currently not supported, such as Timestamps, Dates, Binary, and Complex data types such as Maps, Lists, Structs. 

## Performance

This connector will attempt to push as many constraints to Google BigQuery to decrease the number of results returned. This connector currently does not support querying partitioned tables. This will be added in a future release.
