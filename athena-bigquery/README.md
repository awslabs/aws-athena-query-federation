# Amazon Athena Google BigQuery Connector

This connector enables Amazon Athena to communicate with BigQuery, making your BigQuery data accessible. 

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
  
  ### Deploying The Connector
  
  To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:
  
  1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
  2. From the athena-bigquery dir, run `mvn clean install`.
  3. From the athena-bigquery dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-bigquery` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)


## Limitations and Other Notes

The following is a list of limitations or other notes. 
- Lambda has a maximum timeout value of 15 mins. Each split executes a query on BigQuery and must finish with enough time to store the results for Athena to read. If the Lambda times out, the query will fail.
- Google BigQuery is case sensitive. We attempt to correct the case of dataset names, and table names but we do not do any case correction for project id's. This is necessary because Presto lower cases all metadata. These corrections will make many extra calls to Google BigQuery. 
- Many data types are currently not supported, such as Timestamps, Dates, Binary, and Complex data types such as Maps, Lists, Structs. 

## Performance

This connector will attempt to push as many constraints to Google BigQuery to decrease the number of results returned. This connector currently does not support querying partitioned tables. This will be added in a future release.

## License

This project is licensed under the Apache-2.0 License.