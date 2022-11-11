# Amazon Athena GCS Connector

This connector enables Amazon Athena to communicate with your GCS buckets and files and treat them as databases and tables. It allows accessing records inside those files via SQL.

### Design

![GCS Connector](gcs-connector.png "Architecture")

The Amazon Athena GCS connector for federated queries uses the following design.
1. A SQL query is issued against the table(s) in GCS. A table is formed from either a PARQUET or a CSV file (option is mutually exclusive) as of the current implementation
2. Table from the files in GCS is accessed via Google Cloud Storage SDK using a credential JSON (generated via service account key) found in the AWS Secret Manager
3. The connector will parse the SQL query to determine whether predicates will be used to filter the records found in PARQUET/CSV files.
5. The connector will use the Athena GCS Storage API, a library, to list tables/fields and fetch the records
6. The connector sends the fetched records to an S3 bucket (spill, when required). This will eliminate the performance problem of sending sizeable multi-GB result sets through Athena Storage API.
7. Athena will scan the S3 bucket to determine the number of files to read for the result set.
8. Athena will call Lambda with multiple calls to read back all the files (using S3 SELECT) that comprise the result set from GCS. This will allow Athena to parallelize the read of the S3 files up to a maximum bandwidth of 100GB per second.
9. Athena will process the data returned from GCS with data scanned from the data lake and return the result to the user.


### Parameters

The Amazon Athena GCS Connector exposes several configuration options via Lambda environment variables. More details on the available parameters are given below.

* **AthenaCatalogName:** Lambda function name
* **SpillBucket:** The bucket name where this function can spill data.
* **SpillPrefix:** The prefix within SpillBucket where this function can spill data.
* **SecurityGroupIds:** One or more SecurityGroup IDs corresponding to the SecurityGroup that should be applied to the Lambda function. (e.g., sg1, sg2, sg3)
* **SubnetIds:** One or more Subnet IDs corresponding to the Subnet that the Lambda function can use to access your data source. (e.g. subnet1, subnet2)
* **SecretNameOrPrefix:** The name or prefix of a set of names within Secrets Manager that this function should have access to. (e.g. gcs-*)
* **VpcId:** The VPC ID to be attached to the Lambda function
* **SecretNameOrPrefix** The name or prefix of a set of names within Secrets Manager that this function should have access to. (e.g. gcs-*). Please note that if you set up a required secret value without the given prefix, Athena will raise error.
* **GcsSecretName** Name of the secret in the AWS Secret Manager
* **GcsCredentialKey** Name of the key that contains GCS credential JSON (generated via service account key). Please refer to [this document](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) for more details
* **FileExtension** File extension (without period character) of the file in GCS buckets. Only one extension can be set; eitHer PARQUET or CSV (case-insensitive). These files will be parsed and shown as a Table under a database. So as you know, buckets are treated as databases. Please also note that invalid ANSI SQL characters in the file name will be replaced by an underscore character ('_'), and the file extension will be removed in the Table name

### Terms

* **Database Instance:** Any bucket in GCS.
* **Handler:** A Lambda handler accessing your database instance(s). It could be metadata or a record handler.
* **Metadata Handler:** A Lambda handler retrieves metadata from your database instance(s).
* **Record Handler:** A Lambda handler that retrieves data records from your database instance(s).
* **Composite Handler:** A Lambda handler retrieves metadata and data records from your database instance(s). This is recommended to be set as a Lambda function handler.

### Database-specific handler parameters

Database-specific metadata (extracted from GCS buckets and files inside those buckets) be used to list buckets (hence database) and tables (hence tables) and record handlers is used to fetch records and spill the data to an S3 bucket.

|Handler|Class|
|---|---|
|Composite Handler|GCSCompositeHandler|
|Metadata Handler|GCSMetadataHandler|
|Record Handler|GCSRecordHandler|

### Spill parameters:

Lambda SDK may spill data to S3. All database instances are accessed using a single Lambda spill to the exact location.

```
spill_bucket  Spill bucket name. Required.
spill_prefix  Spill bucket key prefix. Required.
```

### GCS Data types supported

| PARQUET       | Arrow     |
|-----------|-----------|
| BINARY | VARCHAR|
| BOOLEAN |BOOL|
| DOUBLE| DOUBLE|
| ENUM| VARCHAR|
| FIXED_LEN_BYTE_ARRAY| DECIMAL|
| FLOAT | FLOAT (32-bit)|
| INT32|INT32  or DATEDAY (When Parquet’s column’s logical type is DATE)|
| INT64 | INT64 OR Timestamp|
| INT96 | Timestamp|
| MAP | VARCHAR|
| LIST | VARCHAR|

**NOTE:** For CSV, all types are converted as Apache Arrow’s VARCHAR type by default

### Secrets

Secret Name and Credential Kay Name are two essential configuration elements without which it is impossible for the connector to list buckets and files as databases and tables using the Google GCS SDK.

Following are two such configurations:

* **GcsSecretName** Name of the secret in the AWS Secret Manager
* **GcsCredentialKey** Name of the key that contains GCS credential JSON (generated via service account key)

Usually, a JSON set against the **GcsCredentialKey** value that looks like the following:

```json
{
 "type": "service_account",
 "project_id": "[Your GCP project id]",
 "private_key_id": "[Private key id, automatically generated for you]",
 "private_key": "-----BEGIN PRIVATE KEY-----\n[The entire private key string automatically inserted during generation]\n-----END PRIVATE KEY-----\n",
 "client_email": "[Auto generated email for the specified IAM user]",
 "client_id": "[Id of the client based on IAM user]",
 "auth_uri": "https://accounts.google.com/o/oauth2/auth",
 "token_uri": "https://oauth2.googleapis.com/token",
 "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
 "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/[auto-inserted id for the IAM user]"
}
```

### Deploying The Connector

To use the Amazon Athena GCS Connector in your queries, build and deploy this connector from the source; follow the below steps:

1. From the athena-federation dir, run `mvn clean install` if you haven't already.
2. From the athena-storage-api dir, run `mvn clean install` if you haven't already.
3. From the athena-gcs dir, run `mvn clean install`.
4. From the athena-gcs dir, run `../tools/publish.sh S3_BUCKET_NAME athena-gcs [region]` to publish the connector to your private AWS Serverless Application Repository. The `S3_BUCKET_NAME` in the command is where a copy of the connector's code will be stored for the serverless application repository to retrieve. This will allow users with permission to do so the ability to deploy instances of the connector via the 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)
5. Deploy the serverless application following the instructions [here](https://docs.aws.amazon.com/serverlessrepo/latest/devguide/serverlessrepo-how-to-consume.html)
6. Connect the serverless application with Athena following the instructions [here](https://docs.aws.amazon.com/athena/latest/ug/connect-to-a-data-source-Lambda.html)

### Limitations
6. Write DDL operations are not supported
7. Any relevant AWS Lambda limits

