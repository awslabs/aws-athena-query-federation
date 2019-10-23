# Amazon Athena DocumentDB Connector

This connector enables Amazon Athena to communicate with your DocumentDB instance(s), making your DocumentDB data accessible via SQL. The also works with any MongoDB compatible endpoint.

Unlike traditional relational data stores, DocumentDB collections do not have set schema. Each entry can have different fields and data types. While we are investigating the best way to support schema-on-read usecases for this connector, it presently supports two mechanisms for generating traditional table schema information. The default mechanism is for the connector to scan a small number of documents in your collection in order to form a union of all fields and coerce fields with non-overlap data types. This basic schema inference works well for collections that have mostly uniform entries. For more diverse collections, the connector supports retrieving meta-data from the Glue Data Catalog. If the connector sees a database and table which match your DocumentDB database and collection names it will use the corresponding Glue table for schema. We recommend creating your Glue table such that it is a superset of all fields you may want to access from your DocumentDB Collection.

## Usage

To use the Amazon Athena DocumentDB Connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps:

1. (pre-requisite) Install maven on your development machine.
2. (pre-requisite) Install the aws cli and sam tool on your development machine.
3. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
3. From the athena-redis dir, run `mvn clean install`.
4. From the athena-redis dir, run `sam package --template-file athena-docdb.yaml --output-template-file packaged.yaml --s3-bucket <your_lambda_source_bucket_name>`
6. Deploy your application using either Server-less Application Repository or Lambda directly. Instructions below.

For Serverless Application Repository, run `sam publish --template packaged.yaml --region <aws_region>` from the athena-redis directory and then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

For a direct deployment to Lambda, you can use the below command from the athena-docdb directory. Be sure to insert you S3 Bucket an Role ARN when indicated. You also need to go to the Lambda console to configure your VPC details so that Lambda function can access your DocumentDB instance(s) as well as S3.

```bash
aws lambda create-function \
        --function-name example-connector \
        --runtime java8 \
        --timeout 900 \
        --memory-size 3008 \
        --environment Variables={spill_bucket=<!!BUCKET_NAME!!>} \
        --zip-file fileb://target/athena-docdb-1.0.jar \
        --handler com.amazonaws.athena.connectors.docdb.DocDBCompositeHandler \
        --role "!!!<LAMBDA_ROLE_ARN>!!!" 
```

### Parameters

The Amazon Athena DocumentDB Connector exposes several configuration options via Lambda environment variables. More detail on the available parameters can be found below.

|Parameter Name|Example Value|Description|
|--------------|--------------------|------------------|
|spill_bucket|my_bucket|When the data returned by your Lambda function exceeds Lambda’s limits, this is the bucket that the data will be written to for Athena to read the excess from.|
|spill_prefix|temporary/split| (Optional) Defaults to sub-folder in your bucket called 'athena-federation-spill'. Used in conjunction with spill_bucket, this is the path within the above bucket that large responses are spilled to. You should configure an S3 lifecycle on this location to delete old spills after X days/Hours.|
|kms_key_id|a7e63k4b-8loc-40db-a2a1-4d0en2cd8331|(Optional) By default any data that is spilled to S3 is encrypted using AES-GCM and a randomly generated key. Setting a KMS Key ID allows your Lambda function to use KMS for key generation for a stronger source of encryption keys.|
|disable_spill_encryption|True or False|(Optional) Defaults to False so that any data that is spilled to S3 is encrypted using AES-GMC either with a randomly generated key or using KMS to generate keys. Setting this to false will disable spill encryption. You may wish to disable this for improved performance, especially if your spill location in S3 uses S3 Server Side Encryption.|
|catalog_id_1|mongo connection string or SecretsManager secret id|This connector allows you to connect to multiple different MongoDB compatible endpoints (e.g. DocumentDB) from a single Lambda function. Each endpoint that you’d like to query should have a parameter that matches the catalog id you wish to use in Athena with the value being the mongodb connection string to use or the id of a ‘secret’ in Secrets Manager which contains the connection string. (e.g. connection string: mongodb://<username>:<password>@<hostname>:<port>/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0|

For example, if I want to deploy an instance of this connector that enabled two different DocumentDB instances in Athena I'd set the following Environment Variable parameters on the Lambda function:

1. spill_bucket=my-athena-spill-bucket
2. docbd_1=mongodb://fakeuser:fakepassword@docdb1.hostname.com:1234
3. docdb_2=docdb_2_secret

Note that param 2 uses an inline connection string with credentials. While Lambda does support encrypting these values we recommend using the approach employed by param 3.  Param 3 specifies a secret which the connector will look up in Secrets Manager.

Then I can run an Athena query like the below:
`select * from docdb_1.database.collection` or `select * from docdb_2.database.collection`

## Performance

The Athena Redis Connector will attempt to parallelize queries against your Redis instance depending on the type of table you've defined (zset keys vs. prefix keys). 

