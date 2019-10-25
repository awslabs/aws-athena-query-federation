# Amazon Athena DocumentDB Connector

This connector enables Amazon Athena to communicate with your DocumentDB instance(s), making your DocumentDB data accessible via SQL. The also works with any MongoDB compatible endpoint.

Unlike traditional relational data stores, DocumentDB collections do not have set schema. Each entry can have different fields and data types. While we are investigating the best way to support schema-on-read usecases for this connector, it presently supports two mechanisms for generating traditional table schema information. The default mechanism is for the connector to scan a small number of documents in your collection in order to form a union of all fields and coerce fields with non-overlap data types. This basic schema inference works well for collections that have mostly uniform entries. For more diverse collections, the connector supports retrieving meta-data from the Glue Data Catalog. If the connector sees a database and table which match your DocumentDB database and collection names it will use the corresponding Glue table for schema. We recommend creating your Glue table such that it is a superset of all fields you may want to access from your DocumentDB Collection.

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

### Required Permissions

Review the "Policies" section of the athena-docdb.yaml file for full details on the IAM Policies required by this connector. A brief summary is below.

1. S3 Write Access - In order to successfully handle large queries, the connector requires write access to a location in S3. 
2. SecretsManager Read Access - If you choose to store redis-endpoint details in SecretsManager you will need to grant the connector access to those secrets.
3. Glue Data Catalog - Since DocumentDB does not have a meta-data store, the connector requires Read-Only access to Glue's DataCatalog for supplemental table schema information.
4. VPC Access - In order to connect to your VPC for the purposes of communicating with your DocumentDB instance(s), the connector needs the ability to attach/detach an interface to the VPC.
5. CloudWatch Logs - This is a somewhat implicit permission when deploying a Lambda function but it needs access to cloudwatch logs for storing logs.

### Deploying The Connector

To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-docdb dir, run `mvn clean install`.
3. From the athena-docdb dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-docdb` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

For a direct deployment to Lambda, you can use the below command from the athena-aws-cmdb directory. Be sure to insert your S3 Bucket and Role ARN as indicated.

```bash
aws lambda create-function \
        --function-name athena-docdb \
        --runtime java8 \
        --timeout 900 \
        --memory-size 3008 \
        --zip-file fileb://target/athena-docdb-1.0.jar \
        --handler com.amazonaws.athena.connectors.docdb.DocDBCompositeHandler \
        --role "!!!<LAMBDA_ROLE_ARN>!!!" \
        --environment Variables={spill_bucket=<!!BUCKET_NAME!!>}
```

## Performance

The Athena Redis Connector will attempt to parallelize queries against your Redis instance depending on the type of table you've defined (zset keys vs. prefix keys). 

