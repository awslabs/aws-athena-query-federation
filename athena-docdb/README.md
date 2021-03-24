# Amazon Athena DocumentDB Connector

This connector enables Amazon Athena to communicate with your DocumentDB instance(s), making your DocumentDB data accessible via SQL. The also works with any MongoDB compatible endpoint.

**Athena Federated Queries are now enabled as GA in US-East-1 (IAD), US-West-2 (PDX), and US-East-2 (CMH), US-West-1 (SFO), AP-South-1 (BOM), AP-Northeast-1 (NRT), and EU-West-1 (DUB). To use this feature, upgrade your engine version to Athena V2 in your workgroup settings. Check documentation here for more details: https://docs.aws.amazon.com/athena/latest/ug/engine-versions.html.  To enable this feature in other regions, you need to create an Athena workgroup named AmazonAthenaPreviewFunctionality and run any queries attempting to federate to this connector, use a UDF, or SageMaker inference from that workgroup.**

Unlike traditional relational data stores, DocumentDB collections do not have set schema. Each entry can have different fields and data types. While we are investigating the best way to support schema-on-read usecases for this connector, it presently supports two mechanisms for generating traditional table schema information. The default mechanism is for the connector to scan a small number of documents in your collection in order to form a union of all fields and coerce fields with non-overlap data types. This basic schema inference works well for collections that have mostly uniform entries. For more diverse collections, the connector supports retrieving meta-data from the Glue Data Catalog. If the connector sees a database and table which match your DocumentDB database and collection names it will use the corresponding Glue table for schema. We recommend creating your Glue table such that it is a superset of all fields you may want to access from your DocumentDB Collection.

### Parameters

The Amazon Athena DocumentDB Connector exposes several configuration options via Lambda environment variables. More detail on the available parameters can be found below.

1. **spill_bucket** - When the data returned by your Lambda function exceeds Lambda’s limits, this is the bucket that the data will be written to for Athena to read the excess from. (e.g. my_bucket)
2. **spill_prefix** - (Optional) Defaults to sub-folder in your bucket called 'athena-federation-spill'. Used in conjunction with spill_bucket, this is the path within the above bucket that large responses are spilled to. You should configure an S3 lifecycle on this location to delete old spills after X days/Hours.
3. **kms_key_id** - (Optional) By default any data that is spilled to S3 is encrypted using AES-GCM and a randomly generated key. Setting a KMS Key ID allows your Lambda function to use KMS for key generation for a stronger source of encryption keys. (e.g. a7e63k4b-8loc-40db-a2a1-4d0en2cd8331)
4. **disable_spill_encryption** - (Optional) Defaults to False so that any data that is spilled to S3 is encrypted using AES-GMC either with a randomly generated key or using KMS to generate keys. Setting this to false will disable spill encryption. You may wish to disable this for improved performance, especially if your spill location in S3 uses S3 Server Side Encryption. (e.g. True or False)
5. **disable_glue** - (Optional) If present, with any valye, the connector will no longer attempt to retrieve supplemental metadata from Glue.
6. **glue_catalog** - (Optional) Can be used to target a cross-account Glue catalog. By default the connector will attempt to get metadata from its own Glue account.
7. **default_docdb** If present, this DocDB connection string is used when there is not a catalog specific environment variable (as explained below). (e.g. mongodb://<username>:<password>@<hostname>:<port>/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred)

You can also provide one or more properties which define the DocumentDB connection details for the DocumentDB instance(s) you'd like this connector to use. You can do this by setting a Lambda environment variable that corresponds to the catalog name you'd like to use in Athena. For example, if I'd like to query two different DocumentDB instances from Athena in the below queries:

```sql
 select * from "docdb_instance_1".database.table 
 select * from "docdb_instance_2".database.table
 ```

To support these two SQL statements we'd need to add two environment variables to our Lambda function:

1. **docdb_instance_1** - The value should be the DocumentDB connection details in the format of:mongodb://<username>:<password>@<hostname>:<port>/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0
2. **docdb_instance_2** - The value should be the DocumentDB connection details in the format of: mongodb://<username>:<password>@<hostname>:<port>/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0

You can also optionally use SecretsManager for part or all of the value for the preceeding connection details. For example, if I set a Lambda environment variable for  **docdb_instance_1** to be "mongodb://${docdb_instance_1_creds}@myhostname.com:123/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0" the Athena Federation 
SDK will automatically attempt to retrieve a secret from AWS SecretsManager named "docdb_instance_1_creds" and inject that value in place of "${docdb_instance_1_creds}". Basically anything between ${...} is attempted as a secret in SecretsManager. If no such secret exists, the text isn't replaced.


### Setting Up Databases & Tables

To enable a Glue Table for use with DocumentDB, you simply need to have a Glue database and table that matches any DocumentDB Database and Collection that you'd like to supply supplemental metadata for (instead of relying on the DocumentDB Connector's ability to infer schema). The connector's in built schema inference only supports a subset of data types and scans a limited number of documents. You can enable a Glue table to be used for supplemental metadata by setting the below table properties from the Glue Console when editing the Table and database in question. The only other thing you need to do ensure you use the appropriate data types listed in a later section.

1. **docdb-metadata-flag** - Flag indicating that the table can be used for supplemental meta-data by the Athena DocDB Connector. The value is unimportant as long as this key is present in the properties of the table.
  
### Data Types

The schema inference feature of this connector will attempt to infer values as one of the following:

|Apache Arrow DataType|Java/DocDB Type|
|-------------|-----------------|
|VARCHAR|String|
|INT|Integer|
|BIGINT|Long|
|BIT|Boolean|
|FLOAT4|Float|
|FLOAT8|Double|
|TIMESTAMPSEC|Date|
|VARCHAR|ObjectId|
|LIST|List|
|STRUCT|Document|

Alternatively, if you are using Glue for supplimental metadata you can configure the following types:
        
|Glue DataType|Apache Arrow Type|
|-------------|-----------------|
|int|INT|
|bigint|BIGINT|
|double|FLOAT8|
|float|FLOAT4|
|boolean|BIT|
|binary|VARBINARY|
|string|VARCHAR|
|List|LIST|
|Struct|STRUCT|

### Required Permissions

Review the "Policies" section of the athena-docdb.yaml file for full details on the IAM Policies required by this connector. A brief summary is below.

1. S3 Write Access - In order to successfully handle large queries, the connector requires write access to a location in S3. 
2. SecretsManager Read Access - If you choose to store redis-endpoint details in SecretsManager you will need to grant the connector access to those secrets.
3. Glue Data Catalog - Since DocumentDB does not have a meta-data store, the connector requires Read-Only access to Glue's DataCatalog for supplemental table schema information.
4. VPC Access - In order to connect to your VPC for the purposes of communicating with your DocumentDB instance(s), the connector needs the ability to attach/detach an interface to the VPC.
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

To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-docdb dir, run `mvn clean install`.
3. From the athena-docdb dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-docdb` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)


## Performance

The Athena DocumentDB Connector does not current support parallel scans but will attempt to push down predicates as part of its DocumentDB queries.

