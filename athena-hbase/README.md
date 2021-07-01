# Amazon Athena HBase Connector

This connector enables Amazon Athena to communicate with your HBase instance(s), making your HBase data accessible via SQL. 

**Athena Federated Queries are now enabled as GA in us-east-1, us-east-2, us-west-2, eu-west-1, ap-northeast-1, ap-south-1, us-west-1, ap-southeast-1, ap-southeast-2, eu-west-2, ap-northeast-2, eu-west-3, ca-central-1, sa-east-1, and eu-central-1. To use this feature, upgrade your engine version to Athena V2 in your workgroup settings. Check documentation here for more details: https://docs.aws.amazon.com/athena/latest/ug/engine-versions.html.**

Unlike traditional relational data stores, HBase tables do not have set schema. Each entry can have different fields and data types. While we are investigating the best way to support schema-on-read usecases for this connector, it presently supports two mechanisms for generating traditional table schema information. The default mechanism is for the connector to scan a small number of documents in your collection in order to form a union of all fields and coerce fields with non-overlapping data types. This basic schema inference works well for collections that have mostly uniform entries. For more diverse collections, the connector supports retrieving meta-data from the Glue Data Catalog. If the connector sees a Glue database and table which match your HBase namespace and collection names it will use the corresponding Glue table for schema. We recommend creating your Glue table such that it is a superset of all fields you may want to access from your HBase table.

## Usage

### Parameters

The Athena HBase Connector supports several configuration options via Lambda environment variables. More detail on the available parameters can be found below.

1. **spill_bucket** - When the data returned by your Lambda function exceeds Lambda’s limits, this is the bucket that the data will be written to for Athena to read the excess from. (e.g. my_bucket)
2. **spill_prefix** - (Optional) Defaults to sub-folder in your bucket called 'athena-federation-spill'. Used in conjunction with spill_bucket, this is the path within the above bucket that large responses are spilled to. You should configure an S3 lifecycle on this location to delete old spills after X days/Hours.
3. **kms_key_id** - (Optional) By default any data that is spilled to S3 is encrypted using AES-GCM and a randomly generated key. Setting a KMS Key ID allows your Lambda function to use KMS for key generation for a stronger source of encryption keys. (e.g. a7e63k4b-8loc-40db-a2a1-4d0en2cd8331)
4. **disable_spill_encryption** - (Optional) Defaults to False so that any data that is spilled to S3 is encrypted using AES-GMC either with a randomly generated key or using KMS to generate keys. Setting this to false will disable spill encryption. You may wish to disable this for improved performance, especially if your spill location in S3 uses S3 Server Side Encryption. (e.g. True or False)
5. **disable_glue** - (Optional) If present, with any valye, the connector will no longer attempt to retrieve supplemental metadata from Glue.
6. **glue_catalog** - (Optional) Can be used to target a cross-account Glue catalog. By default the connector will attempt to get metadata from its own Glue account.
7. **default_hbase** If present, this HBase connection string (e.g. master_hostname:hbase_port:zookeeper_port) is used when there is not a catalog specific environment variable (as explained below).

You can also provide one or more properties which define the HBase connection details for the HBase instance(s) you'd like this connector to use. You can do this by setting a Lambda environment variable that corresponds to the catalog name you'd like to use in Athena. For example, if I'd like to query two different HBase instances from Athena in the below queries:

```sql
 select * from "hbase_instance_1".database.table 
 select * from "hbase_instance_2".database.table
 ```

To support these two SQL statements we'd need to add two environment variables to our Lambda function:

1. **hbase_instance_1** - The value should be the HBase connection details in the format of: master_hostname:hbase_port:zookeeper_port
2. **hbase_instance_2** - The value should be the HBase connection details in the format of: master_hostname:hbase_port:zookeeper_port

You can also optionally use SecretsManager for part or all of the value for the preceding connection details. For example, if I set a Lambda environment variable for  **hbase_instance_1** to be "${hbase_host_1}:${hbase_master_port_1}:${hbase_zookeeper_port_1}" the Athena Federation SDK will automatically attempt to retrieve a secret from AWS SecretsManager named "hbase_host_1" and inject that value in place of "${hbase_host_1}". It wil do the same for the other secrets: hbase_zookeeper_port_1, hbase_master_port_1. Basically anything between ${...} is attempted as a secret in SecretsManager. If no such secret exists, the text isn't replaced.
To use the Athena Federated Query feature with AWS Secrets Manager, the VPC connected to your Lambda function should have [internet access](https://aws.amazon.com/premiumsupport/knowledge-center/internet-access-lambda-function/) or a [VPC endpoint](https://docs.aws.amazon.com/secretsmanager/latest/userguide/vpc-endpoint-overview.html#vpc-endpoint-create) to connect to Secrets Manager.

### Setting Up Databases & Tables

To enable a Glue Table for use with HBase, you simply need to have a Glue database and table that matches any HBase Namespace and Table that you'd like to supply supplemental metadata for (instead of relying on the HBase Connector's ability to infer schema). The connector's in built schema inference only supports values serialized in HBase as Strings (e.g. String.valueOf(int)). You can enable a Glue table to be used for supplemental metadata by seting the below table properties from the Glue Console when editing the Table in question. The only other thing you need to do ensure you use the appropriate data types and, optionally, HBase column family naming conventions.

1. **hbase-metadata-flag** - Flag indicating that the table can be used for supplemental meta-data by the Athena HBase Connector. The value is unimportant as long as this key is present in the properties of the table.
1. **hbase-native-storage-flag** - This flag toggles the two modes of value serialization supported by the connector. By default (when this field is not present) the connector assumes all values are stored in HBase as strings. As such it will attempt to parse INT, BIGINT, DOUBLE, etc.. from HBase as Strings. If this field is set (the value of the table property doesn't matter, only its presence) on the table in Glue, the connector will switch to 'native' storage mode and attempt to read INT, BIGINT, BIT, and DOUBLE as bytes by using ByteBuffer.wrap(value).getInt(), ByteBuffer.wrap(value).getLong(), ByteBuffer.wrap(value).get(), and ByteBuffer.wrap(value).getDouble().
  
When it comes to setting your columns, you have two choices for how you model HBase column families. The Athena HBase connector supports fully qualified (aka flattened) naming like "family:column" as well as using STRUCTS to model your column families. In the STRUCT model the name of the STRUCT field should match the column family and then any children of that STRUCT should match the names of the columns in that family. Since predicate push down and columnar reads are not yet fully supported for complex types like STRUCTs we recommend against using the STRUCT approach unless your usecase specifically requires the use of STRUCTS. The below image shows how we've configured a table in Glue using a combination of these approaches.
  
  ![Glue Example Image](https://github.com/awslabs/aws-athena-query-federation/blob/master/docs/img/hbase_glue_example.png?raw=true)
  
### Data Types

All HBase values are retrieved as the basic byte type. From there they are converted to one of the below Apache Arrow data types used by the Athena Query Federation SDK based on how you've defined your table(s) in Glue's DataCatalog. If you are not using Glue to supplement your metedata and instead depending on the connector's schema inference capabilities, only a subset of the below data types will be used, namely: BIGINT, FLOAT8, VARCHAR.

|Glue DataType|Apache Arrow Type|
|-------------|-----------------|
|int|INT|
|bigint|BIGINT|
|double|FLOAT8|
|float|FLOAT4|
|boolean|BIT|
|binary|VARBINARY|
|string|VARCHAR|

                
### Required Permissions

Review the "Policies" section of the athena-hbase.yaml file for full details on the IAM Policies required by this connector. A brief summary is below.

1. S3 Write Access - In order to successfully handle large queries, the connector requires write access to a location in S3. 
2. SecretsManager Read Access - If you choose to store HBase endpoint details in SecretsManager you will need to grant the connector access to those secrets.
3. Glue Data Catalog - Since HBase does not have a meta-data store, the connector requires Read-Only access to Glue's DataCatalog for obtaining HBase key to table/column mappings. 
4. VPC Access - In order to connect to your VPC for the purposes of communicating with your HBase instance(s), the connector needs the ability to attach/detach an interface to the VPC.
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

To use the Amazon Athena HBase Connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-hbase dir, run `mvn clean install`.
3. From the athena-hbase dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-hbase` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

## Performance

The Athena HBase Connector will attempt to parallelize queries against your HBase instance by reading each region server in parallel. Predicate Pushdown is performed within the Lambda function and, where possible, push down into HBase using filters.

## License

This project is licensed under the Apache-2.0 License.
