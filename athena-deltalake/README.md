## Athena DeltaLake connector

This connector enables Amazon Athena to interpret Delta Lake tables (
see [DeltaLake](https://docs.delta.io/latest/index.html)) stored in S3 without the need of generating manifest files or
declaring tables in an external catalog (e.g Glue). The schema, the partitions and the content of the tables are
directly interpreted from the [Delta transaction log](https://github.com/delta-io/delta/blob/master/PROTOCOL.md).

## Usage

### Parameters

The Athena DeltaLake Connector exposes several configuration options via Lambda environment variables. More detail on
the available parameters can be found below.

1. **data_bucket** - This is the bucket that contains all your Delta tables. Databases should be defined at the root of
   this bucket.
1. **spill_bucket** - When the data returned by your Lambda function exceeds Lambda’s limits, this is the bucket that
   the data will be written to for Athena to read the excess from. (e.g. my_bucket)
2. **spill_prefix** - (Optional) Defaults to sub-folder in your bucket called 'athena-federation-spill'. Used in
   conjunction with spill_bucket, this is the path within the above bucket that large responses are spilled to. You
   should configure an S3 lifecycle on this location to delete old spills after X days/Hours.
3. **kms_key_id** - (Optional) By default any data that is spilled to S3 is encrypted using AES-GCM and a randomly
   generated key. Setting a KMS Key ID allows your Lambda function to use KMS for key generation for a stronger source
   of encryption keys. (e.g. a7e63k4b-8loc-40db-a2a1-4d0en2cd8331)
4. **disable_spill_encryption** - (Optional) Defaults to False so that any data that is spilled to S3 is encrypted using
   AES-GMC either with a randomly generated key or using KMS to generate keys. Setting this to false will disable spill
   encryption. You may wish to disable this for improved performance, especially if your spill location in S3 uses S3
   Server Side Encryption. (e.g. True or False)

### Data Types

This connector handles
these [Delta primitive types](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types):

|Delta DataType|Apache Arrow Type|
|-------------|-----------------|
|string|VARCHAR|
|long|BIGINT|	
|integer|INT|	
|short|SMALLINT|	
|byte|TINYINT|	
|float|FLOAT4|	
|double|FLOAT8|	
|decimal|DECIMAL|	
|boolean|BIT|	
|binary|VARBINARY|	
|date|DATEDAY|	
|timestamp|DATEMILLI|

And also complex types: Struct, Array, Map and complex types of complex types, like Array of Structs etc. For now only.
The only unsupported complexity is complex type of Map(it will not break, but will return nulls)

### Required Permissions

Review the "Policies" section of the athena-deltalake.yaml file for full details on the IAM Policies required by this
connector. A brief summary is below.

1. S3 Write Access - In order to successfully handle large queries, the connector requires write access to a location in
   S3.
2. Athena GetQueryExecution - The connector uses this access to fast-fail when the upstream Athena query has terminated.

### Deploying The Connector

To use the Amazon Athena Deltalake Connector in your queries, navigate to AWS Serverless Application Repository and
deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow
the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-deltalake dir, run `AWS_REGION=eu-west-1 mvn clean install`.
3. From the athena-deltalake dir, run  `AWS_REGION=eu-west-1 ../tools/publish.sh S3_BUCKET_NAME athena-deltalake` to
   publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a
   copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow
   users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate
   to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

## Limitations

- The bucket organization should be as follow:
  ```
  my-bucket | my-database-1 | my-table-1
                            | my-table-2
            | my-database-2 | my-table-1
                            | my-table-2
  ```
- List tables/databases will only work if a file with the same name as the table/database and a `_$folder$` suffix
  exists next to the folder
  (see [here](https://aws.amazon.com/premiumsupport/knowledge-center/emr-s3-empty-files))
  ```
  my-bucket | my-database-1          | my-table-1
                                     | my-table-1_$folder$
            | my-database-1_$folder$
  ```
- This implementation does not handle corrupted delta log multi part checkpoints (
  see [multi-part checkpoints](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoints))
- This implementation does not handle partitions of Binary type (as stated in
  the [protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization))
- Database and table name folders in S3 MUST be in lower case
- Complex type of Map, e.g. Array of Maps is unsupported(it will not break, but will return nulls). Strange thing is
  that it works in tests, but not in AWS Athena directly, so not sure if that is a connector or AWS Athena limitation

## ToDo

- deduplication and code cleansing(after resolving nested maps issue)

## License

This project is licensed under the Apache-2.0 License.
