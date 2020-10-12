# Amazon Athena Neptune Connector

This connector enables Amazon Athena to communicate with your Neptune Graph Database instance, making your Neptune graph data accessible via SQL.

**To enable this Preview feature you need to create an Athena workgroup named AmazonAthenaPreviewFunctionality and run any queries attempting to federate to this connector, use a UDF, or SageMaker inference from that workgroup.**

Unlike traditional relational data stores, Neptune graph DB nodes and edges do not have set schema. Each entry can have different fields and data types. While we are investigating the best way to support schema-on-read usecases for this connector, it presently supports retrieving meta-data from the Glue Data Catalog. You need to pre-create the Glue Database and the corresponding Glue tables with required schemas within that database. This allows the connector to populate list of tables available to query within Athena. 

> **NOTE**
>
> Create the Glue database and the corresponding tables within the same AWS Region as your Neptune cluster and where you intend to run this connector Lambda funciton.

Each graph node type is represented as a glue table and node properties are represented as glue table properties with the corresponding datatypes associated with them.

Here's a reference of the Glue DataTypes that you can use:
        
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

<br/>

### Parameters

The Amazon Athena Neptune Connector exposes several configuration options via Lambda environment variables. More detail on the available parameters can be found below.

1. **neptune_endpoint** - The Neptune Cluster Endpoint 
2. **neptune_port** - (Optional) The Neptune Cluster Endpoint port to communicate with. Defaults to 8182
3. **neptune_cluster_res_id** - The Neptune Cluster ResourceID is required to restrict access to specific cluster from the Lambda function within IAM Permissions. To find the Neptune cluster resource ID in the Amazon Neptune AWS Management Console, choose the DB cluster that you want. The Resource ID is shown in the Configuration section.
4. **glue_database_name** - Name of the Glue database that you pre-created.
5. **spill_bucket** - When the data returned by your Lambda function exceeds Lambda’s limits, this is the bucket that the data will be written to for Athena to read the excess from. (e.g. my_bucket)
6. **spill_prefix** - (Optional) Defaults to 'athena-neptune-spill'. Used in conjunction with spill_bucket, this is the path within the above bucket that large responses are spilled to. *You should configure an S3 lifecycle on this location to delete old spills after X days/Hours.*
7. **disable_spill_encryption** - (Optional) Defaults to False so that any data that is spilled to S3 is encrypted using AES-GMC either with a randomly generated key or using KMS to generate keys. Setting this to false will disable spill encryption. You may wish to disable this for improved performance, especially if your spill location in S3 uses S3 Server Side Encryption. (e.g. true or false)

<br/>

### Required Permissions

Review the "Policies" section of the athena-neptune.yaml file for full details on the IAM Policies required by this connector. A brief summary is below.

1. S3 Write Access - In order to successfully handle large queries, the connector requires write access to a location in S3. 
2. Glue Database - Since Neptune does not have a meta-data store, the connector requires Read-Only access to Glue's Database and tables for table schema information.
4. VPC Access - In order to connect to your VPC for the purposes of communicating with your Neptune cluster, the connector needs the ability to attach/detach an interface to the VPC.
5. CloudWatch Logs - This is a somewhat implicit permission when deploying a Lambda function but it needs access to cloudwatch logs for storing logs.
6. Athena GetQueryExecution - The connector uses this access to fast-fail when the upstream Athena query has terminated.
7. Neptune DB - This is to allow access to a specific Neptune cluster based on the provided cluster resource ID.

### Deploying The Connector

To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-neptune dir, run `mvn clean install`.
3. From the athena-neptune dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-neptune` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)


## Current Limitations

Here are some of the current limitations of this connector:

1. The connector currently supports only Property Graph model and does not support RDF Graphs yet.
2. The connector does not support the full graph traversal capabilities that Apache TinkerPop Gremlin supports. 


