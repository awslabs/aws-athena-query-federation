# Amazon Athena Query Federation SDK

The Athena Query Federation SDK defines a set of interfaces and wire protocols that you can implement to enable Athena to delegate portions of it's query execution plan to code that you deploy/write.

This essentially allows you to customize Athena's core execution engine with your own functionality while still taking advantage of Athena's ease of use and fully managed nature.

You can find a collection of ready made modules that allow Athena to connect to various data sources by going to [Serverless Application Repository](https://console.aws.amazon.com/serverlessrepo/). Serverless Application Repository will allow you to search for and 1-Click deploy Athena connectors.
 
 Alternatively, you can explore [the Amazon Athena Query Federation github repository](https://github.com/awslabs/aws-athena-query-federation) for many of those same ready made connectors, modify them as you see fit, or write your own connector using the included example project. 

For those seeking to write their own connectors, we recommend you being by going through the [tutorial in athena-example](https://github.com/awslabs/aws-athena-query-federation/tree/master/athena-example)

## Features

* **Federated Metadata** - It is not always practical to centralize table metadata in a centralized meta-store. As such, this SDK allows Athena to delegate portions of its query planning to your connector in order to retrieve metadata about your data source.
* **Glue DataCatalog Support** - You can optionally enable a pre-built Glue MetadataHandler in your connector which will first attempt to fetch metadata from Glue about any table being queried before given you an opportunitiy to modify or re-write the retrieved metadata. This can be handy when you are using a custom format it S3 or if your data source doesn't have its own source of metadata (e.g. redis). 
* **Federated UDFs** - Athena can delegate calls for batchable Scalar UDFs to your Lambda function, allowing you to write your own custom User Defined Functions.
* **AWS Secrets Manager Integration** - If your connectors need passwords or other sensitive information, you can optionally use the SDK's built in tooling to resolve secrets. For example, if you have a config with a jdbc connection string you can do: "jdbc://${username}:${password}@hostname:port?options" and the SDK will automatically replace ${username} and ${password} with AWS Secrets Manager secrets of the same name. To use the Athena Federated Query feature with AWS Secrets Manager, the VPC connected to your Lambda function should have [internet access](https://aws.amazon.com/premiumsupport/knowledge-center/internet-access-lambda-function/) or a [VPC endpoint](https://docs.aws.amazon.com/secretsmanager/latest/userguide/vpc-endpoint-overview.html#vpc-endpoint-create) to connect to Secrets Manager.
* **Federated Identity** - When Athena federates a query to your connector, you may want to perform Authz based on the identitiy of the entity that executed the Athena Query. 
* **Partition Pruning** - Athena will call you connector to understand how the table being queried is partitioned as well as to obtain which partitions need to be read for a given query. If your source supports partitioning, this give you an opportunity to use the query predicate to perform partition prunning.
* **Parallelized & Pipelined Reads** - Athena will parallelize reading your tables based on the partitioning information you provide. You also have the opportunity to tell Athena how (and if) it should split each partition into multiple (potentially concurrent) read operations. Behind the scenes Athena will parallelize reading the split (work units) you've created and pipeline reads to reduce the performance impact of reading a remote source. 
* **Predicate Pushdown** - (Associative Predicates) Where relevant, Athena will supply you with the associative portion of the query predicate so that you can perform filtering or push the predicate into your source system for even better performance. It is important to note that the predicate is not always the query's full predicate. For example, if the query's predicate was "where (col0 < 1 or col1 < 10) and col2 + 10 < 100" only the "col0 < 1 or col1 < 10" will be supplied to you at this time. We are still considering the best form for supplying connectors with a more complete view of the query and its predicate and expect a future release to provide this to connectors that are capable of utilizing
* **Column Projection** - Where relevant, Athena will supply you with the columns that need to be projected so that you can reduce data scanned.
* **Limited Scans** - While Athena is not yet able to push down limits to you connector, the SDK does expose a mechanism by which you can abandon a scan early. Athena will already avoid scanning partitions and splits that are not needed once a limit, failure, or user cancellation occurs but this functionality will allow connectors that are in the middle of processing a split to stop regardless of the cause. This works even when the query's limit can not be semantically pushed down (e.g. limit happens after a filtered join). In a future release we may also introduce traditional limit pushdwon for the simple cases that would support that.
* **Congestion Control** - Some of the source you may wish to federate to may not be as scalable as Athena or may be running performance sensitive workloads that you wish to protect from an overzealous federated query. Athena will automatically detect congestion by listening for FederationThrottleException(s) as well as many other AWS service exceptions that indicate your source is overwhelmed. When Athena detects congestion it reducing parallelism against your source. Within the SDK you can make use of ThrottlingInvoker to more tightly control congestion yourself. Lastly, you can reduce the concurrency your Lambda functions are allowed to achieve in the Lambda console and Athena will respect that setting.

### DataTypes

The wire protocol between your connector(s) and Athena is built on Apache Arrow with JSON for request/response structures. As such we make use of Apache Arrow's type system. At this time we support the below Apache Arrow types with plans to add more.

The below table lists the supported Apache Arrow types as well as the corresponding java type you can use to 'set' values via Block.setValue(...) or BlockUtils.setValue(...). It is important to remember that while this SDK offers a number of convenience helpers to make working with Apache Arrow easier for the beginner you always have the option of using Apache Arrow directly. Using Arrow Directly can offer improved performance as well as more options for how you handle type conversion and coercion.

|Apache Arrow Data Type|Java Type|
|-------------|-----------------|
|BIT|int, boolean|
|DATEMILLI|Date, long|
|TIMESTAMPMILLITZ|Date, long|
|DATEDAY|Date, long, int|
|FLOAT8|double|
|FLOAT4|float|
|INT|int, long|
|TINYINT|int|
|SMALLINT|int|
|BIGINT|long|
|VARBINARY|byte[]|
|DECIMAL|double, BigDecimal|
|VARCHAR|String, Text|
|STRUCT|Object (w/ FieldResolver)|
|LIST|iterable<Object> (w/Optional FieldResolver)|

UDFs have access to the same type system. When extending UserDefinedFunctionHandler you can expect to recieve the below concrete type mapping. 
 
 | Athena type | Java type                                      |Supported As Partition Col|
 | ----------- | ---------------------------------------------- |----------------------|
 | TIMESTAMP   | java.time.LocalDateTime (UTC)                  |No|
 | DATE        | java.time.LocalDate (UTC)                      |No|
 | TINYINT     | java.lang.Byte                                 |No|
 | SMALLINT    | java.lang.Short                                |No|
 | REAL        | java.lang.Float                                |No|
 | DOUBLE      | java.lang.Double                               |Yes|
 | DECIMAL     | java.math.BigDecimal                           |Yes|
 | BIGINT      | java.lang.Long                                 |Yes|
 | INTEGER     | java.lang.Integer                              |Yes, Int(32)|
 | VARCHAR     | java.lang.String                               |Yes|
 | VARBINARY   | byte[]                                         |No|
 | BOOLEAN     | java.lang.Boolean                              |Yes|
 | ARRAY       | java.util.List                                 |No|
 | ROW         | java.util.Map<String, Object>                  |No|

## What is a 'Connector'?

A 'Connector' is a piece of code that understands how to execute portions of an Athena query outside of Athena's core engine. Connectors must satisfy a few basic requirements.

1. Your connector must provide a source of meta-data for Athena to get schema information about what databases, tables, and columns your connector has. This is done by building and deploying a lambda function that extends or composes com.amazonaws.athena.connector.lambda.handlers.MetadataHandler in the athena-federation-sdk module. 
2. Your connector must provide a way for Athena to read the data stored in your tables. This is done by building and deploying a lambda function that extends or composes com.amazonaws.athena.connector.lambda.handlers.RecordHandler in the athena-federation-sdk module. 

Alternatively, you can deploy a single Lambda function which combines the two above requirements by using com.amazonaws.athena.connector.lambda.handlers.CompositeHandler or com.amazonaws.athena.connector.lambda.handlers.UnifiedHandler. While breaking this into two separate Lambda functions allows you to independently control the cost and timeout of your Lambda functions, using a single Lambda function can be simpler and higher performance due to less cold start.

In the next section we take a closer look at the methods we must implement on the MetadataHandler and RecordHandler.

Included with this SDK is a set of examples in src/com/amazonaws/athena/connector/lambda/examples . You can deploy the examples using the included athena-federation-sdk.yaml file. Run `../tools/publish.sh S3_BUCKET_NAME athena-federation-sdk` to publish the connector to your private AWS Serverless Application Repository. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form in the Serverless Application Repository console. After you've used the Serverless Application Repository UX to deploy and instance of your connection you can run the validation script `../tools/validate_connector.sh <function_name>` be sure to replace <function_name> with the name you gave to your function/catalog when you deployed it via Serverless Application Repository. To ensure you connector is valid before running an Athena query. For detailed steps on building and deploying please view the README.md in the athena-example module of this repository.

You can then run `SELECT count(*) from "lambda:<catalog>"."custom_source"."fake_table" where year > 2010` from your Athena console. Be sure you replace <catalog> with the catalog name you gave your connector when you deployed it.


### MetadataHandler Details

 Below we have the basic functions we need to implement when using the Amazon Athena Query Federation SDK's MetadataHandler to satisfy the boiler plate work of serialization and initialization. The abstract class we are extending takes care of all the Lambda interface bits and delegates only the discrete operations that are relevant to the task at hand, querying our new data source.

All schema names, table names, and column names must be lower case at this time. Any entities that are uppercase or mixed case will not be accessible in queries and will be lower cased by Athena's engine to ensure consistency across sources. As such you may need to handle this when integrating with a source that supports mixed case. As an example, you can look at the CloudwatchTableResolver in the athena-cloudwatch module for one potential approach to this challenge.

```java
public class MyMetadataHandler extends MetadataHandler
{
    @Override
    protected ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest request)
    {
      //Return a list of Schema names (strings) for the requested catalog
    }

    @Override
    protected ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest request)
    {
      //Return a paginated list of tables (strings) for the requested catalog and schema.
      //A complete (un-paginated) list of tables should be returned if the request's pageSize is set to
      //ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE.
    }

    @Override
    protected GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request)
    {
      //Return a table (column names, types, descriptions and table properties)
    }

    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request) {
      //Generates the partitions of the requested table that need to be read
      //to satisfy the supplied predicate. This is meant to be a fast pruning operation.
      //Source that don't support partitioning can return a single partition. Partitions
      //are opaque to Athena and are just used to call the next method, doGetSplits(...)
      //Partition Pruning is automatically handled by BlockWriter which creates
      //Blocks that are constrained to filter out values that do not match
      //the requests constraints. You can optionally get a ConstraintEvaluator
      //from the BlockWriter or get constraints directly from the request if you
      //need to do some customer filtering for performance reasons or to push
      //down into your source system.
    }

    @Override
    protected GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest request)
    {
      //Return the Split(s) that define how reading your the requested table can be parallelized. 
      //Think of this method as a work-producer. Athena will call this paginated API while also
      //scheduling each Split for execution. Sources that don't support parallelism can return
      //a single split. Splits are mostly opaque to Athena and are just used to call your RecordHandler.
    }
}
```

You can find example MetadataHandlers by looking at some of the connectors in the repository. athena-cloudwatch and athena-tpcds are fairly easy to follow along with.

Alternatively, if you wish to use AWS Glue DataCatalog as the authoritative (or supplemental) source of meta-data for your connector you can extend com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler instead of com.amazonaws.athena.connector.lambda.handlers.MetadataHandler. GlueMetadataHandler comes with implementations for doListSchemas(...), doListTables(...), and doGetTable(...) leaving you to implemented only 2 methods. The Amazon Athena DocumentDB Connector in the athena-docdb module is an example of using GlueMetadataHandler.

### RecordHandler Details

Lets take a closer look at what is required for a RecordHandler. Below we have the basic functions we need to implement when using the Amazon Athena Query Federation SDK's MetadataHandler to satisfy the boiler plate work of serialization and initialization. The abstract class we are extending takes care of all the Lambda interface bits and delegates on the discrete operations that are relevant to the task at hand, querying our new data source.

```java
public class MyRecordHandler
        extends RecordHandler
{
    @Override
    protected void readWithConstraint(ConstraintEvaluator constraintEvaluator,
                                      BlockSpiller blockSpiller,
                                      ReadRecordsRequest request)
    {
       //read the data represented by the Split in the request and use the blockSpiller.writeRow() 
       //to write rows into the response. The Amazon Athena Query Federation SDK handles all the 
       //boiler plate of spilling large response to S3, and optionally encrypting any spilled data.
       //If you source supports filtering, use the Constraints objects on the request to push the predicate
       //down into your source. You can also use the provided ConstraintEvaluator to performing filtering
       //in this code block.
    }
}
```

## What is a scalar UDF?

A scalar UDF is a user Defined Function that is applied one row at a time and returns a single column value. Athena will call your scalar UDF with batches of rows (potentially in parallel) in order to limit the performance impact associated with making a remote call for the UDF itself. 

In order for Athena to delegate UDF calls to your Lambda function, you need to implement a UserDefinedFunctionHandler in your Lambda function.  The Athena Query Federation SDK offers an abstract [UserDefinedFunctionHandler](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-sdk/src/main/java/com/amazonaws/athena/connector/lambda/handlers/UserDefinedFunctionHandler.java) which handles all the boiler plate associated serialization and managing the lifecycle of a UDF and leaves you to simply implement the UDF methods themselves. 

### UserDefinedFunctionHandler Details

UDF implementation is a bit different from implementing a connector. Lets say you have the following query you want to run (we'll actually run this query for real later in the tutorial).

```sql
USING 
FUNCTION extract_tx_id(value ROW(id INT, completed boolean) ) RETURNS INT TYPE LAMBDA_INVOKE WITH (lambda_name = 'my_lambda_function'),
FUNCTION decrypt(payload VARCHAR ) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'my_lambda_function')
SELECT year, month, day, account_id, decrypt(encrypted_payload) as decrypted_payload, extract_tx_id(transaction) as tx_id
FROM schema1.table1 WHERE year=2017 AND month=11 AND day=1;
```

This query defined 2 UDFs: extract_tx_id and decrypt which are said to be hosted in a Lambda function called "my_lambda_function". My UserDefinedFunctionHandler would look like the one below. I simply need two methods which match the signature of the UDF I defined in my query. For full data type and method signature info, check the [SDK documentation](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-sdk/README.md).

```java
public class MyUDF extends UserDefinedFunctionHandler
{

    /**
     * This UDF extracts an 'Account' from the input STRUCT (provided as a Map). In this case 'Account' is
     * an application specific concept and very custom to our test dataset's schema.
     *
     * @param transaction The transaction from which to extract the id field.
     * @return An Integer containing the Transaction ID or -1 if the id couldn't be extracted.
     */
    public Integer extract_tx_id(Map<String, Object> transaction){}

    /**
     * Decrypts the provided value using our application's secret key and encryption Algo.
     *
     * @param payload The cipher text to decrypt.
     * @return ClearText version if the input payload, null if the decrypt failed.
     */
    public String decrypt(String payload)

}
```

## Performance

Federated queries may run more slowly than queries which are 100% localized to Athena's execution engine, however much of this is dependent upon the source you are interacting with.
When running a federated query, Athena make use of a deep execution pipeline as well as various data pre-fetch techniques to hide the performance impact of doing remote reads. If
your source supports parallel scans and predicate push-down it is possible to achieve performance that is close to that of native Athena.

To put some real work context around this, we tested this SDK as well as the usage of AWS Lambda by re-creating Athena's S3 + AWS Glue integration as a federated connector. We then 
ran 2 tests using a highly (~3000 files totaling 350GB) parallelizable dataset on S3. The tests were a select count(*) from test_table where our test table had 4 columns of 
primitive types (int, bigint, float4, float8). This query was purposely simple because we wanted to stress test the TABLE_SCAN operation which corresponds very closely to the 
current capabilities of our connector. We expect most workloads, for parallelizable source tables, to bottleneck on other areas of query execution before running into constraints
associated with federated TABLE_SCAN performance.

|Test|GB/Sec|Rows/Sec|
|-------------|-----------------|-------------|
|Federated S3 Query w/Apache Arrow|102 Gbps|1.5B rows/sec|
|Athena + TextCSV on S3 Query|115 Gbps|120M rows/sec|
|Athena + Parquet on S3 Query|30Gbps*|2.7B rows/sec|

*Parquet's run-length encoding makes the GB/sec number somewhat irrelevant for comparison testing but since it is more compact than Apache Arrow it does mean lower network utilization.
**These are not exhaustive tests but rather represent the point at which we stopped validation testing.


### Throttling & Rate Limiting

If your Lambda function(s) throw a FederationThrottleException or if Lambda/EC2 throws a limit exceed exception, Athena will use that as an indication that your Lambda function(s) 
or the source they talk to are under too much load and trigger Athena's Additive-Increase/Multiplicative-Decrease based Congestion Control mechanism. Some sources may generate 
throttling events in the middle of a Lambda invocation, after some data has already been returned. In these cases, Athena can not always automatically apply congestion control 
because retrying the call may lead to incorrect query results. We recommend using ThrottlingInvoker to handle calls to depedent services in your connector. The ThrottlingInvoker 
has hooks to see if you've already written rows to the response and thus decide how best to handle a Throttling event either by: sleeping and retrying in your Lamnbda function or 
by bubbling up a FederationThrottleException to Athena.

You can configure ThrottlingInvoker via its builder or for pre-built connectors like athena-cloudwatch by setting the following environment variables:

1. **throttle_initial_delay_ms** - (Default: 10ms) This is the initial call delay applied after the first congestion event.
1. **throttle_max_delay_ms** - (Default: 1000ms) This is the max delay between calls. You can derive TPS by dividing it into 1000ms.
1. **throttle_decrease_factor** - (Default: 0.5) This is the factor by which we reduce our call rate.
1. **throttle_increase_ms** - (Default: 10ms) This is the rate at which we decrease the call delay.

## License

This project is licensed under the Apache-2.0 License.
