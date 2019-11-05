# Amazon Athena Query Federation SDK



## Features

### DataTypes

## What is a 'Connector'?

A 'Connector' is a piece of code that understands how to execute portions of an Athena query outside of Athena's core engine. Connectors must satisfy a few basic requirements.

1. Your connector must provide a source of meta-data for Athena to get schema information about what databases, tables, and columns your connector has. This is done by building and deploying a lambda function that extends com.amazonaws.athena.connector.lambda.handlers.MetadataHandler in the athena-federation-sdk module. 
2. Your connector must provide a way for Athena to read the data stored in your tables. This is done by building and deploying a lambda function that extends com.amazonaws.athena.connector.lambda.handlers.RecordHandler in the athena-federation-sdk module. 

Alternatively, you can deploy a single Lambda function which combines the two above requirements by using com.amazonaws.athena.connector.lambda.handlers.CompositeHandler or com.amazonaws.athena.connector.lambda.handlers.UnifiedHandler. While breaking this into two separate Lambda functions allows you to independently control the cost and timeout of your Lambda functions, using a single Lambda function can be simpler and higher performance due to less cold start.

In the next section we take a closer look at the methods we must implement on the MetadataHandler and RecordHandler.

### MetadataHandler Details

Lets take a closer look at what is required for a MetadataHandler. Below we have the basic functions we need to implement when using the Amazon Athena Query Federation SDK's MetadataHandler to satisfy the boiler plate work of serialization and initialization. The abstract class we are extending takes care of all the Lambda interface bits and delegates on the discrete operations that are relevant to the task at hand, querying our new data source.

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
      //Return a list of tables (strings) for the requested catalog and schema
    }

    @Override
    protected GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request)
    {
      //Return a table (column names, types, descriptions and table properties)
    }

    @Override
    protected GetTableLayoutResponse doGetTableLayout(BlockAllocator blockAllocator, GetTableLayoutRequest request)
    {
      //Return the partitions of the requested table that need to be read
      //to satisfy the supplied predicate. This is meant to be a fast pruning operation.
      //Source that don't support partitioning can return a single partition. Partitions
      //are opaque to Athena and are just used to call the next method, doGetSplits(...)
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

Alternatively, if you wish to use AWS Glue DataCatalog as the authrotiative (or suplimental) source of meta-data for your connector you can extend com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler instead of com.amazonaws.athena.connector.lambda.handlers.MetadataHandler. GlueMetadataHandler comes with implementations for doListSchemas(...), doListTables(...), and doGetTable(...) leaving you to implemented only 2 methods. The Amazon Athena DocumentDB Connector in the athena-docdb module is an example of using GlueMetadataHandler.

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
       //If you source supports filtering, use the Contraints objects on the request to push the predicate
       //down into your source. You can also use the provided ContrainEvaluator to performing filtering
       //in this code block.
    }
}
```


## Performance Tuning

### Partition Pruning

### Predicate Push-Down

--talk about associative predicates

### Native Apache Arrow

### Throttling & Rate Limiting

If your Lambda function(s) throw a FederationThrottleException, Athena will use that as an indication that your Lambda function(s) or the source they talk to are under too much load and trigger Athena's Additive-Increase/Multiplicative-Decrease based Congestion Control mechanism. Some sources may generate throttling events in the middle of a Lambda invocation, after some data has already been returned. In these cases, Athena can not always automatically apply congestion control because retrying the call may lead to incorrect query results. We recommend using ThrottlingInvoker to handle calls to depedent services in your connector. The ThrottlingInvoker has hooks to see if you've already written rows to the response and thus decide how best to handle a Throttling event either by: sleeping and retrying in your Lamnbda function or by bubbling up a FederationThrottleException to Athena.

You can configure ThrottlingInvoker via its builder or for pre-built connectors like athena-cloudwatch by setting the following environment variables:

1. **throttle_initial_delay_ms** - (Default: 10ms) This is the initial call delay applied after the first congestion event.
1. **throttle_max_delay_ms** - (Default: 1000ms) This is the max delay between calls. You can derive TPS by dividing it into 1000ms.
1. **throttle_decrease_factor** - (Default: 0.5) This is the factor by which we reduce our call rate.
1. **throttle_increase_ms** - (Default: 10ms) This is the rate at which we decrease the call delay.


## License

This project is licensed under the Apache-2.0 License.