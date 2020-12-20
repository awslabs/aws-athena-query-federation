# Amazon Athena Cloudwatch Metrics Connector

This connector enables Amazon Athena to communicate with Cloudwatch Metrics, making your metrics data accessible via SQL. 

**Athena Federated Queries are now enabled as GA in US-East-1 (IAD), US-West-2 (PDX), and US-East-2 (CMH), US-West-1 (SFO), AP-South-1 (BOM), AP-Northeast-1 (NRT), and EU-West-1 (DUB). To use this feature, upgrade your engine version to Athena V2 in your workgroup settings. Check documentation here for more details: https://docs.aws.amazon.com/athena/latest/ug/engine-versions.html.  To enable this feature in other regions, you need to create an Athena workgroup named AmazonAthenaPreviewFunctionality and run any queries attempting to federate to this connector, use a UDF, or SageMaker inference from that workgroup.**

## Usage

### Parameters

The Athena Cloudwatch Metrics Connector exposes several configuration options via Lambda environment variables. More detail on the available parameters can be found below.

1. **spill_bucket** - When the data returned by your Lambda function exceeds Lambda’s limits, this is the bucket that the data will be written to for Athena to read the excess from. (e.g. my_bucket)
2. **spill_prefix** - (Optional) Defaults to sub-folder in your bucket called 'athena-federation-spill'. Used in conjunction with spill_bucket, this is the path within the above bucket that large responses are spilled to. You should configure an S3 lifecycle on this location to delete old spills after X days/Hours.
3. **kms_key_id** - (Optional) By default any data that is spilled to S3 is encrypted using AES-GCM and a randomly generated key. Setting a KMS Key ID allows your Lambda function to use KMS for key generation for a stronger source of encryption keys. (e.g. a7e63k4b-8loc-40db-a2a1-4d0en2cd8331)
4. **disable_spill_encryption** - (Optional) Defaults to False so that any data that is spilled to S3 is encrypted using AES-GMC either with a randomly generated key or using KMS to generate keys. Setting this to false will disable spill encryption. You may wish to disable this for improved performance, especially if your spill location in S3 uses S3 Server Side Encryption. (e.g. True or False)

The connector also supports AIMD Congestion Control for handling throttling events from Cloudwatch via the Athena Query Federation SDK's ThrottlingInvoker construct. You can tweak the default throttling behavior by setting any of the below (optional) environment variables:

1. **throttle_initial_delay_ms** - (Default: 10ms) This is the initial call delay applied after the first congestion event.
1. **throttle_max_delay_ms** - (Default: 1000ms) This is the max delay between calls. You can derive TPS by dividing it into 1000ms.
1. **throttle_decrease_factor** - (Default: 0.5) This is the factor by which we reduce our call rate.
1. **throttle_increase_ms** - (Default: 10ms) This is the rate at which we decrease the call delay.


### Databases & Tables

The Athena Cloudwatch Metrics Connector maps your Namespaces, Dimensions, Metrics, and Metric Values into two tables in a single schema called "default".

1. **metrics** - This table contains the available metrics as uniquely defined by a triple of namespace, set<dimension>, name. More specifically, this table contains the following columns.

  * **namespace** - A VARCHAR containing the namespace.
  * **metric_name** - A VARCHAR containing the metric name.
  * **dimensions** - A LIST of STRUCTS comprised of dim_name (VARCHAR) and dim_value (VARCHAR).
  * **statistic** - A List of VARCH statistics (e.g. p90, AVERAGE, etc..) avialable for the metric. 

1. **metric_samples** - This table contains the available metric samples for each metric named in the **metrics** table. More specifically, the table contains the following columns:
  * **namespace** - A VARCHAR containing the namespace.
  * **metric_name** - A VARCHAR containing the metric name.
  * **dimensions** - A LIST of STRUCTS comprised of dim_name (VARCHAR) and dim_value (VARCHAR).
  * **dim_name** - A VARCHAR convenience field used to easily filter on a single dimension name.
  * **dim_value** - A VARCHAR convenience field used to easily filter on a single dimension value.
  * **period** - An INT field representing the 'period' of the metric in seconds. (e.g. 60 second metric)
  * **timestamp** - A BIGINT field representing the epoch time (in seconds) the metric sample is for.
  * **value** - A FLOAT8 field containing the value of the sample.
  * **statistic** - A VARCHAR containing the statistic type of the sample. (e.g. AVERAGE, p90, etc..)

### Required Permissions

Review the "Policies" section of the athena-cloudwatch-metrics.yaml file for full details on the IAM Policies required by this connector. A brief summary is below.

1. S3 Write Access - In order to successfully handle large queries, the connector requires write access to a location in S3. 
2. Cloudwatch Metrics ReadOnly - The connector uses this access to query your metrics data.
2. Cloudwatch Logs Write - The connector uses this access to write its own diagnostic logs.
1. Athena GetQueryExecution - The connector uses this access to fast-fail when the upstream Athena query has terminated.

### Deploying The Connector

To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-cloudwatch-metrics dir, run `mvn clean install`.
3. From the athena-cloudwatch-metrics dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-cloudwatch-metrics` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)
4. Try running a query like the one below in Athena: 
```sql
-- Get the list of available metrics
select * from "lambda:<CATALOG_NAME>"."default".metrics limit 100

-- Query the last 3 days of AWS/Lambda Invocations metrics
SELECT * 
FROM   "lambda:<CATALOG_NAME>"."default".metric_samples 
WHERE  metric_name = 'Invocations' 
       AND namespace = 'AWS/Lambda' 
       AND statistic IN ( 'p90', 'Average' ) 
       AND period = 60 
       AND timestamp BETWEEN To_unixtime(Now() - INTERVAL '3' day) AND 
                             To_unixtime(Now()) 
LIMIT  100; 
```

## Performance

The Athena Cloudwatch Metrics Connector will attempt to parallelize queries against Cloudwatch Metrics by parallelizing scans of the various metrics needed for your query. Predicate Pushdown is performed within the Lambda function and also within Cloudwatch Logs for certain time period , metric, namespace, and dimension filters.

## License

This project is licensed under the Apache-2.0 License.