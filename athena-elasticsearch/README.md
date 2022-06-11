# Amazon Athena Elasticsearch Connector

This connector enables Amazon Athena to communicate with your Elasticsearch instance(s) 
making your Elasticsearch data accessible via SQL. This connector will work with Amazon 
Elasticsearch Service as well as any Elasticsearch compatible endpoint configured with 
`Elasticsearch version 7.0` or higher.

**Athena Federated Queries are now enabled as GA in us-east-1, us-east-2, us-west-2, eu-west-1, ap-northeast-1, ap-south-1, us-west-1, ap-southeast-1, ap-southeast-2, eu-west-2, ap-northeast-2, eu-west-3, ca-central-1, sa-east-1, and eu-central-1. To use this feature, upgrade your engine version to Athena V2 in your workgroup settings. Check documentation here for more details: https://docs.aws.amazon.com/athena/latest/ug/engine-versions.html.**

## Nomenclature

This document includes descriptions and explanations using Elasticsearch concepts and
terminology:

* **Domain** - A name this connector uses to associate with the endpoint of your Elasticsearch
 instance and is also used as the database name. For Elasticsearch instances 
 defined within the Amazon Elasticsearch Service, the domain is auto-discoverable. For all
 other instances, a mapping between the domain name and endpoint will need to be provided.
 
* **Index** - A database table defined in your Elasticsearch instance.

* **Mapping** - If an index is a database table, then a mapping is its schema (i.e. definitions 
of fields/attributes). This connector supports metadata retrieval directly from the 
Elasticsearch instance, as well as from the Glue Data Catalog. If the connector finds a Glue 
database and table matching your Elasticsearch domain and index names it will attempt to use it 
for schema definition. We recommend creating your Glue table such that it is a superset of all 
fields defined in your Elasticsearch index.

* **Document** - A record within a database table.

## Parameters

The Amazon Athena Elasticsearch Connector exposes several configuration options via Lambda 
environment variables:

1. **disable_glue** - (Optional) If present, with any value except false, the connector will no longer 
attempt to retrieve supplemental metadata from Glue.

2. **auto_discover_endpoint** - true/false (true is the default value). If you are using Amazon
Elasticsearch Service, having this set to true, allows the connector to auto-discover your 
domains and endpoints by calling the appropriate describe/list APIs on Amazon Elasticsearch.
For any other type of Elasticsearch instance (e.g. self-hosted), the associated domain-endpoints 
must be specified in the **domain_mapping** variable. This also determines which credentials will 
be used to access the endpoint. If **auto_discover_endpoint**=**true**, then AWS credentials will 
be used to authenticate to Elasticsearch. Otherwise, username/password credentials retrieved from 
Amazon Secrets Manager via the **domain_mapping** variable will be used.*

3. **domain_mapping** - Used only when **auto_discover_endpoint**=**false**, 
this is the mapping between the domain names and their associated endpoints. The variable can
accommodate multiple Elasticsearch endpoints using the following format: 
`domain1=endpoint1,domain2=endpoint2,domain3=endpoint3,...` For the purpose of authenticating to 
an Elasticsearch endpoint, this connector supports substitution strings injected with the format 
`${SecretName}:` with username and password retrieved from AWS Secrets Manager (see example 
below).* The colon `:` at the end of the expression serves as a separator from the rest of the 
endpoint.
    ```                        
        Example (using secret elasticsearch-creds): 
            
        movies=https://${elasticsearch-creds}:search-movies-ne...qu.us-east-1.es.amazonaws.com
   
        Will be modified to:
   
        movies=https://myusername@mypassword:search-movies-ne...qu.us-east-1.es.amazonaws.com
    ```
    Each domain-endpoint pair can utilize a different secret. The secret itself must be specified 
    in the format `username@password`. Although, the password may contain embedded `@` signs, the
    first one serves as a separator from the username. It is also important to note that `,` and 
    `=` are used by this connector as separators for the domain-endpoint pairs. Therefor, they 
    should **NOT** be used anywhere inside the stored secret.

4. **query_timeout_cluster** - timeout period (in seconds) for Cluster-Health queries used in the
generation of parallel scans.

5. **query_timeout_search** - timeout period (in seconds) for Search queries used in the retrieval
of documents from an index.

6. **spill_bucket** - When the data returned by your Lambda function exceeds Lambda’s limits,
this is the bucket that the data will be written to for Athena to read the excess from (e.g. 
my_bucket).

7. **spill_prefix** - (Optional) Defaults to sub-folder in your bucket called
'athena-federation-spill'. Used in conjunction with spill_bucket, this is the path within the 
above bucket where large responses spill. You should configure an S3 lifecycle on this 
location to delete old spills after X days/hours.

3. **spill_put_request_headers** - (Optional) This is a JSON encoded map of request headers and values for the s3 putObject request used for spilling. Example: `{"x-amz-server-side-encryption" : "AES256"}`. For more possible headers see: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html

*To use the Athena Federated Query feature with AWS Secrets Manager, the VPC connected to your Lambda function should have [internet access](https://aws.amazon.com/premiumsupport/knowledge-center/internet-access-lambda-function/) or a [VPC endpoint](https://docs.aws.amazon.com/secretsmanager/latest/userguide/vpc-endpoint-overview.html#vpc-endpoint-create) to connect to Secrets Manager.

## Setting Up Databases & Tables

A Glue table can be set up as a supplemental metadata definition source. To enable
this feature, define a Glue database and table that match the domain and index of the source
you are supplementing.

Alternatively, this connector will take advantage of metadata definitions stored in the 
Elasticsearch instance by retrieving the mapping for the specified index. It is worth noting that
Elasticsearch does not have a dedicated array data-type. Any field can contain zero or more 
values so long as they are of the same data-type. If you intend on using Elasticsearch as your 
metadata definition source, you will have to define a **_meta** property in all indices used with 
Athena to indicate which field(s) should be considered a list (array). Failure to do so will 
result in the extraction of only the first element in a list field. When specifying the _meta 
property, field names should be fully qualified for nested JSON structures (e.g. `address.street`, 
where street is a nested field inside an address structure).

``` 
    Example:

    PUT movies/_mapping
    {
      "_meta": {
        "actor": "list",
        "genre": "list"
      }
    }
```

### Data Types

As discussed above, this connector is capable of extracting metadata definitions from either
Glue, or the Elasticsearch instance. Those definitions will be converted to Apache Arrow 
data-types using the following table (see NOTES below):

|**Elasticsearch**|**Apache Arrow**|**Glue** 
|-----------------|----------------|------------------|
|text, keyword, binary|VARCHAR|string|
|long|BIGINT|bigint
|scaled_float|BIGINT|SCALED_FLOAT(...)
|integer|INT|int
|short|SMALLINT|smallint
|byte|TINYINT|tinyint
|double|FLOAT8|double|
|float, half_float|FLOAT4|float|
|boolean|BIT|boolean|
|date, date_nanos|DATEMILLI|timestamp
|JSON structure|STRUCT|STRUCT|
|_meta (see above)|LIST|ARRAY|

NOTES:

* Only the Elasticsearch/Glue data-types listed above are supported for this connector at 
the present time.

* A **scaled_float** is a floating-point number scaled by a fixed double scaling factor and
represented as a **BIGINT** in Arrow (e.g. 0.756 with a scaling factor of 100 is rounded to 76).

* To define a scaled_float in Glue you must select the **array** column type and declare the 
field using the format `SCALED_FLOAT(<scaling_factor>)`.
    
    Examples of valid values:
    ```  
    SCALED_FLOAT(10.51)
    SCALED_FLOAT(100)
    SCALED_FLOAT(100.0)
    ```

    Examples of invalid values:
    ```
    SCALED_FLOAT(10.)
    SCALED_FLOAT(.5)
    ```
* When converting from **date_nanos** to **DATEMILLI**, nanoseconds will be rounded to the 
nearest millisecond. Valid values for date and date_nanos include but are not limited to:
    * "2020-05-18T10:15:30.123456789"
    * "2020-05-15T06:50:01.123Z"
    * "2020-05-15T06:49:30.123-05:00"
    * 1589525370001 (epoch milliseconds)

* An Elasticsearch **binary** is a string representation of a binary value encoded using Base64,
and will be converted to a **VARCHAR**.

## Running Integration Tests

The integration tests in this module are designed to run without the prior need for deploying the connector. Nevertheless,
the integration tests will not run straight out-of-the-box. Certain build-dependencies are required for them to execute correctly.
For build commands and step-by-step instructions on building and running the integration tests see the
[Running Integration Tests](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-integ-test/README.md#running-integration-tests) README section in the **athena-federation-integ-test** module.

In addition to the build-dependencies, certain test configuration attributes must also be provided in the connector's [test-config.json](./etc/test-config.json) JSON file.
For additional information about the test configuration file, see the [Test Configuration](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-integ-test/README.md#test-configuration) README section in the **athena-federation-integ-test** module.

Once all prerequisites have been satisfied, the integration tests can be executed by specifying the following command: `mvn failsafe:integration-test` from the connector's root directory.

## Deploying The Connector

To use this connector in your queries, navigate to AWS Serverless Application Repository and 
deploy a pre-built version of this connector. Alternatively, you can build and deploy this 
connector from source. To do so, follow the steps below, or use the more detailed tutorial in the 
athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-federation-integ-test dir, run `mvn clean install` if you haven't already
   (**Note: failure to follow this step will result in compilation errors**).
3. From the athena-elasticsearch dir, run `mvn clean install`.
4. From the athena-elasticsearch dir, run `../tools/publish.sh S3_BUCKET_NAME athena-elasticsearch` to publish the connector to your 
private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of 
the connector's code will be stored and retrieved by the Serverless Application Repository. This 
will allow users with permission the ability to deploy instances of the connector via a
1-Click form.
4. Navigate to the [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo).

## Performance

The Athena Elasticsearch Connector supports shard-based parallel scans. Using cluster health information
retrieved from the Elasticsearch instance, the connector generates multiple requests (for a document
search query) that are split per shard and run concurrently.

Additionally, the connector will push down predicates as part of its document search queries. The following
example demonstrates this connector's ability to utilize predicate push-down.

**Query:**
```sql
select * from "lambda:elasticsearch".movies.movies
where year >= 1955 and year <= 1962 or year = 1996;
```
**Predicate:**
```
(_exists_:year) AND year:([1955 TO 1962] OR 1996)
```

## Executing SQL Queries

The following are examples of DDL queries you can send with this connector. Note that 
**<function_name>** corresponds to the name of your Lambda function, **domain** is the name of 
the domain you wish to query, and **index** is the name of your index:

```sql
show databases in `lambda:<function_name>`;
show tables in `lambda:<function_name>`.domain;
describe `lambda:<function_name>`.domain.index;
```
