# Amazon Athena Elasticsearch Connector

This connector enables Amazon Athena to communicate with your Elasticsearch instance(s) 
making your Elasticsearch data accessible via SQL. This connector will work with Amazon 
Elasticsearch Service as well as any Elasticsearch compatible endpoint.

To enable this Preview feature you need to create an Athena workgroup named 
AmazonAthenaPreviewFunctionality and run any queries attempting to federate to this 
connector, use a UDF, or SageMaker inference from that workgroup.

## Nomenclature

This document includes descriptions and explanations using Elasticsearch concepts and
terminology:

* **Domain** - A name Amazon Athena uses to associate with the endpoint of your Elasticsearch
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

The Amazon Athena DocumentDB Connector exposes several configuration options via Lambda 
environment variables:

1. **disable_glue** - (Optional) If present, with any value, the connector will no longer 
attempt to retrieve supplemental metadata from Glue.

2. **auto_discover_endpoint** - true/false (true is the default value). For Amazon 
Elasticsearch Service the domain-names and their associated endpoints are auto-discoverable.
For any other service, the associated domain-endpoints must be specified in the **domain_mapping** 
variable. This also determine which credentials will be used to access the endpoint. If 
**auto_discover_endpoint**=**true**, then AWS credentials will be used. Otherwise, 
username/password credentials retrieved from Amazon Secrets Manger via the **secret_name** 
variable will be used.

3. **domain_mapping** - Used only when **auto_discover_endpoint**=**false**, 
this is the mapping between the domain names and their associated endpoints (in the format:
`domain1=endpoint1,domain2=endpont2`). 

    ```                        
        Example: 
            
        movies=search-movies-ne...qu.us-east-1.es.amazonaws.com
    ```

4. **secret_name** - Used only when **auto_discover_endpoint**=**false** to retrieve the 
username/password credentials from Amazon Secrets Manager in order to connect to the
Elasticsearch instance. The username and password can be stored in Secrets Manager  using the 
following formats:
    
    * Two Key-Value Pairs:
    `"username": <username>, "password": <password>`
    * Single Key-Value: `<username>: <password>`
    
5. **spill_bucket** - When the data returned by your Lambda function exceeds Lambda’s limits, 
this is the bucket that the data will be written to for Athena to read the excess from. (e.g. 
my_bucket)

6. **spill_prefix** - (Optional) Defaults to sub-folder in your bucket called 
'athena-federation-spill'. Used in conjunction with spill_bucket, this is the path within the 
above bucket where large responses spill. You should configure an S3 lifecycle on this 
location to delete old spills after X days/hours.

## Setting Up Databases & Tables

Elasticsearch does not have a dedicated array datatype. Any field can contain zero or more 
values so long as they are of the same datatype. If you intend on using Elasticsearch as your 
metadata definition source, you will have to define a **_meta** field in all indices used with 
Athena to indicate which field(s) should be considered a list (array). The field names should be 
fully qualified for nested JSON structures (e.g. `address.street`, where street is a nested field 
inside an address structure).

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

Alternatively, a Glue table can be set up as a supplemental metadata definition source. To enable
this feature, define a Glue database and table that match the domain and index of the source
you are trying to supplement.

### Data Types

As discussed above, the schema inference feature of this connector will attempt to infer the
indices' metadata definitions from either the Elasticsearch instance, or the Glue Data Catalog 
and convert them to Apache Arrow data-types using the following table (see NOTES below):

|**Elasticsearch**|**Apache Arrow**|**Glue** 
|----------------|-----------------|------------------|
|text, keyword, binary|VARCHAR|string|
|long, scaled_float|BIGINT|bigint
|integer|INT|int
|short|SMALLINT|smallint
|byte|TINYINT|tinyint
|double|FLOAT8|double|
|float, half_float|FLOAT4|float|
|boolean|BIT|boolean|
|data, date_nanos|DATEMILLI|timestamp
|JSON structure|STRUCT|STRUCT|
|_meta (see above)|LIST|ARRAY|

NOTES:

* Only the Elasticsearch/Glue data-types listed above are supported for this connector at 
the present time.

* When converting from **date_nanos** to **DATEMILLI**, nanoseconds will be rounded to the 
nearest millisecond. Valid values for date and date_nanos include but are not limited to:
    * "2020-05-18T10:15:30.123456789"
    * "2020-05-15T06:50:01.123Z"
    * "2020-05-15T06:49:30.123-05:00"
    * 1589525370001 (epoch milliseconds)

* A **scaled_float** is a floating-point number represented as a **BIGINT**, and scaled by a fixed double 
scaling factor (e.g. 0.756 with a scaling factor of 100 is rounded to 76).

* An Elasticsearch **binary** is a string representation of a binary value encoded using Base64,
and will be converted to a **VARCHAR**.

## Deploying The Connector

To use this connector in your queries, navigate to AWS Serverless Application Repository and 
deploy a pre-built version of this connector. Alternatively, you can build and deploy this 
connector from source follow the below steps or use the more detailed tutorial in the 
athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-elasticsearch dir, run `mvn clean install`.
3. From the athena-elasticsearch dir, run `../tools/publish.sh S3_BUCKET_NAME athena-elasticsearch` to publish the connector to your 
private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of 
the connector's code will be stored for Serverless Application Repository to retrieve it. This 
will allow users with permission to do so, the ability to deploy instances of the connector via 
1-Click form. Then navigate to 
[Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

## Performance

The Athena Elasticsearch Connector does not currently support parallel scans but will attempt 
to push down predicates as part of its document search queries.

## Executing SQL Queries

The following are examples of DDL queries you can send with this connector. Note that 
**<function_name>** corresponds to the name of your Lambda function, **domain** is the name of 
the domain you wish to query, and **index** is the name of your index:

```sql
show databases in `lambda:<function_name>`;
show tables in `lambda:<function_name>`.domain;
describe `lambda:<function_name>`.domain.index;
```

The following example demonstrates this connector's ability to utilize predicate push-down. The
predicate pushed down in this query is `(_exists_:year) AND year:((>=1955 AND <=1962) OR 1996)`:

```sql
select * from "lambda:elasticsearch".movies.movies
where year >= 1955 and year <= 1962 or year = 1996;
```

Results:

|actor|year|director|genre|title
|-----|----|--------|-----|-----|
|Lansbury, Angela, Sinatra, Frank, Leigh, Janet, Harvey, Laurence, Silva, Henry, Frees, Paul, Gregory, James, Bissell, Whit, McGiver, John, Parrish, Leslie, Edwards, James, Flowers, Bess, Dhiegh, Khigh, Payne, Julie, Kleeb, Helen, Gray, Joe, Nalder, Reggie, Stevens, Bert, Masters, Michael, Lowell, Tom|1962|Frankenheimer, John|Drama, Mystery, Thriller, Crime|The Manchurian Candidate
|Hopper, Dennis, Wood, Natalie, Dean, James, Mineo, Sal, Backus, Jim, Platt, Edward, Ray, Nicholas, Hopper, William, Allen, Corey, Birch, Paul, Hudson, Rochelle, Doran, Ann, Hicks, Chuck, Leigh, Nelson, Williams, Robert, Wessel, Dick, Bryar, Paul, Sessions, Almira, McMahon, David, Peters Jr., House|1955|Ray, Nicholas|Drama, Romance|Rebel Without a Cause
|Jack Nicholson, Pierce Brosnan, Sarah Jessica Parker|1996|Burton, Tim|Comedy, Sci-Fi|Mars Attacks!
