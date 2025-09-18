# Setup AWS Glue Catalog

Although Neptune is a graph database, the Neptune Athena Connector presents results of SQL queries through Athena in tabular form. You must define the tabular structure using the Glue Data Catalog. Each table defines the columns of the table and how to map graph data to that structure.

We support the following datatypes for table columns:
        
|Glue DataType|Apache Arrow Type|
|-------------|-----------------|
|int|INT|
|bigint|BIGINT|
|double|FLOAT8|
|float|FLOAT4|
|boolean|BIT|
|binary|VARBINARY|
|string|VARCHAR|
|timestamp|DATEMILLI|

> NOTE: A datatype of `array` is not supported with the Athena Federated Query connector for Amazon Neptune.  For multi-valued properties, include the data type of the resident values (one of the data types in the table above).  If the multi-valued property is a set of strings, a semi-colon (`;`) delimited string of all values will be returned.  If the multi-valued property is a set of numeric or datetime values, the first value in the set will be returned.

## CloudFormation Template for Air Routes

To use the `air routes` dataset example, download the CloudFormation template [cfn/cfn_nac_glue.yaml](cfn/cfn_nac_glue.yaml). In the CloudFormation console, create a stack based on this template. Accept defaults and wait for stack to complete. It creates two Glue databases with several tables. 

The `graph-database` Glue database has tables `airport`, `country`, `continent`, `route`, and `customairport`. These tables put structure around the property graph representation of the `air routes` dataset.

The `graph-database-rdf` Glue database has tables `airport_rdf`, `route_rdf`, and `route_rdf_nopfx`. These tables put structure around the RDF representation of the `air routes` dataset.

## Understanding Graph-Table Mapping Through Glue Catalog

See [PropertyGraph.md](PropertyGraph.md) for more on how to map property graph to tables.

See [RDF.md](RDF.md) for more on how to map property graph to tables.

