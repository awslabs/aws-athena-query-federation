# Amazon Athena Neptune Connector

This connector enables Amazon Athena to communicate with your Neptune Graph Database instance, making your Neptune graph data accessible via SQL.

**Athena Federated Queries are now enabled as GA in the following regions: us-east-1, us-east-2, us-west-2, eu-west-1, ap-northeast-1, ap-south-1, us-west-1, ap-southeast-1, ap-southeast-2, eu-west-2, ap-northeast-2, eu-west-3, ca-central-1, sa-east-1, and eu-central-1. To use this feature, upgrade your engine version to Athena V2 in your workgroup settings. Check documentation here for more details: https://docs.aws.amazon.com/athena/latest/ug/engine-versions.html.**

## Steps to setup the connector

### Setup AWS Glue Catalog

Unlike traditional relational data stores, Neptune graph DB nodes and edges do not have set schema. Each entry can have different fields and data types. While we are investigating the best way to support schema-on-read usecases for this connector, it presently supports retrieving meta-data from the Glue Data Catalog. You need to pre-create the Glue Database and the corresponding Glue tables with required schemas within that database. This allows the connector to populate list of tables available to query within Athena. 

Refer to the sample Glue catalog setup [here](./aws-glue-sample-scripts)

### Deploy the Neptune Athena Connector

Once you have created the glue catalog, follow steps [here](./neptune-connector-setup) to setup the Athena Neptune Connector

## Current Limitations

Here are some of the current limitations of this connector:

1. The connector currently supports only Property Graph model and does not support RDF Graphs yet.
2. The connector does not support edge traversal capabilities that Apache TinkerPop Gremlin supports. 


