# Neptune Athena Connector Example

To get started with the Neptune Athena Connector, follow these steps:

1. Create a Amazon Neptune database cluster, if you do not already have one. Then populate the database with the sample `air routes` dataset. This is available in both Labeled Property Graph (LPG) and Resource Description Framework (RDF) formats. You may load both if you would like to test the connector against both formats. For more, see [neptune-cluster-setup/README.md](neptune-cluster-setup/README.md).
2. The connector requires you to define a table structure in AWS Glue. Follow [aws-glue-sample-scripts/README.md](aws-glue-sample-scripts/README.md) to setup for the `air routes` dataset.
3. Deploy the connector following [neptune-connector-setup/README.md](neptune-connector-setup/README.md). To use both LPG and RDF, deploy two copies of the connector.
   
