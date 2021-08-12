# Amazon Athena Neptune Connector

This connector enables Amazon Athena to communicate with your Neptune Graph Database instance, making your Neptune graph data accessible via SQL.

**Athena Federated Queries are now enabled as GA in the following regions: us-east-1, us-east-2, us-west-2, eu-west-1, ap-northeast-1, ap-south-1, us-west-1, ap-southeast-1, ap-southeast-2, eu-west-2, ap-northeast-2, eu-west-3, ca-central-1, sa-east-1, and eu-central-1. To use this feature, upgrade your engine version to Athena V2 in your workgroup settings. Check documentation here for more details: https://docs.aws.amazon.com/athena/latest/ug/engine-versions.html.**

## Steps to setup the connector

### Setup Neptune Cluster (Optional)
You can skip this step if you’ve an existing Amazon Neptune cluster and property graph dataset in it that you would like to use. Ensure to have an Internet Gateway and NAT Gateway in the VPC hosting your Neptune cluster and the private subnets in which the Amazon Athena Neptune Connector Lambda function will be running should have route to the internet via this NAT Gateway. This NAT Gateway will be later on used by the Amazon Athena Neptune Connector Lambda function to talk to AWS Glue.

For detailed instructions on setting up a new Neptune cluster and loading the sample property graph air routes dataset into it, follow the steps mentioned [here](./docs/neptune-cluster-setup).
### Setup AWS Glue Catalog

Unlike traditional relational data stores, Neptune graph DB nodes and edges do not have set schema. Each entry can have different fields and data types. While we are investigating the best way to support schema-on-read usecases for this connector, it presently supports retrieving meta-data from the Glue Data Catalog. You need to pre-create the Glue Database and the corresponding Glue tables with required schemas within that database. This allows the connector to populate list of tables available to query within Athena. 

Refer to the sample Glue catalog setup [here](./docs/aws-glue-sample-scripts).

### Deploy the Neptune Athena Connector

Once you have created the glue catalog, follow steps [here](./docs/neptune-connector-setup) to setup the Athena Neptune Connector

Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-neptune dir, run `mvn clean install`.
3. From the athena-neptune dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-neptune` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

## Current Limitations

Here are some of the current limitations of this connector:

1. The connector currently supports only Property Graph model and does not support RDF Graphs yet.
2. The connector does not support edge traversal capabilities that Apache TinkerPop Gremlin supports. 


