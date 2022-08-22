
# Setup AWS Glue Catalog

> **NOTE**
>
> Create the Glue database and the corresponding tables within the same AWS Region as your Neptune cluster and where you intend to run this connector Lambda funciton.

<br/>

Each table within the AWS Glue Catalog based database maps to one node/vertex or edge/relatoinship type within your Amazon Neptune Property Graph model. Each Column for a Table maps to one property of the graph node or edge with the corresponding datatypes.

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
|timestamp|DATEMILLI|

<br/>

As an example, if we have a node labelled “country” with properties “type”, “code” and “desc”.  In the Glue database, we will create a table named “country” with columns “type”, “code” and “desc”. Setup data types of the columns based on their data types in the property graph. 

Refer to the diagram below:

![](./assets/connector-propertygraph.png)

## Create AWS Glue Catalog Database and Tables

AWS Glue Catalog Database and Tables can be created either by using [Amazon Neptune Export Configuration](#create-aws-glue-database-and-tables-using-amazon-neptune-export-configuration) or [Manually](#create-aws-glue-database-and-tables-manually). 

### Create AWS Glue Database and Tables using Amazon Neptune Export Configuration

You can use the sample node.js script [here](./automation/script.js) to create a Glue Database by the name "graph-database" and tables: airport, country, continent and route corresponding to the Air Routes Property Graph sample dataset. The node.js script uses the Amazon Neptune export configuration file. There is a sample export configuration for the Air Routes sample dataset in the [folder](./automation).

From inside the [folder](./automation), run these commands

Install dependencies

```
npm install
```

Make sure you have access to your AWS environment via CLI and Execute the script

```
node script.js

```
If you are using a different dataset make sure to replace the config.json with export output from your database. Refer [this](https://github.com/awslabs/amazon-neptune-tools/tree/master/neptune-export) for how to export configuration from Amazon Neptune database.  You have to download the source code and build it. Once you have built the neptune-export jar file, run the below command from machine where your Amazon Neptune cluster is accessible, to generated export configuration

```
bin/neptune-export.sh create-pg-config -e <neptuneclusterendpoint> -d <outputfolderpath>

```

### Create AWS Glue Database and Tables manually


If you want to create database and tables manually, you can use the sample shell script [here](./manual/sample-cli-script.sh) to create a Glue Database by the name "graph-database" and tables: airport, country, continent and route  corresponding to the Air Routes Property Graph sample dataset. 

If you're planning to use your own data set instead of the Air Routes sample dataset, then you need to modify the script according to your data structure. 

Ensure to have the right executable permissions on the script once you download it.

```
chmod 755 sample-cli-script.sh
```
Ensure to setup credentials for your AWS CLI to work.

Replace &lt;aws-profile> with the AWS profile name that carries your credentials and replace &lt;aws-region> with AWS region where you are creating the AWS Glue tables which should be the same as your Neptune Cluster's AWS region.

```
./sample-cli-script.sh  <aws-profile> <aws-region>
```


If all goes well you now have the Glue Database and Tables that are required for your Athena Neptune Connector setup and you can move on to those steps mentioned [here](../neptune-connector-setup/).