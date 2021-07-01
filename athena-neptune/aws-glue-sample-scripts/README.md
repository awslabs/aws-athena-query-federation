
# Setup AWS Glue Catalog

> **NOTE**
>
> Create the Glue database and the corresponding tables within the same AWS Region as your Neptune cluster and where you intend to run this connector Lambda funciton.

<br/>

Each table within the AWS Glue Catalog based database maps to one node/vertex type within your Amazon Neptune Property Graph model. Each Column for a Table maps to one property of the graph node with the corresponding datatypes.

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

<br/>

As an example, if we have a node labelled “country” with properties “type”, “code” and “desc”.  In the Glue database, we will create a table named “country” with columns “type”, “code” and “desc”. Setup data types of the columns based on their data types in the property graph. 

Refer to the diagram below:

![](./assets/connector-propertygraph.png)

### Create AWS Glue Catalog Database and Tables

The sample script [here](./sample-cli-script.sh) creates a Glue Database by the name "graph-database" and tables: airport, country and continent corresponding to the Air Routes Property Graph sample dataset. If you're planning to use your own data set instead of the Air Routes sample dataset, then you need to modify the script according to your data structure. 

Ensure to have the right executable permissions on the script once you download it.

```
chmod 755 sample-cli-scripts.sh
```
Ensure to setup credentials for your AWS CLI to work.

Replace &lt;aws-profile> with the AWS profile name that carries your credentials and replace &lt;aws-region> with AWS region where you are creating the AWS Glue tables which should be the same as your Neptune Cluster's AWS region.

```
./sample-cli-scripts.sh  <aws-profile> <aws-region>
```
If all goes well you now have the Glue Database and Tables that are required for your Athena Neptune Connector setup and you can move on to those steps mentioned [here](../neptune-connector-setup/README.md).