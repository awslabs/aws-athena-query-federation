
# Setup AWS Glue Catalog

> **NOTE**
>
> Create the Glue database and the corresponding tables within the same AWS Region as your Neptune cluster and where you intend to run this connector Lambda funciton.

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


### Create sample AWS Glue Catalog tables

```
chmod 755 sample-cli-scripts.sh

```
Replace &lt;aws-profile> with local profile name and replace &lt;aws-region> with aws region where you are creating the AWS Glue tables.

```
./sample-cli-scripts.sh  &lt;aws-profile> &lt;aws-region>

```
