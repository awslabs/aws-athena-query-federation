
# Setup AWS Glue Catalog

> **NOTE**
>
> Create the Glue database and the corresponding tables within the same AWS Region as your Neptune cluster and where you intend to run this connector Lambda funciton.

<br/>

> **Case sensitive graph labels**
>
> When creating a Glue Table for Neptune, users should specify the following Glue Table parameter if their table name has casing (since Glue only supports lowercased table names):
    "glabel" = &lt;GraphLabelName&gt;
<br/>


> **New feature support - Custom Schema/Gremlin Query**
>
> You can now create a custom schema and specify a gremlin query to fetch the data for the schema from the graph databases. This enhancement allows users to specify a gremlin query instead of doing table join operations to retrieve data. You can also put a limit on number of records you want to retrieve.  Go to custom query example section for details

> **New feature support - RDF support**
>
> You can now query RDF data using the connector



<br/>
Each table within the AWS Glue Catalog based database maps to:

- One node/vertex or edge/relationship type within your Amazon Neptune Property Graph model
- One resource or one SPARQL query result within your Amazon Neptune RDF model

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

<br/>

For more on creating tables for property graph data, see [PropertyGraph.md](PropertyGraph.md).
For more on creating tables for RDF data, see [RDF.md](RDF.md).



