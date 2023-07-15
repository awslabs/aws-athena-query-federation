# RDF Glue Data Catalog Setup

To query RDF data using this connector, create a table in the Glue data catalog that maps to RDF data in the Neptune database. There are styles of mapping available:

- **Class-based**: The table represents an RDFS class. Each row represents an RDF resource whose type of that class. Columns represents datatype or object properties. See the airport_rdf example below.
- **Query-based**: The table represents the resultset of a SPARQL query. Each row is one result. See the route_rdf example below.

In each case, you define columns and use table properties to map RDF to that column structure. Here is a summary of table properties to indicate RDF mapping:

|Property|Values|Description|
|--------|------|-----------|
|componenttype|rdf||
|querymode|class, sparql||
|sparql|SPARQL query to use to find resultset.|Only if querymode='sparql'. Omit prefixes. Define prefixes as table properties.|
|classuri|Class of resources to find|In curie form prefix:classname. Only if querymode='class'. Connector will query for resources whose RDF type is this classuri.|
|subject|Name of column that is the subject in triples.|Only if querymode='class'. Connector will query for resources whose RDF type is this classuri. In that query. THiS IS THE SUBJECT.|
|preds_prefix|Prefix for predicates to find|Only if querymode='class'. If that prefix is P, you must define property prefix_P. For each resource, the connector finds column values as objects of predicates preds_prefix:colname|
|prefix_|Default prefix for query| URI prefix without angled brackets|
|prefix_X|Prefix known by shortform X| URI prefix without angled brackets|
|strip_uri|true, false|Only only localname of URIs in resultset|

## Examples
We provide examples of both class-based and query-based tables. The examples use the Air Routes dataset. 

### Step 1: Seed Air Routes Data in Neptune
In your Neptune cluster, seed that dataset as RDF using the instructions in [../neptune-connector-setup/README.md](../neptune-connector-setup/README.md). TODO .. is that link correct.

### Step 2: Create Glue Tables
Then create the Glue tables. We provide a shell script [manual/sample-cli-script.sh](manual/sample-cli-script.sh). 

Ensure to have the right executable permissions on the script once you download it.

```
chmod 755 sample-cli-script.sh
```
Ensure to setup credentials for your AWS CLI to work.

Replace &lt;aws-profile> with the AWS profile name that carries your credentials and replace &lt;aws-region> with AWS region where you are creating the 
AWS Glue tables which should be the same as your Neptune Cluster's AWS region.

```
./sample-cli-script.sh  <aws-profile> <aws-region>

```
TODO .. some pictures of the tables...

### Step 3: Understanding Class-Based Tables
The **airport_rdf** table is a class-based table. Its rows represent individual RDF resources that have a specified RDF type. The column names represent predicates. The column values represent objects. 

We set the table properties as follows:
- componenttype:rdf
- querymode: class
- classuri: class:Airport
- subject: id
- preds_prefix: prop
- prefix_class: http://kelvinlawrence.net/air-routes/class/
- prefix_prop: http://kelvinlawrence.net/air-routes/datatypeProperty/

We set componenttype to rdf to indicate this is an RDF-based table. We set querymode to class to indicate the RDF mapping is classed-based. We indicate the class using classuri. The value is given in CURIR form as class:Airport. Here class is a prefix. The full value is deinfed by the prefix_class property. We can see that the fully-qualified class URI is http://kelvinlawrence.net/air-routes/class/Airport.

TODO ... 


It is based on RDF class class:Airport where class is a prefix for http://kelvinlawrence.net/air-routes/class/. 
Each row in the table has triples whose subject is the id column. The remaining columns are objects of preficates preds_prefix:type, preds_prefix:code, 
and preds_prefix:icao, where preds_prefix is http://kelvinlawrence.net/air-routes/datatypeProperty/. 

    
Next, I define a table airport_route with columns incode, outcode, dist. I obtain results using custom SPARQL

Set table properties as follows:

- componenttype:rdf
- querymode: sparql
- sparql: select ?incode ?outcode ?dist where { ?resin op:route ?resout . GRAPH ?route { ?resin op:route ?resout } . ?route prop:dist ?dist . ?resin prop:code ?incode .?resout prop:code ?outcode . }
- prefix_op: http://kelvinlawrence.net/air-routes/objectProperty/
- prefix_prop: http://kelvinlawrence.net/air-routes/datatypeProperty/

- - Explaining the airport_rdf. How GENERALLY to do this

- select distinct ?p where {?s rdf:type $MYCLASS . ?s ?p ?o } LIMIT 100
- 
- Explaining route_rdf inc. the funny named graph stuff.  




### Step 4: Deploy Connector

### Step 5; Query


