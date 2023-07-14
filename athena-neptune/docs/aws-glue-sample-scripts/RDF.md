# RDF Glue Data Catalog Setup

To query RDF data using this connector, create a table in the Glue data catalog that maps to RDF data in the Neptune database. There are styles of mapping available:

- Class-based: The table represents an RDFS class. Each row represents an RDF resource whose type of that class. Columns represents datatype or object properties. See the airport_rdf example below.
- Query-based: The table represents the resultset of a SPARQL query. Each row is one result. See the route_rdf example below.
  
## Create AWS Glue Database and Tables manually
We provide two example tables. Run the sample shell script [here](./manual/sample-cli-rdf.sh) to create a Glue database and tables corresponding to the air routes dataset. 

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

If all goes well you now have the Glue Database and Tables that are required for your Athena Neptune Connector setup and 
you can move on to those steps mentioned [here](../neptune-connector-setup/).

TODO .. how did I arrive at this.

- Explaining the airport_rdf. How GENERALLY to do this

- select distinct ?p where {?s rdf:type $MYCLASS . ?s ?p ?o } LIMIT 100
- 
- Explaining route_rdf inc. the funny named graph stuff.  

## Then deploy the connector

## Then run queries in Athena such as ...


