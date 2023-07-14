# RDF Glue Data Catalog Setup

### Create AWS Glue Database and Tables manually

To create If you want to create database and tables manually, you can use the sample shell script [here](./manual/sample-cli-rdf.sh) to create a 
Glue Database by the name "graph-database-rdf" with tables airport_rdf and route_rdf corresponding to the Air Routes RDF sample dataset. 

If you're planning to use your own data set instead of the Air Routes sample dataset, then you need to modify the script according to your data structure. 

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
