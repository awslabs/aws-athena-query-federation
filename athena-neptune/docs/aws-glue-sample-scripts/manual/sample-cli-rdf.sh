#!/bin/bash

# id is the reserved column for vertex tables
# id, from, to are the reserved columns for edge tables

echo $1;
echo $2;
echo $3;

dbname='graph-database-rdf'


aws glue create-database \
--database-input "{\"Name\":\"${dbname}\"}" \
--profile $1 \
--endpoint https://glue.$2.amazonaws.com \
--region $3
    
aws glue create-table \
    --database-name $dbname \
    --table-input  '{"Name":"airport_rdf", "StorageDescriptor":{ 
        "Columns":[ 
            {"Name":"id", "Type":"string"}, 
            {"Name":"type", "Type":"string"}, 
            {"Name":"code", "Type":"string"},
            {"Name":"icao", "Type":"string"},
            {"Name":"desc", "Type":"string"},
            {"Name":"region", "Type":"string"},
            {"Name":"runways", "Type":"int"},
            {"Name":"longest", "Type":"int"},
            {"Name":"elev", "Type":"int"},
            {"Name":"country", "Type":"string"},
            {"Name":"city", "Type":"string"},
            {"Name":"lat", "Type":"double"},
            {"Name":"lon", "Type":"double"}
        ], 
        "Location":"s3://dummy-bucket/"},
        "Parameters":{ 
             "separatorChar":",",
            "componenttype":"rdf",
            "prefix_prop":"http://kelvinlawrence.net/air-routes/datatypeProperty/",
            "prefix_class":"http://kelvinlawrence.net/air-routes/class/",
            "querymode":"class",
            "classuri":"class:Airport",
            "subject":"id",
            "strip_uri":"true",
            "preds_prefix":"prop"
        } 
        }' \
    --profile $1 \
    --endpoint https://glue.$2.amazonaws.com
    --region $3
    
aws glue create-table \
    --database-name $dbname \
    --table-input  '{"Name":"airport_route", "StorageDescriptor":{ 
        "Columns":[ 
            {"Name":"incode", "Type":"string"}, 
            {"Name":"outcode", "Type":"string"}, 
            {"Name":"dist", "Type":"int"}
        ], 
        "Location":"s3://dummy-bucket/"},
        "Parameters":{ 
             "separatorChar":",",
            "componenttype":"rdf",
            "prefix_prop":"http://kelvinlawrence.net/air-routes/datatypeProperty/",
            "prefix_op":"http://kelvinlawrence.net/air-routes/objectProperty/",
            "querymode":"sparql",
            "sparql": "select ?incode ?outcode ?dist where {  ?resin op:route ?resout . GRAPH ?route { ?resin op:route ?resout } .  ?route prop:dist ?dist  . ?resin prop:code ?incode .?resout prop:code ?outcode . } ",
            "strip_uri":"true",
        } 
        }' \
    --profile $1 \
    --endpoint https://glue.$2.amazonaws.com
    --region $3

    
