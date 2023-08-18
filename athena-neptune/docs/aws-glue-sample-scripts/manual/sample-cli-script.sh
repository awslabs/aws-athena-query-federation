#!/bin/bash

# id is the reserved column for vertex tables
# id, from, to are the reserved columns for edge tables

echo $1;
echo $2;

dbname='graph-database'


aws glue create-database \
--database-input "{\"Name\":\"${dbname}\"}" \
--profile $1 \
--endpoint https://glue.$2.amazonaws.com \
--region $2


aws glue create-table \
    --database-name $dbname \
    --table-input  '{"Name":"airport", "StorageDescriptor":{ 
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
        "Location":"s2://dummy-bucket/"},
        "Parameters":{ 
            "separatorChar":",",
            "componenttype":"vertex",
            "glabel":"airport"
            } 
        }' \
    --profile $1 \
    --endpoint https://glue.$2.amazonaws.com \
    --region $2

aws glue create-table \
    --database-name $dbname \
    --table-input  '{"Name":"country", "StorageDescriptor":{ 
        "Columns":[ 
            {"Name":"id", "Type":"string"}, 
            {"Name":"code", "Type":"string"}, 
            {"Name":"desc", "Type":"string"}
        ], 
        "Location":"s2://dummy-bucket/"},
        "Parameters":{ 
             "separatorChar":",",
             "componenttype":"vertex",
                "glabel":"country"
            } 
        }' \
    --profile $1 \
    --endpoint https://glue.$2.amazonaws.com \
    --region $2

aws glue create-table \
    --database-name $dbname \
    --table-input  '{"Name":"continent", "StorageDescriptor":{ 
        "Columns":[ 
            {"Name":"id", "Type":"string"}, 
            {"Name":"code", "Type":"string"}, 
            {"Name":"desc", "Type":"string"}
        ], 
        "Location":"s2://dummy-bucket/"},
        "Parameters":{ 
            "separatorChar":",",
            "componenttype":"vertex",
             "glabel":"continent"} 
        }' \
    --profile $1 \
    --endpoint https://glue.$2.amazonaws.com \
    --region $2

aws glue create-table \
    --database-name $dbname \
    --table-input  '{"Name":"route", "StorageDescriptor":{ 
        "Columns":[ 
            {"Name":"id", "Type":"string"}, 
            {"Name":"out", "Type":"string"}, 
            {"Name":"in", "Type":"string"},
            {"Name":"dist", "Type":"int"} 
        ], 
        "Location":"s2://dummy-bucket/"},
        "Parameters":{ 
             "separatorChar":",",
            "componenttype":"edge",
            "glabel":"route"} 
        }' \
    --profile $1 \
    --endpoint https://glue.$2.amazonaws.com \
    --region $2


aws glue create-table \
    --database-name $dbname \
    --table-input  '{"Name":"sourcetodestinationairport", "StorageDescriptor":{ 
        "Columns":[ 
            {"Name":"source", "Type":"string"}, 
            {"Name":"destination", "Type":"string"}
        ], 
        "Location":"s2://dummy-bucket/"},
        "Parameters":{ 
            "separatorChar":",",
            "componenttype":"view",
            "query":"g.V().hasLabel(\"airport\").as(\"source\").out(\"route\").as(\"destination\").select(\"source\",\"destination\").by(\"code\").limit(10)"
            } 
        }' \
    --profile $1 \
    --endpoint https://glue.$2.amazonaws.com \
    
<<<<<<< HEAD
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
            "prefix_res":"http://kelvinlawrence.net/air-routes/resource/",
            "prefix_prop":"http://kelvinlawrence.net/air-routes/datatypeProperty/",
            "prefix_op":"http://kelvinlawrence.net/air-routes/objectProperty/",
            "prefix_class":"http://kelvinlawrence.net/air-routes/class/",
            "prefix_geo":"http://www.w3.org/2003/01/geo/wgs84_pos#",
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
            "prefix_res":"http://kelvinlawrence.net/air-routes/resource/",
            "prefix_prop":"http://kelvinlawrence.net/air-routes/datatypeProperty/",
            "prefix_op":"http://kelvinlawrence.net/air-routes/objectProperty/",
            "prefix_class":"http://kelvinlawrence.net/air-routes/class/",
            "prefix_geo":"http://www.w3.org/2003/01/geo/wgs84_pos#",
            "querymode":"sparql",
            "sparql": "select ?incode ?outcode ?dist where {  ?resin op:route ?resout . GRAPH ?route { ?resin op:route ?resout } .  ?route prop:dist ?dist  . ?resin prop:code ?incode .?resout prop:code ?outcode . } ",
            "strip_uri":"true",
        } 
        }' \
    --profile $1 \
    --endpoint https://glue.$2.amazonaws.com
    --region $3
=======
>>>>>>> fd0c7fc7 (Update sample-cli-script.sh)

