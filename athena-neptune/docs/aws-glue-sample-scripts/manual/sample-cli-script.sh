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
            "componenttype":"query",
            "query":"g.V().hasLabel(\"airport\").as(\"s\").out(\"route\").as(\"d\").project(\"source\",\"destination\").by(select(\"s\").id()).by(select(\"d\").id()).limit(10)"
            } 
        }' \
    --profile $1 \
    --endpoint https://glue.$2.amazonaws.com \
    --region $2