#!/bin/bash

# id is the reserved column for vertex tables
# id, from, to are the reserved columns for edge tables

echo $1;
echo $2;

dbname='graph-database'


aws glue create-database \
--database-input "{\"Name\":\"${dbname}\"}" \
--profile $1 \
--endpoint https://glue.$2.amazonaws.com


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
        "Location":"s3://dummy-bucket/"},
        "Parameters":{ 
            "separatorChar":",",
            "componenttype":"vertex"
            } 
        }' \
    --profile $1 \
    --endpoint https://glue.$2.amazonaws.com

aws glue create-table \
    --database-name $dbname \
    --table-input  '{"Name":"country", "StorageDescriptor":{ 
        "Columns":[ 
            {"Name":"id", "Type":"string"}, 
            {"Name":"code", "Type":"string"}, 
            {"Name":"desc", "Type":"string"}
        ], 
        "Location":"s3://dummy-bucket/"},
        "Parameters":{ 
             "separatorChar":",",
            "componenttype":"vertex"} 
        }' \
    --profile $1 \
    --endpoint https://glue.$2.amazonaws.com

aws glue create-table \
    --database-name $dbname \
    --table-input  '{"Name":"continent", "StorageDescriptor":{ 
        "Columns":[ 
            {"Name":"id", "Type":"string"}, 
            {"Name":"code", "Type":"string"}, 
            {"Name":"desc", "Type":"string"}
        ], 
        "Location":"s3://dummy-bucket/"},
        "Parameters":{ 
            "separatorChar":",",
            "componenttype":"vertex"} 
        }' \
    --profile $1 \
    --endpoint https://glue.$2.amazonaws.com

aws glue create-table \
    --database-name $dbname \
    --table-input  '{"Name":"route", "StorageDescriptor":{ 
        "Columns":[ 
            {"Name":"id", "Type":"string"}, 
            {"Name":"out", "Type":"string"}, 
            {"Name":"in", "Type":"string"},
            {"Name":"dist", "Type":"int"} 
        ], 
        "Location":"s3://dummy-bucket/"},
        "Parameters":{ 
             "separatorChar":",",
            "componenttype":"edge"} 
        }' \
    --profile $1 \
    --endpoint https://glue.$2.amazonaws.com
