#!/bin/bash

echo $1;
echo $2;


aws glue create-database \
--database-input "{\"Name\":\"graph-database\"}" \
--profile $1 \
--endpoint https://glue.$2.amazonaws.com

aws glue create-table \
    --database-name graph-database \
    --table-input  '{"Name":"airport", "StorageDescriptor":{ 
        "Columns":[ 
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
            "separatorChar":","} 
        }' \
    --profile $1 \
    --endpoint https://glue.$2.amazonaws.com

aws glue create-table \
    --database-name graph-database \
    --table-input  '{"Name":"country", "StorageDescriptor":{ 
        "Columns":[ 
            {"Name":"code", "Type":"string"}, 
            {"Name":"desc", "Type":"string"}
        ], 
        "Location":"s3://dummy-bucket/"},
        "Parameters":{ 
            "separatorChar":","} 
        }' \
    --profile $1 \
    --endpoint https://glue.$2.amazonaws.com

aws glue create-table \
    --database-name graph-database \
    --table-input  '{"Name":"continent", "StorageDescriptor":{ 
        "Columns":[ 
            {"Name":"code", "Type":"string"}, 
            {"Name":"desc", "Type":"string"}
        ], 
        "Location":"s3://dummy-bucket/"},
        "Parameters":{ 
            "separatorChar":","} 
        }' \
    --profile $1 \
    --endpoint https://glue.$2.amazonaws.com
