## Athena DeltaLake connector

Work in progress

### Known issues

handle getSplits
handle readRecords
add unit tests
add integration tests

handle types: Decimal, Map
handle partially written checkpoints (when checkpoint should be in 3 parts and there are only 2 files)

test conf fs.s3a.metadatastore.impl

Split the code properly

Clean pom.xml if dependencies can be removed
is it necessary to have 'parquet-hadoop-bundle' with the bundle ?

set athena-deltalake.yaml


Limitations

Partitions of type Binary
List tables without _$folder$ suffix

### Data Types

Using these primitive types: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types

|Delta DataType|Apache Arrow Type|
|-------------|-----------------|
|string|VARCHAR|
|long|BIGINT|	
|integer|INT|	
|short|SMALLINT|	
|byte|TINYINT|	
|float|FLOAT4|	
|double|FLOAT8|	
|decimal|DECIMAL|	
|boolean|BIT|	
|binary|VARBINARY|	
|date|DATEDAY|	
|timestamp|DATEMILLI|