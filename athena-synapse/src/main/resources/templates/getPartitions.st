getPartitions(name, schemaname) ::=  <<
SELECT s.[name] AS [schema_name],
t.[name]                        AS      [table_name],
i.[name]                        AS      [index_name],
p.[partition_number]            AS      [partition_number],
p.[rows]                        AS      [partition_row_count],
rv.[value]                      AS      [partition_boundary_value],
p.[data_compression_desc]       AS      [partition_compression_desc],
c.[name]                        AS      [Partition_Column]
FROM        sys.schemas s
JOIN        sys.tables t                    ON      t.[schema_id]         = s.[schema_id]
JOIN        sys.partitions p                ON      p.[object_id]         = t.[object_id]
JOIN        sys.indexes i                   ON      i.[object_id]         = p.[object_id]
AND     i.[index_id]          = p.[index_id]
JOIN        sys.data_spaces ds              ON      ds.[data_space_id]    = i.[data_space_id]
JOIN        sys.index_columns AS ic         ON      ic.[object_id] = i.[object_id]
AND     ic.index_id = i.index_id
AND     ic.partition_ordinal >= 1
JOIN        sys.columns AS c                ON      t.[object_id] = c.[object_id]
AND     ic.column_id = c.column_id
LEFT JOIN   sys.partition_schemes ps        ON      ps.[data_space_id]    = ds.[data_space_id]
LEFT JOIN   sys.partition_functions pf      ON      pf.[function_id]      = ps.[function_id]
LEFT JOIN   sys.partition_range_values rv   ON      rv.[function_id]      = pf.[function_id]
AND     rv.[boundary_id] = p.[partition_number]
WHERE       p.[index_id] <=1 and t.[name] = '$name$' and s.[name] = '$schemaname$'
>>