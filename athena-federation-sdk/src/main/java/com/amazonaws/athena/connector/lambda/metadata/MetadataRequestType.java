package com.amazonaws.athena.connector.lambda.metadata;

public enum MetadataRequestType
{
    LIST_TABLES,
    LIST_SCHEMAS,
    GET_TABLE,
    GET_TABLE_LAYOUT,
    GET_SPLITS;
}
