package com.amazonaws.athena.connectors.dynamodb;

final class DynamoDBConstants
{
    private DynamoDBConstants() {}

    public static final String PARTITION_TYPE_METADATA = "partitionType";
    public static final String QUERY_PARTITION_TYPE = "query";
    public static final String SCAN_PARTITION_TYPE = "scan";
    public static final String SEGMENT_COUNT_METADATA = "segmentCount";
    public static final String SEGMENT_ID_PROPERTY = "segmentId";
    public static final String INDEX_METADATA = "index";
    public static final String HASH_KEY_NAME_METADATA = "hashKeyName";
    public static final String RANGE_KEY_NAME_METADATA = "rangeKeyName";
    public static final String PROVISIONED_READ_CAPACITY_METADATA = "provisionedCapacity";
}
