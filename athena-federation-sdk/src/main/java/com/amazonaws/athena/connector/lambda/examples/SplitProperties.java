package com.amazonaws.athena.connector.lambda.examples;

public enum SplitProperties
{
    LOCATION(ExampleMetadataHandler.PARTITION_LOCATION),
    SERDE(ExampleMetadataHandler.SERDE),
    SPLIT_PART("SPLIT_PART");

    private final String id;

    SplitProperties(String id)
    {
        this.id = id;
    }

    public String getId()
    {
        return id;
    }
}
