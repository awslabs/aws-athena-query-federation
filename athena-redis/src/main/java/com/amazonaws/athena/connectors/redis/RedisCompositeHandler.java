package com.amazonaws.athena.connectors.redis;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data. In this case we just compose RedisMetadataHandler and RedisRecordHandler.
 */
public class RedisCompositeHandler
        extends CompositeHandler
{
    private static final String SOURCE_TYPE = "redis";

    public RedisCompositeHandler()
    {
        super(new RedisMetadataHandler(), new RedisRecordHandler(), SOURCE_TYPE);
    }
}
