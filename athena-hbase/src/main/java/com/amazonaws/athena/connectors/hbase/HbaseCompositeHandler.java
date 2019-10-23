package com.amazonaws.athena.connectors.hbase;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data. In this case we just compose HbaseMetadataHandler and HbaseRecordHandler.
 */
public class HbaseCompositeHandler
        extends CompositeHandler
{
    private static final String SOURCE_TYPE = "hbase";

    public HbaseCompositeHandler()
    {
        super(new HbaseMetadataHandler(), new HbaseRecordHandler(), SOURCE_TYPE);
    }
}
