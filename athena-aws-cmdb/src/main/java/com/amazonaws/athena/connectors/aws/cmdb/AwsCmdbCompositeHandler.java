package com.amazonaws.athena.connectors.aws.cmdb;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data. In this case we just compose AwsCmdbMetadataHandler and AwsCmdbRecordHandler.
 */
public class AwsCmdbCompositeHandler
        extends CompositeHandler
{
    private static final String SOURCE_TYPE = "cmdb";

    public AwsCmdbCompositeHandler()
    {
        super(new AwsCmdbMetadataHandler(), new AwsCmdbRecordHandler(), SOURCE_TYPE);
    }
}
