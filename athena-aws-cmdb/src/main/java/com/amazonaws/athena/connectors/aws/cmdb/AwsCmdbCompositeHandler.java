package com.amazonaws.athena.connectors.aws.cmdb;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

public class AwsCmdbCompositeHandler
        extends CompositeHandler
{
    private static final String SOURCE_TYPE = "cmdb";

    public AwsCmdbCompositeHandler()
    {
        super(new AwsCmdbMetadataHandler(), new AwsCmdbRecordHandler(), SOURCE_TYPE);
    }
}
