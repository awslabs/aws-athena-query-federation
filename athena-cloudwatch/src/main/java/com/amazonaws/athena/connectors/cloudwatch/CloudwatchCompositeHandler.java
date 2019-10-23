package com.amazonaws.athena.connectors.cloudwatch;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

public class CloudwatchCompositeHandler
        extends CompositeHandler
{
    private static final String SOURCE_TYPE = "cloudwatch";

    public CloudwatchCompositeHandler()
    {
        super(new CloudwatchMetadataHandler(), new CloudwatchRecordHandler(), SOURCE_TYPE);
    }
}
