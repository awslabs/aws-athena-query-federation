package com.amazonaws.athena.connectors.cloudwatch;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data. In this case we just compose CloudwatchMetadataHandler and CloudwatchRecordHandler.
 */
public class CloudwatchCompositeHandler
        extends CompositeHandler
{
    private static final String SOURCE_TYPE = "cloudwatch";

    public CloudwatchCompositeHandler()
    {
        super(new CloudwatchMetadataHandler(), new CloudwatchRecordHandler(), SOURCE_TYPE);
    }
}
