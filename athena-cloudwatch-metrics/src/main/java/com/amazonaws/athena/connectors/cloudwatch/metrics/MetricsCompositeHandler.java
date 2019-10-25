package com.amazonaws.athena.connectors.cloudwatch.metrics;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data. In this case we just compose MetricsMetadataHandler and MetricsRecordHandler.
 */
public class MetricsCompositeHandler
        extends CompositeHandler
{
    private static final String SOURCE_TYPE = "metrics";

    public MetricsCompositeHandler()
    {
        super(new MetricsMetadataHandler(), new MetricsRecordHandler(), SOURCE_TYPE);
    }
}
