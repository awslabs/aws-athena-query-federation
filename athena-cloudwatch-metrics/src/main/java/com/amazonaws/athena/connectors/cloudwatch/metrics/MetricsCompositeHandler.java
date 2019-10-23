package com.amazonaws.athena.connectors.cloudwatch.metrics;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

public class MetricsCompositeHandler
        extends CompositeHandler
{
    private static final String SOURCE_TYPE = "metrics";

    public MetricsCompositeHandler()
    {
        super(new MetricsMetadataHandler(), new MetricsRecordHandler(), SOURCE_TYPE);
    }
}
