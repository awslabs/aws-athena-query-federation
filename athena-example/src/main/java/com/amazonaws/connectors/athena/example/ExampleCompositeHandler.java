package com.amazonaws.connectors.athena.example;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

public class ExampleCompositeHandler
        extends CompositeHandler
{
    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "example";

    public ExampleCompositeHandler()
    {
        super(new ExampleMetadataHandler(), new ExampleRecordHandler(), SOURCE_TYPE);
    }
}
