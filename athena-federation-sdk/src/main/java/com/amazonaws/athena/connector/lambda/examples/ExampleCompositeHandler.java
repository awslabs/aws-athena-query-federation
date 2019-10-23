package com.amazonaws.athena.connector.lambda.examples;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;
import com.amazonaws.services.s3.AmazonS3;

public class ExampleCompositeHandler
        extends CompositeHandler
{
    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "custom";

    public ExampleCompositeHandler(AmazonS3 amazonS3, String sourceType)
    {
        super(new ExampleMetadataHandler(), new ExampleRecordHandler(), SOURCE_TYPE);
    }
}
