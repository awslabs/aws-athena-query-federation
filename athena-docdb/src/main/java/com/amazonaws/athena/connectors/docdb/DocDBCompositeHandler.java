package com.amazonaws.athena.connectors.docdb;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

public class DocDBCompositeHandler
        extends CompositeHandler
{
    private static final String SOURCE_TYPE = "documentdb";

    public DocDBCompositeHandler()
    {
        super(new DocDBMetadataHandler(), new DocDBRecordHandler(), SOURCE_TYPE);
    }
}
