package com.amazonaws.athena.connector.lambda.metadata;

import com.amazonaws.services.lambda.invoke.LambdaFunction;

public interface MetadataService
{
    @LambdaFunction(functionName="metadata")
    MetadataResponse getMetadata(final MetadataRequest request);
}
