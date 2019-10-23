package com.amazonaws.athena.connector.lambda.records;

import com.amazonaws.services.lambda.invoke.LambdaFunction;

public interface RecordService
{
    @LambdaFunction(functionName = "record")
    RecordResponse readRecords(final RecordRequest request);
}
