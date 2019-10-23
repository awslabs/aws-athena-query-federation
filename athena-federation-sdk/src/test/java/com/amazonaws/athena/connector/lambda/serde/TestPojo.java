package com.amazonaws.athena.connector.lambda.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.arrow.vector.types.pojo.Schema;

public class TestPojo
{
    private final Schema schema;

    @JsonCreator
    public TestPojo(@JsonProperty("schema") Schema schema)
    {
        this.schema = schema;
    }

    public Schema getSchema()
    {
        return schema;
    }
}
