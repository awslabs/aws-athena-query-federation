package com.amazonaws.athena.connector.lambda.data;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class SchemaAware
{

    protected abstract Schema internalGetSchema();

    //TODO: This isn't very performant, perhaps we should keep a map
    public Field getField(String fieldName)
    {
        Schema schema = internalGetSchema();
        List<Field> results = schema.getFields()
                .stream().filter(next -> next.getName()
                        .equals(fieldName)).collect(Collectors.toList());

        if (results.size() != 1) {
            throw new IllegalArgumentException("fieldName is ambiguous, found:" + results.toString());
        }

        return results.get(0);
    }

    public List<Field> getFields()
    {
        return internalGetSchema().getFields();
    }

    public String getMetaData(String key)
    {
        return internalGetSchema().getCustomMetadata().get(key);
    }

    public Map<String, String> getMetaData()
    {
        return internalGetSchema().getCustomMetadata();
    }
}
