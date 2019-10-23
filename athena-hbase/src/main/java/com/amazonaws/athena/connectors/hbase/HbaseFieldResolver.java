package com.amazonaws.athena.connectors.hbase;

import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseFieldResolver
        implements FieldResolver
{
    private final byte[] family;

    public HbaseFieldResolver(byte[] family)
    {
        this.family = family;
    }

    public static HbaseFieldResolver resolver(String family)
    {
        return new HbaseFieldResolver(family.getBytes());
    }

    @Override
    public Object getFieldValue(Field field, Object val)
    {
        if (!(val instanceof Result)) {
            String clazz = (val != null) ? val.getClass().getName() : "null";
            throw new IllegalArgumentException("Expected value of type Result but found " + clazz);
        }

        Result row = (Result) val;
        return HbaseSchemaUtils.coerceType(field.getType(), row.getValue(family, field.getName().getBytes()));
    }
}
