package com.amazonaws.athena.connector.lambda.data;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaBuilder
{
    private final ImmutableList.Builder<Field> fields = ImmutableList.builder();
    private final ImmutableMap.Builder<String, String> metadata = ImmutableMap.builder();
    private final Map<String, FieldBuilder> nestedFieldBuilderMap = new HashMap<>();

    public SchemaBuilder addField(Field field)
    {
        fields.add(field);
        return this;
    }

    public SchemaBuilder addField(String fieldName, ArrowType type)
    {
        fields.add(new Field(fieldName, FieldType.nullable(type), null));
        return this;
    }

    public SchemaBuilder addField(String fieldName, ArrowType type, List<Field> children)
    {
        fields.add(new Field(fieldName, FieldType.nullable(type), children));
        return this;
    }

    public SchemaBuilder addStructField(String fieldName)
    {
        nestedFieldBuilderMap.put(fieldName, FieldBuilder.newBuilder(fieldName, Types.MinorType.STRUCT.getType()));
        return this;
    }

    public SchemaBuilder addListField(String fieldName, ArrowType type)
    {
        fields.add(new Field(fieldName, FieldType.nullable(Types.MinorType.LIST.getType()),
                Collections.singletonList(new Field("", FieldType.nullable(type), null))));
        return this;
    }

    public SchemaBuilder addChildField(String parent, String child, ArrowType type)
    {
        nestedFieldBuilderMap.get(parent).addField(child, type, null);
        return this;
    }

    public SchemaBuilder addChildField(String parent, Field child)
    {
        nestedFieldBuilderMap.get(parent).addField(child);
        return this;
    }

    public SchemaBuilder addStringField(String fieldName)
    {
        fields.add(new Field(fieldName, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null));
        return this;
    }

    public SchemaBuilder addIntField(String fieldName)
    {
        fields.add(new Field(fieldName, FieldType.nullable(Types.MinorType.INT.getType()), null));
        return this;
    }

    public SchemaBuilder addFloat8Field(String fieldName)
    {
        fields.add(new Field(fieldName, FieldType.nullable(Types.MinorType.FLOAT8.getType()), null));
        return this;
    }

    public SchemaBuilder addBigIntField(String fieldName)
    {
        fields.add(new Field(fieldName, FieldType.nullable(Types.MinorType.BIGINT.getType()), null));
        return this;
    }

    public SchemaBuilder addBitField(String fieldName)
    {
        fields.add(new Field(fieldName, FieldType.nullable(Types.MinorType.BIT.getType()), null));
        return this;
    }

    public SchemaBuilder addMetadata(String key, String value)
    {
        metadata.put(key, value);
        return this;
    }

    public static SchemaBuilder newBuilder()
    {
        return new SchemaBuilder();
    }

    public Schema build()
    {

        for (FieldBuilder next : nestedFieldBuilderMap.values()) {
            fields.add(next.build());
        }
        return new Schema(fields.build(), metadata.build());
    }
}
