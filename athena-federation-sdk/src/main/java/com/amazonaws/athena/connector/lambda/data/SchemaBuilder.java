package com.amazonaws.athena.connector.lambda.data;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Convenience builder that can be used to create new Apache Arrow Schema for common
 * types more easily than alternative methods of construction, especially for complex types.
 */
public class SchemaBuilder
{
    //Using LinkedHashMap to maintain the order of elements in fields and present consistent schema view.
    private final Map<String, Field> fields = new LinkedHashMap<>();
    private final ImmutableMap.Builder<String, String> metadata = ImmutableMap.builder();
    //Using LinkedHashMap because Apache Arrow makes field order important so honoring that contract here
    private final Map<String, FieldBuilder> nestedFieldBuilderMap = new LinkedHashMap<>();

    public SchemaBuilder addField(Field field)
    {
        fields.put(field.getName(), field);
        return this;
    }

    /**
     * Adds a new Field with the provided details to the Schema as a top-level Field with no children.
     *
     * @param fieldName The name of the field to add.
     * @param type The type of the field to add.
     * @return This SchemaBuilder itself.
     */
    public SchemaBuilder addField(String fieldName, ArrowType type)
    {
        fields.put(fieldName, new Field(fieldName, FieldType.nullable(type), null));
        return this;
    }

    /**
     * Adds a new Field with the provided details to the Schema as a top-level Field.
     *
     * @param fieldName The name of the field to add.
     * @param type The type of the field to add.
     * @param children The list of child fields to add to the new Field.
     * @return This SchemaBuilder itself.
     */
    public SchemaBuilder addField(String fieldName, ArrowType type, List<Field> children)
    {
        fields.put(fieldName, new Field(fieldName, FieldType.nullable(type), children));
        return this;
    }

    /**
     * Adds a new STRUCT Field to the Schema as a top-level Field.
     *
     * @param fieldName The name of the field to add.
     * @return This SchemaBuilder itself.
     */
    public SchemaBuilder addStructField(String fieldName)
    {
        nestedFieldBuilderMap.put(fieldName, FieldBuilder.newBuilder(fieldName, Types.MinorType.STRUCT.getType()));
        return this;
    }

    /**
     * Adds a new LIST Field to the Schema as a top-level Field.
     *
     * @param fieldName The name of the field to add.
     * @param type The concrete type of the values that are held within the list.
     * @return This SchemaBuilder itself.
     */
    public SchemaBuilder addListField(String fieldName, ArrowType type)
    {
        fields.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.LIST.getType()),
                Collections.singletonList(new Field("", FieldType.nullable(type), null))));
        return this;
    }

    /**
     * Adds a new Field as a child of the requested top-level parent field.
     *
     * @param parent The name of the pre-existing top-level parent field to add a child field to.
     * @param child The name of the new child field.
     * @param type The type of the new child field to add.
     * @return This SchemaBuilder itself.
     * @note For more complex nesting, please use FieldBuilder.
     */
    public SchemaBuilder addChildField(String parent, String child, ArrowType type)
    {
        nestedFieldBuilderMap.get(parent).addField(child, type, null);
        return this;
    }

    /**
     * Adds a new Field as a child of the requested top-level parent field.
     *
     * @param parent The name of the pre-existing top-level parent field to add a child field to.
     * @param child The child field to add to the parent.
     * @return This SchemaBuilder itself.
     * @note For more complex nesting, please use FieldBuilder.
     */
    public SchemaBuilder addChildField(String parent, Field child)
    {
        nestedFieldBuilderMap.get(parent).addField(child);
        return this;
    }

    /**
     * Adds a new VARCHAR Field to the Schema as a top-level Field with no children.
     *
     * @param fieldName The name of the field to add.
     * @return This SchemaBuilder itself.
     */
    public SchemaBuilder addStringField(String fieldName)
    {
        fields.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null));
        return this;
    }

    /**
     * Adds a new INT Field to the Schema as a top-level Field with no children.
     *
     * @param fieldName The name of the field to add.
     * @return This SchemaBuilder itself.
     */
    public SchemaBuilder addIntField(String fieldName)
    {
        fields.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.INT.getType()), null));
        return this;
    }

    /**
     * Adds a new TINYINT Field to the Schema as a top-level Field with no children.
     *
     * @param fieldName The name of the field to add.
     * @return This SchemaBuilder itself.
     */
    public SchemaBuilder addTinyIntField(String fieldName)
    {
        fields.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.TINYINT.getType()), null));
        return this;
    }

    /**
     * Adds a new SMALLINT Field to the Schema as a top-level Field with no children.
     *
     * @param fieldName The name of the field to add.
     * @return This SchemaBuilder itself.
     */
    public SchemaBuilder addSmallIntField(String fieldName)
    {
        fields.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.SMALLINT.getType()), null));
        return this;
    }

    /**
     * Adds a new FLOAT8 Field to the Schema as a top-level Field with no children.
     *
     * @param fieldName The name of the field to add.
     * @return This SchemaBuilder itself.
     */
    public SchemaBuilder addFloat8Field(String fieldName)
    {
        fields.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.FLOAT8.getType()), null));
        return this;
    }

    /**
     * Adds a new FLOAT4 Field to the Schema as a top-level Field with no children.
     *
     * @param fieldName The name of the field to add.
     * @return This SchemaBuilder itself.
     */
    public SchemaBuilder addFloat4Field(String fieldName)
    {
        fields.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.FLOAT4.getType()), null));
        return this;
    }

    /**
     * Adds a new BIGINT Field to the Schema as a top-level Field with no children.
     *
     * @param fieldName The name of the field to add.
     * @return This SchemaBuilder itself.
     */
    public SchemaBuilder addBigIntField(String fieldName)
    {
        fields.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.BIGINT.getType()), null));
        return this;
    }

    /**
     * Adds a new BIT Field to the Schema as a top-level Field with no children.
     *
     * @param fieldName The name of the field to add.
     * @return This SchemaBuilder itself.
     */
    public SchemaBuilder addBitField(String fieldName)
    {
        fields.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.BIT.getType()), null));
        return this;
    }

    /**
     * Adds a new DECIMAL Field to the Schema as a top-level Field with no children.
     *
     * @param fieldName The name of the field to add.
     * @param precision The precision to use for the new decimal field.
     * @param scale The scale to use for the new decimal field.
     * @return This SchemaBuilder itself.
     */
    public SchemaBuilder addDecimalField(String fieldName, int precision, int scale)
    {
        fields.put(fieldName, new Field(fieldName, FieldType.nullable(new ArrowType.Decimal(precision, scale)), null));
        return this;
    }

    /**
     * Adds a new DateDay Field to the Schema as a top-level Field with no children.
     *
     * @param fieldName The name of the field to add.
     * @return This SchemaBuilder itself.
     */
    public SchemaBuilder addDateDayField(String fieldName)
    {
        fields.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.DATEDAY.getType()), null));
        return this;
    }

    /**
     * Adds a new DateMilli Field to the Schema as a top-level Field with no children.
     *
     * @param fieldName The name of the field to add.
     * @return This SchemaBuilder itself.
     */
    public SchemaBuilder addDateMilliField(String fieldName)
    {
        fields.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null));
        return this;
    }

    /**
     * Adds the provided metadata to the Schema.
     *
     * @param key The key of the metadata to add.
     * @param value The value of the metadata to add.
     * @return This SchemaBuilder itself.
     */
    public SchemaBuilder addMetadata(String key, String value)
    {
        metadata.put(key, value);
        return this;
    }

    public Field getField(String name)
    {
        return fields.get(name);
    }

    public FieldBuilder getNestedField(String name)
    {
        return nestedFieldBuilderMap.get(name);
    }

    /**
     * Creates a new SchemaBuilder.
     *
     * @return A new SchemaBuilder.
     */
    public static SchemaBuilder newBuilder()
    {
        return new SchemaBuilder();
    }

    /**
     * Builds an Apache Arrow Schema from the collected metadata and fields.
     *
     * @return A new Apache Arrow Schema.
     * @note Attempting to reuse this builder will have unexpected side affects.
     */
    public Schema build()
    {
        for (Map.Entry<String, FieldBuilder> next : nestedFieldBuilderMap.entrySet()) {
            fields.put(next.getKey(), next.getValue().build());
        }
        return new Schema(fields.values(), metadata.build());
    }
}
