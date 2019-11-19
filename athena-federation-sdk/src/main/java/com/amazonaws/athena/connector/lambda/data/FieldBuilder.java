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

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Convenience builder that can be used to create new Apache Arrow fields for common
 * types more easily than alternative methods of construction, especially for complex types.
 */
public class FieldBuilder
{
    private final String name;
    private final ArrowType type;
    //Using LinkedHashMap because Apache Arrow makes field order important so honoring that contract here
    private final Map<String, Field> children = new LinkedHashMap<>();

    /**
     * Creates a FieldBuilder for a Field with the given name and type.
     *
     * @param name The name to use for the Field being built.
     * @param type The type to use for the Field being built, most often one of STRUCT or LIST.
     */
    private FieldBuilder(String name, ArrowType type)
    {
        this.name = name;
        this.type = type;
    }

    /**
     * Creates a FieldBuilder for a Field with the given name and type.
     *
     * @param name The name to use for the Field being built.
     * @param type The type to use for the Field being built, most often one of STRUCT or LIST.
     * @return A new FieldBuilder for the specified name and type.
     */
    public static FieldBuilder newBuilder(String name, ArrowType type)
    {
        return new FieldBuilder(name, type);
    }

    /**
     * Adds a new child field with the requested attributes.
     *
     * @param fieldName The name of the child field.
     * @param type The type of the child field.
     * @param children The children to add to the child field (empty list if no children desired).
     * @return This FieldBuilder itself.
     */
    public FieldBuilder addField(String fieldName, ArrowType type, List<Field> children)
    {
        this.children.put(fieldName, new Field(fieldName, FieldType.nullable(type), children));
        return this;
    }

    /**
     * Adds the provided field as a child to the builder.
     *
     * @param child The child to add to the Field being built.
     * @return This FieldBuilder itself.
     */
    public FieldBuilder addField(Field child)
    {
        this.children.put(child.getName(), child);
        return this;
    }

    /**
     * Adds a new VARCHAR child field with the given name to the builder.
     *
     * @param fieldName The name to use for the newly added child field.
     * @return This FieldBuilder itself.
     */
    public FieldBuilder addStringField(String fieldName)
    {
        this.children.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null));
        return this;
    }

    /**
     * Adds a new LIST child field with the given name to the builder.
     *
     * @param fieldName The name to use for the newly added child field.
     * @param type The concrete type for values in the List
     * @return This FieldBuilder itself.
     */
    public FieldBuilder addListField(String fieldName, ArrowType type)
    {
        Field baseField = new Field("", FieldType.nullable(type), null);
        Field field = new Field(fieldName,
                FieldType.nullable(Types.MinorType.LIST.getType()),
                Collections.singletonList(baseField));
        this.children.put(fieldName, field);
        return this;
    }

    /**
     * Adds a new INT child field with the given name to the builder.
     *
     * @param fieldName The name to use for the newly added child field.
     * @return This FieldBuilder itself.
     */
    public FieldBuilder addIntField(String fieldName)
    {
        this.children.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.INT.getType()), null));
        return this;
    }

    /**
     * Adds a new FLOAT8 child field with the given name to the builder.
     *
     * @param fieldName The name to use for the newly added child field.
     * @return This FieldBuilder itself.
     */
    public FieldBuilder addFloat8Field(String fieldName)
    {
        this.children.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.FLOAT8.getType()), null));
        return this;
    }

    /**
     * Adds a new BIGINT child field with the given name to the builder.
     *
     * @param fieldName The name to use for the newly added child field.
     * @return This FieldBuilder itself.
     */
    public FieldBuilder addBigIntField(String fieldName)
    {
        this.children.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.BIGINT.getType()), null));
        return this;
    }

    /**
     * Adds a new BIT child field with the given name to the builder.
     *
     * @param fieldName The name to use for the newly added child field.
     * @return This FieldBuilder itself.
     */
    public FieldBuilder addBitField(String fieldName)
    {
        this.children.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.BIT.getType()), null));
        return this;
    }

    /**
     * Adds a new TinyInt child field with the given name to the builder.
     *
     * @param fieldName The name to use for the newly added child field.
     * @return This FieldBuilder itself.
     */
    public FieldBuilder addTinyIntField(String fieldName)
    {
        this.children.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.TINYINT.getType()), null));
        return this;
    }

    /**
     * Adds a new SmallInt child field with the given name to the builder.
     *
     * @param fieldName The name to use for the newly added child field.
     * @return This FieldBuilder itself.
     */
    public FieldBuilder addSmallIntField(String fieldName)
    {
        this.children.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.SMALLINT.getType()), null));
        return this;
    }

    /**
     * Adds a new Float4 child field with the given name to the builder.
     *
     * @param fieldName The name to use for the newly added child field.
     * @return This FieldBuilder itself.
     */
    public FieldBuilder addFloat4Field(String fieldName)
    {
        this.children.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.FLOAT4.getType()), null));
        return this;
    }

    /**
     * Adds a new Decimal child field with the given name to the builder.
     *
     * @param fieldName The name to use for the newly added child field.
     * @return This FieldBuilder itself.
     */
    public FieldBuilder addDecimalField(String fieldName, int precision, int scale)
    {
        this.children.put(fieldName, new Field(fieldName, FieldType.nullable(new ArrowType.Decimal(precision, scale)), null));
        return this;
    }

    /**
     * Adds a new DateDay child field with the given name to the builder.
     *
     * @param fieldName The name to use for the newly added child field.
     * @return This FieldBuilder itself.
     */
    public FieldBuilder addDateDayField(String fieldName)
    {
        this.children.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.DATEDAY.getType()), null));
        return this;
    }

    /**
     * Adds a new DateMilli child field with the given name to the builder.
     *
     * @param fieldName The name to use for the newly added child field.
     * @return This FieldBuilder itself.
     */
    public FieldBuilder addDateMilliField(String fieldName)
    {
        this.children.put(fieldName, new Field(fieldName, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null));
        return this;
    }

    public Field getChild(String fieldName)
    {
        return children.get(fieldName);
    }

    /**
     * Builds the fields.
     *
     * @return The newly constructed Field.
     */
    public Field build()
    {
        return new Field(name, FieldType.nullable(type), new ArrayList<>(children.values()));
    }
}
