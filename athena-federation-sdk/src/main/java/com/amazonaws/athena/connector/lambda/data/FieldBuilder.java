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
import java.util.List;

public class FieldBuilder
{
    private final String name;
    private final ArrowType type;
    private final List<Field> children = new ArrayList<>();

    private FieldBuilder(String name, ArrowType type)
    {
        this.name = name;
        this.type = type;
    }

    public static FieldBuilder newBuilder(String name, ArrowType type)
    {
        return new FieldBuilder(name, type);
    }

    public FieldBuilder addField(String fieldName, ArrowType type, List<Field> children)
    {
        this.children.add(new Field(fieldName, FieldType.nullable(type), children));
        return this;
    }

    public FieldBuilder addField(Field child)
    {
        this.children.add(child);
        return this;
    }

    public FieldBuilder addStringField(String fieldName)
    {
        this.children.add(new Field(fieldName, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null));
        return this;
    }

    public FieldBuilder addListField(String fieldName, ArrowType type)
    {
        Field baseField = new Field("", FieldType.nullable(type), null);
        Field field = new Field(fieldName,
                FieldType.nullable(Types.MinorType.LIST.getType()),
                Collections.singletonList(baseField));
        this.children.add(field);
        return this;
    }

    public FieldBuilder addIntField(String fieldName)
    {
        this.children.add(new Field(fieldName, FieldType.nullable(Types.MinorType.INT.getType()), null));
        return this;
    }

    public FieldBuilder addFloat8Field(String fieldName)
    {
        this.children.add(new Field(fieldName, FieldType.nullable(Types.MinorType.FLOAT8.getType()), null));
        return this;
    }

    public FieldBuilder addBigIntField(String fieldName)
    {
        this.children.add(new Field(fieldName, FieldType.nullable(Types.MinorType.BIGINT.getType()), null));
        return this;
    }

    public FieldBuilder addBitField(String fieldName)
    {
        this.children.add(new Field(fieldName, FieldType.nullable(Types.MinorType.BIT.getType()), null));
        return this;
    }

    public Field build()
    {
        return new Field(name, FieldType.nullable(type), children);
    }
}
