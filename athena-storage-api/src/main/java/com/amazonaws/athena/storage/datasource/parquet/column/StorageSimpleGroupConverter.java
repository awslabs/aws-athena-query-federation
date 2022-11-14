/*-
 * #%L
 * Amazon Athena Storage API
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.amazonaws.athena.storage.datasource.parquet.column;

import com.amazonaws.athena.storage.datasource.TypeFactory;
import com.amazonaws.athena.storage.datasource.parquet.filter.ConstraintEvaluator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import java.util.HashMap;
import java.util.Map;

class StorageSimpleGroupConverter extends GroupConverter
{
    /**
     * Self as parent. Could be null
     */
    private final StorageSimpleGroupConverter parent;

    /**
     * Maps field index to associated Type
     */
    private final Map<Integer, Type> typeMap = new HashMap<>();

    /**
     * Converts value based on provided type
     */
    private final TypeFactory.TypedValueConverter valueConverter = TypeFactory.typedValueConverter();

    /**
     * A row index
     */
    private final int index;

    /**
     * To check whether a process should assemble the record
     */
    private boolean matched = true;

    /**
     * Current instance of {@link Group}
     */
    protected Group current;

    /**
     * List of field converter
     */
    private final Converter[] converters;

    StorageSimpleGroupConverter(StorageSimpleGroupConverter parent, int index, GroupType schema, ConstraintEvaluator evaluator)
    {
        this.parent = parent;
        this.index = index;

        converters = new Converter[schema.getFieldCount()];

        for (int i = 0; i < converters.length; i++) {
            final Type type = schema.getType(i);
            typeMap.put(i, type);
            if (type.isPrimitive()) {
                converters[i] = new StorageSimplePrimitiveConverter(this, i, evaluator);
            }
            else {
                converters[i] = new StorageSimpleGroupConverter(this, i, type.asGroupType(), evaluator);
            }
        }
    }

    /**
     * Sets value to indicate a match.
     *
     * @param isMatched Ture if a match found, false otherwise
     */
    void matched(boolean isMatched)
    {
        matched &= isMatched;
    }

    /**
     * Checks to see if a match found
     *
     * @return Ture if a match found, false otherwise
     */
    boolean matched()
    {
        return matched;
    }

    @Override
    public void start()
    {
        matched = true;
        current = parent.getCurrentRecord().addGroup(index);
    }

    /**
     * @param fieldIndex A field index for which a converter will be retrieved
     * @return An instance of {@link Converter}
     */
    @Override
    public Converter getConverter(int fieldIndex)
    {
        return converters[fieldIndex];
    }

    @Override
    public void end()
    {
    }

    /**
     * @return The current record if assembled, null otherwise.
     */
    public Group getCurrentRecord()
    {
        return current;
    }

    protected Object convert(int fieldIndex, Object value)
    {
        if (value == null) {
            return null;
        }
        Type type = typeMap.get(fieldIndex);
        if (type != null) {
            return valueConverter.convert(type, value);
        }
        return value;
    }
}
