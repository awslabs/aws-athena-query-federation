/*-
 * #%L
 * athena-hbase
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
package com.amazonaws.athena.connectors.hbase;

import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.hbase.client.Result;

/**
 * Used to resolve and convert complex types from HBase to Apache Arrow's type system
 * when using BlockUtils.setComplexValue(...).
 */
public class HbaseFieldResolver
        implements FieldResolver
{
    private final byte[] family;
    private final boolean isNative;

    /**
     * @param isNative True if the values are stored as native byte arrays in HBase.
     * @param family The HBase column family that this field resolver is for.
     */
    public HbaseFieldResolver(boolean isNative, byte[] family)
    {
        this.isNative = isNative;
        this.family = family;
    }

    /**
     * Static construction helper.
     *
     * @param isNative True if the values are stored as native byte arrays in HBase.
     * @param family The HBase column family that this field resolver is for.
     */
    public static HbaseFieldResolver resolver(boolean isNative, String family)
    {
        return new HbaseFieldResolver(isNative, family.getBytes());
    }

    /**
     * @param field The Apache Arrow field we'd like to extract from the val.
     * @param val The value from which we'd like to extract the provide field.
     * @return Object containing the value for the requested field.
     * @see FieldResolver in the Athena Query Federation SDK
     */
    @Override
    public Object getFieldValue(Field field, Object val)
    {
        if (!(val instanceof Result)) {
            String clazz = (val != null) ? val.getClass().getName() : "null";
            throw new IllegalArgumentException("Expected value of type Result but found " + clazz);
        }

        byte[] rawFieldValue = ((Result) val).getValue(family, field.getName().getBytes());
        return HbaseSchemaUtils.coerceType(isNative, field.getType(), rawFieldValue);
    }
}
