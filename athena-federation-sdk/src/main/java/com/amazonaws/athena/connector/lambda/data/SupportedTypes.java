/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.data;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.HashSet;
import java.util.Set;

/**
 * This enum defines the ApacheArrow types which are supported by this SDK. Using types not
 * in this enum may lead to unpredictable performance or errors as different engines (e.g. Athena)
 * may support these types to a different extent.
 */
public enum SupportedTypes
{
    BIT(Types.MinorType.BIT),
    DATEMILLI(Types.MinorType.DATEMILLI),
    DATEDAY(Types.MinorType.DATEDAY),
    TIMESTAMPMILLITZ(Types.MinorType.TIMESTAMPMILLITZ),
    TIMESTAMPMICROTZ(Types.MinorType.TIMESTAMPMICROTZ),
    FLOAT8(Types.MinorType.FLOAT8),
    FLOAT4(Types.MinorType.FLOAT4),
    INT(Types.MinorType.INT),
    TINYINT(Types.MinorType.TINYINT),
    SMALLINT(Types.MinorType.SMALLINT),
    BIGINT(Types.MinorType.BIGINT),
    VARBINARY(Types.MinorType.VARBINARY),
    DECIMAL(Types.MinorType.DECIMAL),
    VARCHAR(Types.MinorType.VARCHAR),
    STRUCT(Types.MinorType.STRUCT),
    LIST(Types.MinorType.LIST),
    MAP(Types.MinorType.MAP);

    private Types.MinorType arrowMinorType;

    static final Set<Types.MinorType> SUPPORTED_TYPES = new HashSet<>();

    static {
        for (SupportedTypes next : values()) {
            SUPPORTED_TYPES.add(next.arrowMinorType);
        }
    }

    SupportedTypes(Types.MinorType arrowMinorType)
    {
        this.arrowMinorType = arrowMinorType;
    }

    public Types.MinorType getArrowMinorType()
    {
        return arrowMinorType;
    }

    /**
     * Tests if the provided type is supported.
     *
     * @param minorType The minor type to test
     * @return True if the minor type is supported, false otherwise.
     */
    public static boolean isSupported(Types.MinorType minorType)
    {
        return SUPPORTED_TYPES.contains(minorType);
    }

    /**
     * Tests if the provided type is supported.
     *
     * @param arrowType The arrow type to test
     * @return True if the arrow type is supported, false otherwise.
     */
    public static boolean isSupported(ArrowType arrowType)
    {
        return isSupported(Types.getMinorTypeForArrowType(arrowType));
    }

    /**
     * Asserts if the provided field (and its children) all use supported types.
     *
     * @param field The field to test.
     * @return The field being asserted, if all types used by the field and its children are supported.
     * @throws RuntimeException If any of the types used by this field or its children are not supported.
     */
    public static boolean isSupported(Field field)
    {
        if (!isSupported(Types.getMinorTypeForArrowType(field.getType()))) {
            return false;
        }

        if (field.getChildren() != null) {
            for (Field nextChild : field.getChildren()) {
                if (!isSupported(nextChild)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Asserts if the provided field (and its children) all use supported types.
     *
     * @param field The field to test.
     * @throws RuntimeException If any of the types used by this field or its children are not supported.
     */
    public static void assertSupported(Field field)
    {
        if (!isSupported(field)) {
            throw new RuntimeException("Detected unsupported type[" + field.getType() + " / " + Types.getMinorTypeForArrowType(field.getType()) +
                    " for column[" + field.getName() + "]");
        }
    }
}
