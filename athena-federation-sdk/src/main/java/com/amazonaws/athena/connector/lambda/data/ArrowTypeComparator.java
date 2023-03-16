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

import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.bouncycastle.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.ZonedDateTime;

/**
 * This utility class can be used to implement a comparator for various Apache Arrow typed values. It is mostly
 * used as part of our testing harness and notably does not support certain complex types (e.g. STRUCTs).
 */
public class ArrowTypeComparator
{
    private static final Logger logger = LoggerFactory.getLogger(ArrowTypeComparator.class);

    private ArrowTypeComparator() {}

    public static int compare(FieldReader reader, Object lhs, Object rhs)
    {
        return compare(reader.getField().getType(), lhs, rhs);
    }

    public static int compare(ArrowType arrowType, Object lhs, Object rhs)
    {
        if (lhs == null && rhs == null) {
            return 0;
        }
        else if (lhs == null) {
            return 1;
        }
        else if (rhs == null) {
            return -1;
        }

        Types.MinorType type = Types.getMinorTypeForArrowType(arrowType);
        switch (type) {
            case INT:
            case UINT4:
                return Integer.compare((int) lhs, (int) rhs);
            case TINYINT:
            case UINT1:
                return Byte.compare((byte) lhs, (byte) rhs);
            case SMALLINT:
                return Short.compare((short) lhs, (short) rhs);
            case UINT2:
                return Character.compare((char) lhs, (char) rhs);
            case BIGINT:
            case UINT8:
                return Long.compare((long) lhs, (long) rhs);
            case FLOAT8:
                return Double.compare((double) lhs, (double) rhs);
            case FLOAT4:
                return Float.compare((float) lhs, (float) rhs);
            case VARCHAR:
                return lhs.toString().compareTo(rhs.toString());
            case VARBINARY:
                return Arrays.compareUnsigned((byte[]) lhs, (byte[]) rhs);
            case DECIMAL:
                return ((BigDecimal) lhs).compareTo((BigDecimal) rhs);
            case BIT:
                return Boolean.compare((boolean) lhs, (boolean) rhs);
            case DATEMILLI:
                return ((java.time.LocalDateTime) lhs).compareTo((java.time.LocalDateTime) rhs);
            case DATEDAY:
                return ((Integer) lhs).compareTo((Integer) rhs);
            case TIMESTAMPMICROTZ:
            case TIMESTAMPMILLITZ:
                ArrowType.Timestamp actualArrowType = (ArrowType.Timestamp) arrowType;
                if (lhs instanceof Long) {
                    ZonedDateTime lhsZdt = DateTimeFormatterUtil.constructZonedDateTime(((Long) lhs).longValue(), actualArrowType);
                    ZonedDateTime rhsZdt = DateTimeFormatterUtil.constructZonedDateTime(((Long) rhs).longValue(), actualArrowType);
                    return lhsZdt.compareTo(rhsZdt);
                }
                else {
                    return ((java.time.LocalDateTime) lhs).compareTo((java.time.LocalDateTime) rhs);
                }
            case MAP:
            case LIST:
            case STRUCT: // struct maps to java.util.map
                //This could lead to thrashing if used to sort a collection
                if (lhs.equals(rhs)) {
                    return 0;
                }
                else if (lhs.hashCode() < rhs.hashCode()) {
                    return -1;
                }
                else {
                    return 1;
                }
            default:
                //logging because throwing in a comparator gets swallowed in many unit tests that use equality asserts
                logger.warn("compare: Unknown type " + type + " object: " + lhs.getClass());
                throw new IllegalArgumentException("Unknown type " + type + " object: " + lhs.getClass());
        }
    }
}
