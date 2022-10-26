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
package com.amazonaws.athena.storage.datasource.parquet.filter;

import com.amazonaws.athena.storage.datasource.exception.UncheckedStorageDatasourceException;

import java.util.Date;

public class ObjectComparator
{
    /**
     * Private constructor, as this is the utility type class and all methods are static
     */
    private ObjectComparator()
    {
    }

    /**
     * Compares a value of any type with another which is an instance of {@link Comparable}
     *
     * @param value Value to be compared
     * @param with  Another value to be compared with
     * @return 0 when both are equal, 1 when the value is greater than the other, -1 when the value is less than the other
     */
    public static synchronized int compare(Object value, Comparable<?> with)
    {
        if (value instanceof String) {
            return compareString(with, (String) value);
        }
        else if (value instanceof Integer) {
            return compareInt(with, (Integer) value);
        }
        else if (value instanceof Long) {
            return compareLong(with, (Long) value);
        }
        else if (value instanceof Float) {
            return compareFloat(with, (Float) value);
        }
        else if (value instanceof Double) {
            return compareDouble(with, (Double) value);
        }
        else if (value instanceof Date) {
            return compareDate(with, (Date) value);
        }
        throw new UncheckedStorageDatasourceException("Currently comparing is yet not supported for type "
                + (value == null ? "null" : value.getClass().getSimpleName()));
    }

    /**
     * Compares a String with another which is an instance of {@link Comparable}, which convertible to String
     *
     * @param with To be compared with
     * @param str  Another String value to be compared
     * @return 0 when both are equal, 1 when the value is greater than the other, -1 when the value is less than the other
     */
    private static int compareString(Comparable<?> with, String str)
    {
        return str.compareTo(((String) with));
    }

    /**
     * Compares an Integer with another which is an instance of {@link Comparable}, which convertible to Integer
     *
     * @param with    To be compared with
     * @param integer Another Integer value to be compared
     * @return 0 when both are equal, 1 when the value is greater than the other, -1 when the value is less than the other
     */
    private static int compareInt(Comparable<?> with, Integer integer)
    {
        return integer.compareTo(((Integer) with));
    }

    /**
     * Compares a Long with another which is an instance of {@link Comparable}, which convertible to Long
     *
     * @param with    To be compared with
     * @param longVal Another Long value to be compared
     * @return 0 when both are equal, 1 when the value is greater than the other, -1 when the value is less than the other
     */
    private static int compareLong(Comparable<?> with, Long longVal)
    {
        return longVal.compareTo(((Long) with));
    }

    /**
     * Compares a Float with another which is an instance of {@link Comparable}, which convertible to Float
     *
     * @param with     To be compared with
     * @param floatVal Another Float value to be compared
     * @return 0 when both are equal, 1 when the value is greater than the other, -1 when the value is less than the other
     */
    private static int compareFloat(Comparable<?> with, Float floatVal)
    {
        return floatVal.compareTo(((Float) with));
    }

    /**
     * Compares a Double with another which is an instance of {@link Comparable}, which convertible to Double
     *
     * @param with      To be compared with
     * @param doubleVal Another Double value to be compared
     * @return 0 when both are equal, 1 when the value is greater than the other, -1 when the value is less than the other
     */
    private static int compareDouble(Comparable<?> with, Double doubleVal)
    {
        return doubleVal.compareTo(((Double) with));
    }

    /**
     * Compares a {@link Date} with another which is an instance of {@link Comparable}, which convertible to {@link Date}
     *
     * @param with To be compared with
     * @param date Another instance of {@link Date} value to be compared
     * @return 0 when both are equal, 1 when the value is greater than the other, -1 when the value is less than the other
     */
    private static int compareDate(Comparable<?> with, Date date)
    {
        return date.compareTo(((Date) with));
    }
}
