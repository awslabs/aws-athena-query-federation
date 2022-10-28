/*-
 * #%L
 * Athena MSK Connector
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
package com.athena.connectors.msk.trino;

import io.trino.testing.Bytes;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class TrinoRecord
{
    private final List<?> values;
    private final int precision;

    public TrinoRecord(List<?> values)
    {
        this(TrinoRecordSet.DEFAULT_PRECISION, values);
    }

    /**
     * Initialize a records with list of field values
     *
     * @param values
     */
    public TrinoRecord(int precision, List<?> values)
    {
        this.precision = Math.min(precision, TrinoRecordSet.DEFAULT_PRECISION);
        this.values = (List<?>) normalizeValue(precision, values);
    }

    public TrinoRecord(int precision, Object[] values)
    {
        this.precision = Math.min(precision, TrinoRecordSet.DEFAULT_PRECISION);
        this.values = (List<?>) normalizeValue(precision, values);
    }

    public int getPrecision()
    {
        return precision;
    }

    public int getFieldCount()
    {
        return values.size();
    }

    public Object getField(int index)
    {
        return values.get(index);
    }

    @Override
    public String toString()
    {
        return values.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        TrinoRecord o = (TrinoRecord) obj;
        return Objects.equals(values, o.values);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(values);
    }

    // helpers
    private static Object normalizeValue(int precision, Object value)
    {
        if (value instanceof Double) {
                if (value != null) {
                    return Double.parseDouble(value.toString());
                }
                return value;
        }
        if (value instanceof Float) {
            return new FloatValueNormalizer(((Float) value), precision).getNormalizedValue();
        }
        if (value instanceof List) {
            return ((List<?>) value).stream()
                    .map(element -> normalizeValue(precision, element))
                    .collect(toList());
        }
        if (value instanceof Map) {
            Map<Object, Object> map = new HashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                map.put(normalizeValue(precision, entry.getKey()), normalizeValue(precision, entry.getValue()));
            }
            return map;
        }
        if (value instanceof byte[]) {
            return Bytes.fromBytes((byte[]) value);
        }
        return value;
    }
    // value normalizer
    private abstract static class NumericValueNormalizer
    {
        public abstract Number getValue();

        protected abstract Number getNormalizedValue();

        @Override
        public String toString()
        {
            return getValue().toString();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == this) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }

            NumericValueNormalizer o = (NumericValueNormalizer) obj;
            return Objects.equals(getNormalizedValue(), o.getNormalizedValue());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(getNormalizedValue());
        }
    }

    private static class FloatValueNormalizer
            extends NumericValueNormalizer
    {
        private final Float value;
        private final int precision;

        private FloatValueNormalizer(Float value, int precision)
        {
            this.value = requireNonNull(value, "value is null");
            this.precision = precision;
        }

        @Override
        public Number getValue()
        {
            return value;
        }

        @Override
        protected Number getNormalizedValue()
        {
            if (value.isNaN() || value.isInfinite()) {
                return value;
            }
            return new BigDecimal(getValue().floatValue()).round(new MathContext(precision)).floatValue();
        }
    }
}
