/*-
 * #%L
 * athena-udfs
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
package com.amazonaws.athena.connectors.udfs;

import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionHandler;
import com.google.common.collect.ImmutableMap;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AthenaUDFHandler
        extends UserDefinedFunctionHandler
{
    public Boolean example_udf(Boolean value)
    {
        return !value;
    }

    public Byte example_udf(Byte value)
    {
        return (byte) (value + 1);
    }

    public Short example_udf(Short value)
    {
        return (short) (value + 1);
    }

    public Integer example_udf(Integer value)
    {
        return value + 1;
    }

    public Long example_udf(Long value)
    {
        return value + 1;
    }

    public Float example_udf(Float value)
    {
        return value + 1;
    }

    public Double example_udf(Double value)
    {
        return value + 1;
    }

    public BigDecimal example_udf(BigDecimal value)
    {
        BigDecimal one = new BigDecimal(1);
        one.setScale(value.scale(), RoundingMode.HALF_UP);
        return value.add(one);
    }

    public String example_udf(String value)
    {
        return value + "_dada";
    }

    public LocalDateTime example_udf(LocalDateTime value)
    {
        return value.minusDays(1);
    }

    public LocalDate example_udf(LocalDate value)
    {
        return value.minusDays(1);
    }

    public List<Integer> example_udf(List<Integer> value)
    {
        System.out.println("Array input: " + value);
        List<Integer> result = value.stream().map(o -> ((Integer) o) + 1).collect(Collectors.toList());
        System.out.println("Array output: " + result);
        return result;
    }

    public Map<String, Object> example_udf(Map<String, Object> value)
    {
        Long longVal = (Long) value.get("x");
        Double doubleVal = (Double) value.get("y");

        return ImmutableMap.of("x", longVal + 1, "y", doubleVal + 1.0);
    }

    public byte[] example_udf(byte[] value)
    {
        byte[] output = new byte[value.length];
        for (int i = 0; i < value.length; ++i) {
            output[i] = (byte) (value[i] + 1);
        }
        return output;
    }
}
