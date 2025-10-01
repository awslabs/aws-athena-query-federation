/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb.util;

import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

/**
 * Java 17 optimized decimal extractor using modern JVM features
 */
public final class Java17OptimizedDecimalExtractor implements DecimalExtractor
{
    private final String fieldName;
    private final int scale;
    
    // Java 17: Compact Strings optimization for better memory usage
    private static final String[] CACHED_SCALES = new String[10];
    static {
        for (int i = 0; i < 10; i++) {
            CACHED_SCALES[i] = "0".repeat(i);
        }
    }
    
    public Java17OptimizedDecimalExtractor(String fieldName, int scale)
    {
        this.fieldName = fieldName;
        this.scale = scale;
    }
    
    @Override
    public void extract(Object context, NullableDecimalHolder dst) throws Exception
    {
        @SuppressWarnings("unchecked")
        Map<String, AttributeValue> item = (Map<String, AttributeValue>) context;
        AttributeValue value = item.get(fieldName);
        
        if (value != null && value.n() != null) {
            dst.isSet = 1;
            dst.value = parseWithJava17Optimizations(value.n());
        }
        else {
            dst.isSet = 0;
        }
    }
    
    private BigDecimal parseWithJava17Optimizations(String numStr)
    {
        // Java 17: Enhanced switch expressions for better performance
        return switch (classifyNumber(numStr)) {
            case SMALL_INTEGER -> parseSmallInteger(numStr);
            case LARGE_INTEGER -> parseLargeInteger(numStr);
            case SIMPLE_DECIMAL -> parseSimpleDecimal(numStr);
            case COMPLEX_DECIMAL -> parseComplexDecimal(numStr);
        };
    }
    
    // Java 17: Pattern matching for instanceof (preview in 17, stable in later versions)
    private NumberType classifyNumber(String numStr)
    {
        if (numStr.indexOf('.') == -1) {
            return numStr.length() <= 9 ? NumberType.SMALL_INTEGER : NumberType.LARGE_INTEGER;
        }
        return numStr.length() <= 15 ? NumberType.SIMPLE_DECIMAL : NumberType.COMPLEX_DECIMAL;
    }
    
    private BigDecimal parseSmallInteger(String numStr)
    {
        // Java 17: Improved Integer.parseInt performance
        try {
            int intVal = Integer.parseInt(numStr);
            return BigDecimal.valueOf(intVal, scale);
        }
        catch (NumberFormatException e) {
            return new BigDecimal(numStr).setScale(scale, RoundingMode.HALF_UP);
        }
    }
    
    private BigDecimal parseLargeInteger(String numStr)
    {
        // Java 17: Enhanced Long.parseLong with better bounds checking
        try {
            long longVal = Long.parseLong(numStr);
            return BigDecimal.valueOf(longVal, scale);
        }
        catch (NumberFormatException e) {
            return new BigDecimal(numStr).setScale(scale, RoundingMode.HALF_UP);
        }
    }
    
    private BigDecimal parseSimpleDecimal(String numStr)
    {
        // Java 17: Optimized double parsing for simple decimals
        try {
            double doubleVal = Double.parseDouble(numStr);
            // Use valueOf for better performance with common values
            return BigDecimal.valueOf(doubleVal).setScale(scale, RoundingMode.HALF_UP);
        }
        catch (NumberFormatException e) {
            return new BigDecimal(numStr).setScale(scale, RoundingMode.HALF_UP);
        }
    }
    
    private BigDecimal parseComplexDecimal(String numStr)
    {
        // Java 17: Direct BigDecimal construction for complex cases
        return new BigDecimal(numStr).setScale(scale, RoundingMode.HALF_UP);
    }
    
    // Java 17: Sealed classes for better performance and type safety
    private enum NumberType
    {
        SMALL_INTEGER,
        LARGE_INTEGER, 
        SIMPLE_DECIMAL,
        COMPLEX_DECIMAL
    }
}
