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
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Optimized decimal extractor with caching and fast parsing
 */
public final class OptimizedDecimalExtractor implements DecimalExtractor
{
    private final String fieldName;
    private final int scale;
    private final MathContext mathContext;
    
    // Cache for frequently used BigDecimal values
    private static final Map<String, BigDecimal> DECIMAL_CACHE = new ConcurrentHashMap<>();
    private static final int MAX_CACHE_SIZE = 10000;
    
    // Pre-computed scale multipliers for common scales
    private static final BigDecimal[] SCALE_MULTIPLIERS = new BigDecimal[10];
    static {
        for (int i = 0; i < 10; i++) {
            SCALE_MULTIPLIERS[i] = BigDecimal.TEN.pow(i);
        }
    }
    
    public OptimizedDecimalExtractor(String fieldName, int scale)
    {
        this.fieldName = fieldName;
        this.scale = scale;
        this.mathContext = new MathContext(38, RoundingMode.HALF_UP);
    }
    
    @Override
    public void extract(Object context, NullableDecimalHolder dst) throws Exception
    {
        @SuppressWarnings("unchecked")
        Map<String, AttributeValue> item = (Map<String, AttributeValue>) context;
        AttributeValue value = item.get(fieldName);
        
        if (value != null && value.n() != null) {
            dst.isSet = 1;
            dst.value = parseOptimizedDecimal(value.n());
        }
        else {
            dst.isSet = 0;
        }
    }
    
    private BigDecimal parseOptimizedDecimal(String numStr)
    {
        // 1. Check cache first for frequently used values
        if (DECIMAL_CACHE.size() < MAX_CACHE_SIZE) {
            BigDecimal cached = DECIMAL_CACHE.get(numStr);
            if (cached != null) {
                return adjustScale(cached);
            }
        }
        
        // 2. Fast path for integers (no decimal point)
        if (numStr.indexOf('.') == -1) {
            BigDecimal result = parseInteger(numStr);
            cacheIfAppropriate(numStr, result);
            return adjustScale(result);
        }
        
        // 3. Optimized decimal parsing
        BigDecimal result = new BigDecimal(numStr, mathContext);
        cacheIfAppropriate(numStr, result);
        return adjustScale(result);
    }
    
    private BigDecimal parseInteger(String numStr)
    {
        // Fast path for small integers
        if (numStr.length() <= 9) {
            try {
                int intVal = Integer.parseInt(numStr);
                return BigDecimal.valueOf(intVal);
            }
            catch (NumberFormatException e) {
                // Fall through to BigDecimal
            }
        }
        
        return new BigDecimal(numStr);
    }
    
    private BigDecimal adjustScale(BigDecimal value)
    {
        if (value.scale() == scale) {
            return value;
        }
        
        // Use pre-computed multipliers for common scales
        if (scale < SCALE_MULTIPLIERS.length && value.scale() == 0) {
            return value.divide(SCALE_MULTIPLIERS[scale], scale, RoundingMode.HALF_UP);
        }
        
        return value.setScale(scale, RoundingMode.HALF_UP);
    }
    
    private void cacheIfAppropriate(String numStr, BigDecimal value)
    {
        // Only cache small, commonly used values
        if (numStr.length() <= 10 && DECIMAL_CACHE.size() < MAX_CACHE_SIZE) {
            DECIMAL_CACHE.put(numStr, value);
        }
    }
}
