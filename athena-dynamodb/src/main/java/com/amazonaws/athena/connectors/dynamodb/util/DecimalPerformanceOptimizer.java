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

import org.apache.arrow.vector.DecimalVector;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

/**
 * Advanced decimal performance optimizations for DynamoDB processing
 */
public final class DecimalPerformanceOptimizer
{
    private DecimalPerformanceOptimizer() {}
    
    /**
     * Strategy 1: Batch decimal processing with vectorized operations
     */
    public static void batchProcessDecimals(DecimalVector vector, 
                                          Map<String, AttributeValue>[] items, 
                                          String fieldName,
                                          int scale)
    {
        // Pre-allocate for better memory locality
        BigDecimal[] decimals = new BigDecimal[items.length];
        
        // Parse all decimals first (better CPU cache usage)
        for (int i = 0; i < items.length; i++) {
            AttributeValue value = items[i].get(fieldName);
            if (value != null && value.n() != null) {
                decimals[i] = fastParseDecimal(value.n(), scale);
            }
        }
        
        // Write to Arrow vector in batch
        for (int i = 0; i < decimals.length; i++) {
            if (decimals[i] != null) {
                vector.setSafe(i, decimals[i]);
            }
            else {
                vector.setNull(i);
            }
        }
    }
    
    /**
     * Strategy 2: Fast decimal parsing without BigDecimal for simple cases
     */
    public static BigDecimal fastParseDecimal(String numStr, int targetScale)
    {
        // Fast path for integers
        if (numStr.indexOf('.') == -1) {
            return parseIntegerFast(numStr, targetScale);
        }
        
        // Fast path for simple decimals
        if (numStr.length() <= 15) {
            return parseSimpleDecimal(numStr, targetScale);
        }
        
        // Fallback to BigDecimal for complex cases
        return new BigDecimal(numStr).setScale(targetScale, RoundingMode.HALF_UP);
    }
    
    private static BigDecimal parseIntegerFast(String numStr, int targetScale)
    {
        if (numStr.length() <= 9) {
            try {
                long longVal = Long.parseLong(numStr);
                return BigDecimal.valueOf(longVal, targetScale);
            }
            catch (NumberFormatException e) {
                // Fall through
            }
        }
        
        return new BigDecimal(numStr).setScale(targetScale, RoundingMode.HALF_UP);
    }
    
    private static BigDecimal parseSimpleDecimal(String numStr, int targetScale)
    {
        try {
            double doubleVal = Double.parseDouble(numStr);
            // Only use double path if no precision loss
            if (String.valueOf(doubleVal).equals(numStr)) {
                return BigDecimal.valueOf(doubleVal).setScale(targetScale, RoundingMode.HALF_UP);
            }
        }
        catch (NumberFormatException e) {
            // Fall through
        }
        
        return new BigDecimal(numStr).setScale(targetScale, RoundingMode.HALF_UP);
    }
    
    /**
     * Strategy 3: Optimized decimal writing with fast paths
     */
    public static void writeDecimalOptimized(DecimalVector vector, int index, String numStr, int scale)
    {
        // Fast path for integers - convert to BigDecimal with proper scale
        if (numStr.indexOf('.') == -1 && numStr.length() <= 15) {
            try {
                long longVal = Long.parseLong(numStr);
                BigDecimal decimal = BigDecimal.valueOf(longVal, 0).setScale(scale, RoundingMode.HALF_UP);
                vector.setSafe(index, decimal);
                return;
            }
            catch (NumberFormatException e) {
                // Fall through to BigDecimal
            }
        }
        
        // Standard BigDecimal path with optimized parsing
        BigDecimal decimal = fastParseDecimal(numStr, scale);
        vector.setSafe(index, decimal);
    }
    
    /**
     * Strategy 4: Memory pool for BigDecimal objects
     */
    private static final ThreadLocal<BigDecimal[]> DECIMAL_POOL = ThreadLocal.withInitial(() -> new BigDecimal[1000]);
    private static final ThreadLocal<Integer> POOL_INDEX = ThreadLocal.withInitial(() -> 0);
    
    public static BigDecimal getPooledDecimal(String numStr, int scale)
    {
        BigDecimal[] pool = DECIMAL_POOL.get();
        int index = POOL_INDEX.get();
        
        if (index < pool.length) {
            BigDecimal decimal = new BigDecimal(numStr).setScale(scale, RoundingMode.HALF_UP);
            pool[index] = decimal;
            POOL_INDEX.set(index + 1);
            return decimal;
        }
        
        // Reset pool when full
        POOL_INDEX.set(0);
        return new BigDecimal(numStr).setScale(scale, RoundingMode.HALF_UP);
    }
}
