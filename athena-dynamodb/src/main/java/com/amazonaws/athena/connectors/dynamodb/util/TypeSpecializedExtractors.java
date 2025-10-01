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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableBigIntHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableIntHolder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Type-specialized extractors for common DynamoDB numeric patterns
 */
public final class TypeSpecializedExtractors
{
    private TypeSpecializedExtractors() {}
    
    /**
     * Optimized integer extractor - avoids BigDecimal for integer values
     */
    public static class OptimizedIntExtractor implements IntExtractor
    {
        private final String fieldName;
        
        public OptimizedIntExtractor(String fieldName)
        {
            this.fieldName = fieldName;
        }
        
        @Override
        public void extract(Object context, NullableIntHolder dst) throws Exception
        {
            @SuppressWarnings("unchecked")
            Map<String, AttributeValue> item = (Map<String, AttributeValue>) context;
            AttributeValue value = item.get(fieldName);
            
            if (value != null) {
                String numStr = value.n();
                if (numStr != null) {
                    try {
                        // Direct integer parsing - much faster than BigDecimal
                        int intVal = Integer.parseInt(numStr);
                        dst.isSet = 1;
                        dst.value = intVal;
                        return;
                    } catch (NumberFormatException e) {
                        // Fall through to null
                    }
                }
            }
            dst.isSet = 0;
        }
    }
    
    /**
     * Optimized long extractor for large integers
     */
    public static class OptimizedBigIntExtractor implements BigIntExtractor
    {
        private final String fieldName;
        
        public OptimizedBigIntExtractor(String fieldName)
        {
            this.fieldName = fieldName;
        }
        
        @Override
        public void extract(Object context, NullableBigIntHolder dst) throws Exception
        {
            @SuppressWarnings("unchecked")
            Map<String, AttributeValue> item = (Map<String, AttributeValue>) context;
            AttributeValue value = item.get(fieldName);
            
            if (value != null) {
                String numStr = value.n();
                if (numStr != null) {
                    try {
                        // Direct long parsing - faster than BigDecimal for integers
                        long longVal = Long.parseLong(numStr);
                        dst.isSet = 1;
                        dst.value = longVal;
                        return;
                    } catch (NumberFormatException e) {
                        // Fall through to null
                    }
                }
            }
            dst.isSet = 0;
        }
    }
    
    /**
     * Smart decimal extractor that chooses optimal parsing strategy
     */
    public static class SmartDecimalExtractor implements DecimalExtractor
    {
        private final String fieldName;
        
        public SmartDecimalExtractor(String fieldName)
        {
            this.fieldName = fieldName;
        }
        
        @Override
        public void extract(Object context, NullableDecimalHolder dst) throws Exception
        {
            @SuppressWarnings("unchecked")
            Map<String, AttributeValue> item = (Map<String, AttributeValue>) context;
            AttributeValue value = item.get(fieldName);
            
            if (value != null) {
                String numStr = value.n();
                if (numStr != null) {
                    dst.isSet = 1;
                    
                    // Smart parsing: choose fastest method based on string characteristics
                    if (numStr.indexOf('.') == -1) {
                        // Integer - use valueOf for better performance
                        try {
                            if (numStr.length() <= 9) {
                                dst.value = BigDecimal.valueOf(Integer.parseInt(numStr));
                            } else {
                                dst.value = BigDecimal.valueOf(Long.parseLong(numStr));
                            }
                            return;
                        } catch (NumberFormatException e) {
                            // Fall through to BigDecimal constructor
                        }
                    }
                    
                    // Decimal - use constructor
                    dst.value = new BigDecimal(numStr);
                    return;
                }
            }
            dst.isSet = 0;
        }
    }
}