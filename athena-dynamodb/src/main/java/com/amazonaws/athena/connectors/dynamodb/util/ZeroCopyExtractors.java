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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;

/**
 * Zero-copy extractors that minimize object allocation and memory copying
 */
public final class ZeroCopyExtractors
{
    private ZeroCopyExtractors() {}
    
    /**
     * Zero-copy string extractor - reuses string references without copying
     */
    public static class ZeroCopyVarCharExtractor implements VarCharExtractor
    {
        private final String fieldName;
        
        public ZeroCopyVarCharExtractor(String fieldName)
        {
            this.fieldName = fieldName;
        }
        
        @Override
        public void extract(Object context, NullableVarCharHolder dst) throws Exception
        {
            @SuppressWarnings("unchecked")
            Map<String, AttributeValue> item = (Map<String, AttributeValue>) context;
            AttributeValue value = item.get(fieldName);
            
            if (value != null) {
                String str = value.s();
                if (str != null) {
                    dst.isSet = 1;
                    // Zero-copy: reuse string reference directly
                    dst.value = str;
                    return;
                }
            }
            dst.isSet = 0;
        }
    }
    
    /**
     * Pooled string extractor to reduce GC pressure
     */
    public static class PooledVarCharExtractor implements VarCharExtractor
    {
        private final String fieldName;
        private static final ThreadLocal<StringBuilder> STRING_BUILDER = 
            ThreadLocal.withInitial(() -> new StringBuilder(256));
        
        public PooledVarCharExtractor(String fieldName)
        {
            this.fieldName = fieldName;
        }
        
        @Override
        public void extract(Object context, NullableVarCharHolder dst) throws Exception
        {
            @SuppressWarnings("unchecked")
            Map<String, AttributeValue> item = (Map<String, AttributeValue>) context;
            AttributeValue value = item.get(fieldName);
            
            if (value != null) {
                String str = value.s();
                if (str != null) {
                    dst.isSet = 1;
                    
                    // For small strings, use direct reference
                    if (str.length() <= 64) {
                        dst.value = str;
                    } else {
                        // For large strings, use pooled StringBuilder to avoid large object allocation
                        StringBuilder sb = STRING_BUILDER.get();
                        sb.setLength(0);
                        sb.append(str);
                        dst.value = sb.toString();
                    }
                    return;
                }
            }
            dst.isSet = 0;
        }
    }
}