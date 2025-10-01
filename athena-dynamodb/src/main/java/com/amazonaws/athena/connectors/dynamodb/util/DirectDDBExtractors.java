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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarBinaryExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Direct extractors that convert DynamoDB AttributeValues to Arrow without JSON serialization
 */
public final class DirectDDBExtractors
{
    private DirectDDBExtractors() {}
    
    public static class DirectVarCharExtractor implements VarCharExtractor
    {
        private final String fieldName;
        
        public DirectVarCharExtractor(String fieldName)
        {
            this.fieldName = fieldName;
        }
        
        @Override
        public void extract(Object context, NullableVarCharHolder dst) throws Exception
        {
            @SuppressWarnings("unchecked")
            Map<String, AttributeValue> item = (Map<String, AttributeValue>) context;
            AttributeValue value = item.get(fieldName);
            
            if (value != null && value.s() != null) {
                dst.isSet = 1;
                dst.value = value.s();
            }
            else {
                dst.isSet = 0;
            }
        }
    }
    
    public static class DirectDecimalExtractor implements DecimalExtractor
    {
        private final String fieldName;
        
        public DirectDecimalExtractor(String fieldName)
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
                    dst.value = new BigDecimal(numStr);
                    return;
                }
            }
            dst.isSet = 0;
        }
    }
    
    public static class DirectBitExtractor implements BitExtractor
    {
        private final String fieldName;
        
        public DirectBitExtractor(String fieldName)
        {
            this.fieldName = fieldName;
        }
        
        @Override
        public void extract(Object context, NullableBitHolder dst) throws Exception
        {
            @SuppressWarnings("unchecked")
            Map<String, AttributeValue> item = (Map<String, AttributeValue>) context;
            AttributeValue value = item.get(fieldName);
            
            if (value != null && value.bool() != null) {
                dst.isSet = 1;
                dst.value = value.bool() ? 1 : 0;
            }
            else {
                dst.isSet = 0;
            }
        }
    }
    
    public static class DirectVarBinaryExtractor implements VarBinaryExtractor
    {
        private final String fieldName;
        
        public DirectVarBinaryExtractor(String fieldName)
        {
            this.fieldName = fieldName;
        }
        
        @Override
        public void extract(Object context, NullableVarBinaryHolder dst) throws Exception
        {
            @SuppressWarnings("unchecked")
            Map<String, AttributeValue> item = (Map<String, AttributeValue>) context;
            AttributeValue value = item.get(fieldName);
            
            if (value != null && value.b() != null) {
                dst.isSet = 1;
                dst.value = value.b().asByteArray();
            }
            else {
                dst.isSet = 0;
            }
        }
    }
}
