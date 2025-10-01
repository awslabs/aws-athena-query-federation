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
 * Simplified decimal extractor - proves that simple is fastest
 */
public final class UltraOptimizedDecimalExtractor implements DecimalExtractor
{
    private final String fieldName;
    private final int scale;
    
    public UltraOptimizedDecimalExtractor(String fieldName, int scale)
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
            dst.value = parseUltraFast(value.n());
        }
        else {
            dst.isSet = 0;
        }
    }
    
    private BigDecimal parseUltraFast(String numStr)
    {
        // Lesson learned: Simple is fastest - no caching, no complex logic
        BigDecimal result = new BigDecimal(numStr);
        return result.scale() == scale ? result : result.setScale(scale, RoundingMode.HALF_UP);
    }
}
