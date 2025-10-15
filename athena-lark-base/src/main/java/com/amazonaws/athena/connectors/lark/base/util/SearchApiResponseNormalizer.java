/*-
 * #%L
 * athena-lark-base
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.lark.base.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Normalizes Search API responses to match the structure expected by List API extractors.
 * <p>
 * The Search API has a different response format:
 * - TEXT fields: [{"text": "value", "type": "text"}] instead of plain string
 * - FORMULA fields: {"type": N, "value": [...]} instead of direct values
 * - NUMBER/CURRENCY: numbers instead of strings
 * - USER fields: arrays instead of objects
 * - LINK fields: {"link_record_ids": [...]} instead of full structure
 */
public final class SearchApiResponseNormalizer
{
    private static final Logger logger = LoggerFactory.getLogger(SearchApiResponseNormalizer.class);

    private SearchApiResponseNormalizer()
    {
    }

    /**
     * Normalizes a single record's fields from Search API format to List API format.
     *
     * @param searchFields The fields map from Search API response
     * @return Normalized fields map compatible with existing extractors
     */
    public static Map<String, Object> normalizeRecordFields(Map<String, Object> searchFields)
    {
        if (searchFields == null) {
            return new HashMap<>();
        }

        Map<String, Object> normalized = new HashMap<>();

        for (Map.Entry<String, Object> entry : searchFields.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();

            Object normalizedValue = normalizeFieldValue(fieldName, value);
            normalized.put(fieldName, normalizedValue);
        }

        return normalized;
    }

    /**
     * Normalizes a single field value based on its structure.
     */
    @SuppressWarnings("unchecked")
    private static Object normalizeFieldValue(String fieldName, Object value)
    {
        if (value == null) {
            return null;
        }

        // Handle wrapped formula/lookup format: {"type": N, "value": [...]}
        if (value instanceof Map) {
            Map<String, Object> mapValue = (Map<String, Object>) value;

            // Check if this is a wrapped formula/lookup field
            if (mapValue.containsKey("type") && mapValue.containsKey("value")) {
                Object unwrappedValue = mapValue.get("value");
                logger.debug("Unwrapping formula/lookup field '{}': type={}, value={}",
                        fieldName, mapValue.get("type"), unwrappedValue);

                // The unwrapped value might still need normalization
                return normalizeFieldValue(fieldName, unwrappedValue);
            }

            // Handle link fields: {"link_record_ids": [...]}
            if (mapValue.containsKey("link_record_ids")) {
                // For compatibility, we could expand this, but current extractors
                // should handle the simplified format fine
                return value;
            }

            // Other map values (location, url, etc.) pass through unchanged
            return value;
        }

        // Handle arrays
        if (value instanceof List<?> listValue) {
            if (listValue.isEmpty()) {
                return value;
            }

            Object firstItem = listValue.get(0);

            if (firstItem instanceof Map) {
                Map<String, Object> firstMap = (Map<String, Object>) firstItem;

                // CREATED_USER and MODIFIED_USER: Search API returns arrays but they're STRUCT types
                // Need to extract first element for compatibility
                if (firstMap.containsKey("id") && firstMap.containsKey("name") &&
                        (fieldName.contains("created_user") || fieldName.contains("modified_user"))) {
                    logger.debug("Converting CREATED_USER/MODIFIED_USER array to single object for field '{}'", fieldName);
                    return firstItem;
                }

                // Check if this is a text object array
                if (firstMap.containsKey("text") && firstMap.containsKey("type") &&
                        "text".equals(firstMap.get("type"))) {
                    // If single item, extract just the text value (for TEXT fields)
                    if (listValue.size() == 1) {
                        String textValue = (String) firstMap.get("text");
                        logger.debug("Extracting text from array for field '{}': {}", fieldName, textValue);
                        return textValue;
                    }

                    // Multiple items: keep as array (for FORMULA, LOOKUP fields)
                    logger.debug("Keeping array format for field '{}' with {} items", fieldName, listValue.size());
                    return value;
                }
            }

            // Other list values pass through unchanged (multi-select, attachments, regular user arrays, etc.)
            return value;
        }

        // Primitive values pass through unchanged (numbers, booleans, strings)
        return value;
    }
}
