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
package com.amazonaws.athena.connectors.lark.base.resolver;

import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Implements the {@link FieldResolver} interface for resolving field values from Lark data.
 * <p>
 * **WARNING:** Extremely simplified based on user assertion. Assumes originalValue is always Map or null.
 * Relies solely on direct key matching within Maps. List handling removed.
 * This may cause failures if assumptions are incorrect or data structure changes.
 */
public class LarkBaseFieldResolver implements FieldResolver
{
    private static final Logger logger = LoggerFactory.getLogger(LarkBaseFieldResolver.class);

    @Override
    public Object getFieldValue(Field field, Object dataContext)
    {
        if (dataContext == null) {
            return null;
        }

        String fieldNameInFocus = field.getName();

        if (dataContext instanceof Map<?, ?> mapDataContext) {
            Object value = mapDataContext.get(fieldNameInFocus);
            logger.info("LarkBaseFieldResolver: Field='{}', DataContext is Map, Value for key='{}' is '{}' (type: {})",
                    fieldNameInFocus, fieldNameInFocus, value, (value != null ? value.getClass().getName() : "null"));
            return value;
        }
        else {
            logger.info("LarkBaseFieldResolver: Field='{}', DataContext is NOT a Map (type: {}). Returning dataContext as is.",
                    fieldNameInFocus, dataContext.getClass().getName());
            return dataContext;
        }
    }
}
