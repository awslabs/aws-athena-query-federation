/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.neptune.propertygraph.rowwriters;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Normalizes Gremlin result field values so row writers can handle both
 * valueMap-style results (Map with ArrayList values) and project().by()-style
 * results (Map with scalar or Map values) without ClassCastException.
 */
public final class FieldValueNormalizer
{
    private FieldValueNormalizer()
    {
    }

    /**
     * Converts a raw field value from a Gremlin result into a list suitable for
     * row writers. Handles: valueMap() lists, project().by() scalars, and
     * .by(valueMap()) Map values (e.g. all_properties).
     *
     * @param fieldValue raw value (may be List, Map, scalar, or null)
     * @return null if fieldValue is null; otherwise a list of one or more
     * elements that can be read by row writers (Map is serialized to string)
     */
    @SuppressWarnings("unchecked")
    public static List<Object> toValueList(Object fieldValue)
    {
        if (fieldValue == null) {
            return null;
        }
        if (fieldValue instanceof List) {
            return (List<Object>) fieldValue;
        }
        if (fieldValue instanceof Map) {
            return Collections.singletonList(mapToString((Map<?, ?>) fieldValue));
        }
        return Collections.singletonList(fieldValue);
    }

    private static String mapToString(Map<?, ?> map)
    {
        if (map == null || map.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(entry.getKey()).append("=").append(entry.getValue());
        }
        return "{" + sb + "}";
    }
}
