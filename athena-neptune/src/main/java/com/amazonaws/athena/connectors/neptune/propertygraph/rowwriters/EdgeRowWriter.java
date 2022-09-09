/*-
 * #%L
 * athena-neptune
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
package com.amazonaws.athena.connectors.neptune.propertygraph.rowwriters;

import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter.RowWriterBuilder;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connectors.neptune.propertygraph.Enums.SpecialKeys;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class is a Utility class to create Extractors for each field type as per
 * Schema
 */
public final class EdgeRowWriter 
{
    private EdgeRowWriter() 
    {
        // Empty private constructor
    }

    public static void writeRowTemplate(RowWriterBuilder rowWriterBuilder, Field field) 
    {
        ArrowType arrowType = field.getType();
        Types.MinorType minorType = Types.getMinorTypeForArrowType(arrowType);
        Boolean enableCaseinsensitivematch = (System.getenv("enable_caseinsensitivematch") == null) ? true : Boolean.parseBoolean(System.getenv("enable_caseinsensitivematch"));

        switch (minorType) {
            case BIT:
                rowWriterBuilder.withExtractor(field.getName(),
                        (BitExtractor) (Object context, NullableBitHolder value) -> {
                            Map<String, Object> obj = (Map<String, Object>) contextAsMap(context, enableCaseinsensitivematch);
                            Object fieldValue = obj.get(field.getName());
                            value.isSet = 0;

                            if (fieldValue != null && !(fieldValue.toString().trim().isEmpty())) {
                                Boolean booleanValue = Boolean.parseBoolean(fieldValue.toString());
                                value.value = booleanValue ? 1 : 0;
                                value.isSet = 1;
                            }
                        });
                break;

            case VARCHAR:
                rowWriterBuilder.withExtractor(field.getName(),
                        (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
                            Map<String, Object> obj = (Map<String, Object>) contextAsMap(context, enableCaseinsensitivematch);
                            String fieldName = field.getName();
                            value.isSet = 0;
                            Object fieldValue = obj.get(fieldName);
                            
                            if (fieldValue != null) {
                                value.value = fieldValue.toString();
                                value.isSet = 1;
                            }
                        });
                break;

            case DATEMILLI:
                rowWriterBuilder.withExtractor(field.getName(),
                        (DateMilliExtractor) (Object context, NullableDateMilliHolder value) -> {
                            Map<String, Object> obj = (Map<String, Object>) contextAsMap(context, enableCaseinsensitivematch);
                            Object fieldValue = obj.get(field.getName());
                            value.isSet = 0;

                            if (fieldValue != null && !(fieldValue.toString().trim().isEmpty())) {
                                value.value = ((Date) fieldValue).getTime();
                                value.isSet = 1;
                            }
                        });
                break;

            case INT:
                rowWriterBuilder.withExtractor(field.getName(),
                        (IntExtractor) (Object context, NullableIntHolder value) -> {
                            Map<String, Object> obj = (Map<String, Object>) contextAsMap(context, enableCaseinsensitivematch);
                            Object fieldValue = obj.get(field.getName());
                            value.isSet = 0;

                            if (fieldValue != null && !(fieldValue.toString().trim().isEmpty())) {
                                value.value = Integer.parseInt(fieldValue.toString());
                                value.isSet = 1;
                            }
                        });
                break;

            case BIGINT:
                rowWriterBuilder.withExtractor(field.getName(),
                        (BigIntExtractor) (Object context, NullableBigIntHolder value) -> {
                            Map<String, Object> obj = (Map<String, Object>) contextAsMap(context, enableCaseinsensitivematch);
                            Object fieldValue = obj.get(field.getName());
                            value.isSet = 0;

                            if (fieldValue != null && !(fieldValue.toString().trim().isEmpty())) {
                                value.value = Long.parseLong(fieldValue.toString());
                                value.isSet = 1;
                            }
                        });
                break;

            case FLOAT4:
                rowWriterBuilder.withExtractor(field.getName(),
                        (Float4Extractor) (Object context, NullableFloat4Holder value) -> {
                            Map<String, Object> obj = (Map<String, Object>) contextAsMap(context, enableCaseinsensitivematch);
                            Object fieldValue = obj.get(field.getName());
                            value.isSet = 0;

                            if (fieldValue != null && !(fieldValue.toString().trim().isEmpty())) {
                                value.value = Float.parseFloat(fieldValue.toString());
                                value.isSet = 1;
                            }
                        });
                break;

            case FLOAT8:
                rowWriterBuilder.withExtractor(field.getName(),
                        (Float8Extractor) (Object context, NullableFloat8Holder value) -> {
                            Map<String, Object> obj = (Map<String, Object>) contextAsMap(context, enableCaseinsensitivematch);
                            Object fieldValue = obj.get(field.getName());
                            value.isSet = 0;

                            if (fieldValue != null && !fieldValue.toString().trim().isEmpty()) {
                                value.value = Double.parseDouble(fieldValue.toString());
                                value.isSet = 1;
                            }
                        });

                break;
        }
    }

    private static Map<String, Object> contextAsMap(Object context, boolean caseInsensitive)
    {
        Map<String, Object> contextAsMap = (Map<String, Object>) context;
        Object fieldValueID = contextAsMap.get(T.id);
        
        if (fieldValueID != null) {
            Object fieldValueIN = ((LinkedHashMap) contextAsMap.get(Direction.IN)).get(T.id);
            Object fieldValueOUT = ((LinkedHashMap) contextAsMap.get(Direction.OUT)).get(T.id);

            contextAsMap.remove(T.id);
            contextAsMap.remove(T.label);
            contextAsMap.remove(Direction.IN);
            contextAsMap.remove(Direction.OUT);

            contextAsMap.put(SpecialKeys.ID.toString(), fieldValueID);
            contextAsMap.put(SpecialKeys.IN.toString(), fieldValueIN);
            contextAsMap.put(SpecialKeys.OUT.toString(), fieldValueOUT);         
        } 

        if (!caseInsensitive) {
            return contextAsMap;
        }
 

        TreeMap<String, Object> caseInsensitiveMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitiveMap.putAll(contextAsMap);
        return caseInsensitiveMap;
    }
}
