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
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class is a Utility class to create Extractors for each field type as per
 * Schema
 */
public final class VertexRowWriter 
{
    private VertexRowWriter() 
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
                            ArrayList<Object> objValues = (ArrayList) obj.get(field.getName());

                            value.isSet = 0;
                            if (objValues != null && objValues.get(0) != null && !(objValues.get(0).toString().trim().isEmpty())) {
                                Boolean booleanValue = Boolean.parseBoolean(objValues.get(0).toString());
                                value.value = booleanValue ? 1 : 0;
                                value.isSet = 1;
                            }
                        });
                break;

            case VARCHAR:
                rowWriterBuilder.withExtractor(field.getName(),
                        (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
                            String fieldName = field.getName();
                            Map<String, Object> obj = (Map<String, Object>) contextAsMap(context, enableCaseinsensitivematch);

                            value.isSet = 0;
                            // check for special keys and parse them separately
                            if (fieldName.equals(SpecialKeys.ID.toString().toLowerCase())) {
                                Object fieldValue = obj.get(SpecialKeys.ID.toString());
                                if (fieldValue != null) {
                                    value.value = fieldValue.toString();
                                    value.isSet = 1;
                                }
                            } 
                            else {
                                ArrayList<Object> objValues = (ArrayList) obj.get(fieldName);
                                if (objValues != null && objValues.get(0) != null) {
                                    value.value = objValues.get(0).toString();
                                    value.isSet = 1;
                                }
                            }
                        });
                break;

            case DATEMILLI:

                rowWriterBuilder.withExtractor(field.getName(),
                        (DateMilliExtractor) (Object context, NullableDateMilliHolder value) -> {
                            String fieldName = field.getName();
                            Map<String, Object> obj = (Map<String, Object>) contextAsMap(context, enableCaseinsensitivematch);
                            ArrayList<Object> objValues = (ArrayList) obj.get(fieldName);

                            value.isSet = 0;
                            if (objValues != null && (objValues.get(0) != null) && !(objValues.get(0).toString().trim().isEmpty())) {
                                value.value = ((Date) objValues.get(0)).getTime();
                                value.isSet = 1;
                            }
                        });
                break;

            case INT:
                rowWriterBuilder.withExtractor(field.getName(),
                        (IntExtractor) (Object context, NullableIntHolder value) -> {
                            String fieldName = field.getName();
                            Map<String, Object> obj = (Map<String, Object>) contextAsMap(context, enableCaseinsensitivematch);
                            ArrayList<Object> objValues = (ArrayList) obj.get(fieldName);

                            value.isSet = 0;

                            if (objValues != null && objValues.get(0) != null && !(objValues.get(0).toString().trim().isEmpty())) {
                                value.value = Integer.parseInt(objValues.get(0).toString());
                                value.isSet = 1;
                            }
                        });
                break;

            case BIGINT:
                rowWriterBuilder.withExtractor(field.getName(),
                        (BigIntExtractor) (Object context, NullableBigIntHolder value) -> {
                            String fieldName = field.getName();
                            Map<String, Object> obj = (Map<String, Object>) contextAsMap(context, enableCaseinsensitivematch);
                            ArrayList<Object> objValues = (ArrayList) obj.get(fieldName);

                            value.isSet = 0;
                            if (objValues != null && objValues.get(0) != null && !(objValues.get(0).toString().trim().isEmpty())) {
                                value.value = Long.parseLong(objValues.get(0).toString());
                                value.isSet = 1;
                            }
                        });
                break;

            case FLOAT4:
                rowWriterBuilder.withExtractor(field.getName(),
                        (Float4Extractor) (Object context, NullableFloat4Holder value) -> {
                            Map<String, Object> obj = (Map<String, Object>) contextAsMap(context, enableCaseinsensitivematch);
                            ArrayList<Object> objValues = (ArrayList) obj.get(field.getName());

                            value.isSet = 0;
                            if (objValues != null && objValues.get(0) != null && !(objValues.get(0).toString().trim().isEmpty())) {
                                value.value = Float.parseFloat(objValues.get(0).toString());
                                value.isSet = 1;
                            }
                        });
                break;

            case FLOAT8:
                rowWriterBuilder.withExtractor(field.getName(),
                        (Float8Extractor) (Object context, NullableFloat8Holder value) -> {
                            Map<String, Object> obj = (Map<String, Object>) contextAsMap(context, enableCaseinsensitivematch);
                            ArrayList<Object> objValues = (ArrayList) obj.get(field.getName());

                            value.isSet = 0;
                            if (objValues != null && objValues.get(0) != null && !(objValues.get(0).toString().trim().isEmpty())) {
                                value.value = Double.parseDouble(objValues.get(0).toString());
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
            contextAsMap.remove(T.id);
            contextAsMap.remove(T.label);
            contextAsMap.put(SpecialKeys.ID.toString(), fieldValueID);
        }

        if (!caseInsensitive) {
            return contextAsMap;
        }

        TreeMap<String, Object> caseInsensitiveMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitiveMap.putAll(contextAsMap);
        return caseInsensitiveMap;
    }
}
