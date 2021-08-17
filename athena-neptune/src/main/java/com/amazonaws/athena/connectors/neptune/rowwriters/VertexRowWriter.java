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
package com.amazonaws.athena.connectors.neptune.rowwriters;

import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter.RowWriterBuilder;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;

import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.Map;

/**
 * This class is a Utility class to create Extractors for each field type as per
 * Schema
 */
public final class VertexRowWriter extends BaseRowWriter {
    private VertexRowWriter() {
        // Empty private constructor
    }

    public static void writeRowTemplate(RowWriterBuilder rowWriterBuilder, Field field) {
        ArrowType arrowType = field.getType();
        Types.MinorType minorType = Types.getMinorTypeForArrowType(arrowType);

        switch (minorType) {
            case BIT:
                rowWriterBuilder.withExtractor(field.getName(),
                        (BitExtractor) (Object context, NullableBitHolder value) -> {
                            Map<Object, Object> obj = (Map<Object, Object>) context;
                            ArrayList<Object> objValues = (ArrayList) obj.get(field.getName());

                            value.isSet = 0;
                            if (objValues != null && objValues.get(0) != null) {
                                Boolean booleanValue = Boolean.parseBoolean(objValues.get(0).toString());
                                value.value = booleanValue ? 1 : 0;
                                value.isSet = 1;
                            }
                        });
                break;

            case VARCHAR:
                rowWriterBuilder.withExtractor(field.getName(),
                        (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
                            Map<Object, Object> obj = (Map<Object, Object>) context;
                            String fieldName = field.getName().toLowerCase().trim();

                            value.isSet = 0;
                            // check for special keys and parse them separately
                            if (fieldName.equals(SpecialKeys.ID.toString().toLowerCase())) {
                                Object fieldValue = obj.get(T.id);
                                if (fieldValue != null) {
                                    value.value = fieldValue.toString();
                                    value.isSet = 1;
                                }

                            } else {
                                ArrayList<Object> objValues = (ArrayList) obj.get(field.getName());
                                if (objValues != null && objValues.get(0) != null) {
                                    value.value = objValues.get(0).toString();
                                    value.isSet = 1;
                                }
                            }
                        });
                break;

            case INT:
                rowWriterBuilder.withExtractor(field.getName(),
                        (IntExtractor) (Object context, NullableIntHolder value) -> {
                            Map<Object, Object> obj = (Map<Object, Object>) context;

                            value.isSet = 0;
                            ArrayList<Object> objValues = (ArrayList) obj.get(field.getName());

                            if (objValues != null && objValues.get(0) != null) {
                                value.value = Integer.parseInt(objValues.get(0).toString());
                                value.isSet = 1;
                            }
                        });
                break;

            case BIGINT:
                rowWriterBuilder.withExtractor(field.getName(),
                        (BigIntExtractor) (Object context, NullableBigIntHolder value) -> {
                            Map<Object, Object> obj = (Map<Object, Object>) context;
                            ArrayList<Object> objValues = (ArrayList) obj.get(field.getName());

                            value.isSet = 0;
                            if (objValues != null && objValues.get(0) != null) {
                                value.value = Long.parseLong(objValues.get(0).toString());
                                value.isSet = 1;
                            }
                        });
                break;

            case FLOAT4:
                rowWriterBuilder.withExtractor(field.getName(),
                        (Float4Extractor) (Object context, NullableFloat4Holder value) -> {
                            Map<Object, Object> obj = (Map<Object, Object>) context;
                            ArrayList<Object> objValues = (ArrayList) obj.get(field.getName());

                            value.isSet = 0;
                            if (objValues != null && objValues.get(0) != null) {
                                value.value = Float.parseFloat(objValues.get(0).toString());
                                value.isSet = 1;
                            }
                        });
                break;

            case FLOAT8:
                rowWriterBuilder.withExtractor(field.getName(),
                        (Float8Extractor) (Object context, NullableFloat8Holder value) -> {
                            Map<Object, Object> obj = (Map<Object, Object>) context;
                            ArrayList<Object> objValues = (ArrayList) obj.get(field.getName());

                            value.isSet = 0;
                            if (objValues != null && objValues.get(0) != null) {
                                value.value = Double.parseDouble(objValues.get(0).toString());
                                value.isSet = 1;
                            }
                        });

                break;
        }
    }
}
