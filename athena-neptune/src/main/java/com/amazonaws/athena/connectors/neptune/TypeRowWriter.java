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
package com.amazonaws.athena.connectors.neptune;

import java.util.ArrayList;
import java.util.Map;

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
/**
 * This class is a Utility class to create Extractors for each field type as per
 * Schema
 */
public final class TypeRowWriter
{
    private TypeRowWriter()
    {
        //Empty private constructor
    }

    public static void writeRowTemplate(RowWriterBuilder rowWriterBuilder, Field field)
    {
        ArrowType arrowType = field.getType();
        Types.MinorType minorType = Types.getMinorTypeForArrowType(arrowType);

        switch (minorType) {
            case BIT:
                rowWriterBuilder.withExtractor(field.getName(),
                        (BitExtractor) (Object context, NullableBitHolder value) -> {
                            value.isSet = 1;
                            Map<Object, Object> obj = (Map<Object, Object>) context;
                            ArrayList<Object> objValues = (ArrayList) obj.get(field.getName());

                            Boolean booleanValue = Boolean.parseBoolean(objValues.get(0).toString());
                            value.value = booleanValue ? 1 : 0;
                        });
                break;

            case VARCHAR:
                rowWriterBuilder.withExtractor(field.getName(),
                        (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
                            value.isSet = 1;
                            Map<Object, Object> obj = (Map<Object, Object>) context;
                            ArrayList<Object> objValues = (ArrayList) obj.get(field.getName());

                            value.value = objValues.get(0).toString();
                        });
                break;

            case INT:
                rowWriterBuilder.withExtractor(field.getName(),
                        (IntExtractor) (Object context, NullableIntHolder value) -> {
                            value.isSet = 1;
                            Map<Object, Object> obj = (Map<Object, Object>) context;
                            ArrayList<Object> objValues = (ArrayList) obj.get(field.getName());
                            value.value = Integer.parseInt(objValues.get(0).toString());
                        });
                break;

            case BIGINT:
                rowWriterBuilder.withExtractor(field.getName(),
                    (BigIntExtractor) (Object context, NullableBigIntHolder value) -> {
                        value.isSet = 1;
                        Map<Object, Object> obj = (Map<Object, Object>) context;
                        ArrayList<Object> objValues = (ArrayList) obj.get(field.getName());

                        value.value = Long.parseLong(objValues.get(0).toString());
                    });
            break;

            case FLOAT4:
                rowWriterBuilder.withExtractor(field.getName(),
                        (Float4Extractor) (Object context, NullableFloat4Holder value) -> {
                            value.isSet = 1;
                            Map<Object, Object> obj = (Map<Object, Object>) context;
                            ArrayList<Object> objValues = (ArrayList) obj.get(field.getName());
                            value.value = (Float) (objValues.get(0));
                        });
                break;

            case FLOAT8:
                rowWriterBuilder.withExtractor(field.getName(),
                        (Float8Extractor) (Object context, NullableFloat8Holder value) -> {
                            value.isSet = 1;
                            Map<Object, Object> obj = (Map<Object, Object>) context;
                            ArrayList<Object> objValues = (ArrayList) obj.get(field.getName());
                            value.value = (Double) (objValues.get(0));
                        });

                break;
        }
    }
}
