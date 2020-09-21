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
import com.amazonaws.athena.connector.lambda.data.writers.extractors.*;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;

public class TypeRowWriter {

    public static RowWriterBuilder writeRowTemplate(RowWriterBuilder rowWriterBuilder, Field field) {

        Logger logger = LoggerFactory.getLogger(TypeRowWriter.class);

        ArrowType arrowType = field.getType();
        Types.MinorType minorType = Types.getMinorTypeForArrowType(arrowType);

        switch (minorType) {

            case FLOAT8:
                rowWriterBuilder.withExtractor(field.getName(),
                        (Float8Extractor) (Object context, NullableFloat8Holder value) -> {
                            value.isSet = 1;

                            Map<Object, Object> obj = (Map<Object, Object>) context;
                            ArrayList<Object> objValues = (ArrayList)obj.get(field.getName());

                            value.value = Float.parseFloat(objValues.get(0).toString());
                        });

                break;

            case VARCHAR:

                rowWriterBuilder.withExtractor(field.getName(),
                        (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
                            value.isSet = 1;
                            Map<Object, Object> obj = (Map<Object, Object>) context;
                            ArrayList<Object> objValues = (ArrayList)obj.get(field.getName());

                            value.value = objValues.get(0).toString();
                        });
                break;

            case FLOAT4:

                rowWriterBuilder.withExtractor(field.getName(),
                        (Float4Extractor) (Object context, NullableFloat4Holder value) -> {
                            value.isSet = 1;

                            Map<Object, Object> obj = (Map<Object, Object>) context;
                            ArrayList<Object> objValues = (ArrayList)obj.get(field.getName());

                            value.value = Float.parseFloat(objValues.get(0).toString());
                        });
                break;

            case INT:
                rowWriterBuilder.withExtractor(field.getName(),
                        (IntExtractor) (Object context, NullableIntHolder value) -> {
                            value.isSet = 1;

                            Map<Object, Object> obj = (Map<Object, Object>) context;
                            ArrayList<Object> objValues = (ArrayList)obj.get(field.getName());
                            value.value = Integer.parseInt(objValues.get(0).toString());
                        });
                break;

            default:
                break;
        }

        return rowWriterBuilder;
    }
}
