/*-
 * #%L
 * athena-elasticsearch
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.connectors.athena.elasticsearch;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.metadata.glue.DefaultGlueType;
import com.amazonaws.athena.connector.lambda.metadata.glue.GlueFieldLexer;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is used for mapping Glue data types to Apache Arrow. In addition to the already supported types, it also
 * maps the new SCALED_FLOAT(..) type to BIGINT. The new type is modeled after the scaled_float Elasticsearch data type
 * which is a representation of a floating point number displayed as a long number by multiplying it with the scaling
 * factor and rounding the results (e.g. a floating point value of 0.765 with a scaling factor of 100 will be displayed
 * as 77). The format of the new type is SCALED_FLOAT(double) where double represents a double value scaling factor.
 * When constructing a new Field object for the SCALED_FLOAT data type, the scaling factor will be stored in the
 * object's metadata map (e.g. {"scaling_factor": "100"}). This is done so that the scaled float value can be
 * properly converted to a long value when extracted from the document.
 */
public class ElasticsearchGlueTypeMapper
        implements GlueFieldLexer.BaseTypeMapper
{
    private static final String SCALING_FACTOR = "scaling_factor";

    /**
     * This pattern represents the valid format for the SCALED_FLOAT Glue data type converted to lowercase for
     * normalization purposes. The expected format is scaled_float(double) where double represents a double value
     * scaling factor (e.g. scaled_float(100.0)).
     */
    private final Pattern scaledFloatPattern = Pattern.compile("scaled_float\\(\\d+(\\.\\d+)?\\)");

    /**
     * This pattern represents the scaling factor component of the SCALED_FLOAT. It accepts a double value
     * (e.g 100, 10.0, 5.57). A decimal point is not required. However, A numeric digit must come before and after
     * the decimal point if present (e.g. the following are invalid: .5, 10.).
     */
    private final Pattern scalingFactorPattern = Pattern.compile("\\d+(\\.\\d+)?");

    /**
     * Gets the Arrow type equivalent for the Glue type string representation using the DefaultGlueType.toArrowType
     * conversion routine. The implementation of DefaultGlueType.toArrowType(type) invokes toLowerCase() on the type.
     * @param type is the string representation of a Glue data type to be converted to Apache Arrow.
     * @return an Arrow data type corresponding to the Glue type using the default implementation.
     */
    @Override
    public ArrowType getType(String type)
    {
        return DefaultGlueType.toArrowType(type);
    }

    /**
     * Creates a Field object based on the name and type with special logic for extracting the SCALED_FLOAT data type.
     * When constructing a new Field object for the SCALED_FLOAT data type, the scaling factor will be stored in the
     * object's metadata map (e.g. {"scaling_factor": "100"}). This is done so that the scaled float value can be
     * properly converted to a long value when extracted from the document.
     * @param name is the name of the field.
     * @param type is the string representation of a Glue data type to be converted to Apache Arrow.
     * @return a new Field.
     */
    @Override
    public Field getField(String name, String type)
    {
        if (getType(type) == null) {
            Matcher scaledFloat = scaledFloatPattern.matcher(type.toLowerCase());
            if (scaledFloat.find() && scaledFloat.group().length() == type.length()) {
                Matcher scalingFactor = scalingFactorPattern.matcher(scaledFloat.group());
                if (scalingFactor.find()) {
                    // Create a new field with a BIGINT Arrow type, and store the scaling_factor in the metadata map.
                    return new Field(name, new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                            Collections.singletonMap(SCALING_FACTOR, scalingFactor.group())), null);
                }
            }
            return null;
        }

        return FieldBuilder.newBuilder(name, getType(type)).build();
    }
}
