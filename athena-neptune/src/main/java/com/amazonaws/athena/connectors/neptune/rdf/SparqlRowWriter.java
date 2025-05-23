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
package com.amazonaws.athena.connectors.neptune.rdf;

import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter.RowWriterBuilder;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.datatype.XMLGregorianCalendar;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

/**
 * This class is a Utility class to create Extractors for each field type as per
 * Schema
 */
public final class SparqlRowWriter 
{
    private static final Logger logger = LoggerFactory.getLogger(SparqlRowWriter.class);

    private SparqlRowWriter() 
    {
        // Empty private constructor
    }
    
    private static Object extractValue(Object context, Field field) 
    {
        Map<String, Object> row =  (Map<String, Object>) context;
        Object val = row.get(field.getName());
        return val;
    }
    
    public static Object extractValue(Object context, Field field, Class expectedClass) 
    {
        Map<String, Object> row =  (Map<String, Object>) context;
        Object val = row.get(field.getName());
        logger.debug("extractValue" + field + " " + expectedClass);
        
        if (val == null) {
            return null;
        }
        logger.debug("extract" + field + " " + expectedClass + " actual " + val.getClass());
        if (expectedClass.equals(Integer.class)) {
            if (val instanceof Integer) {
                return val;
            }
            else if (val instanceof Long) {
                return (Integer) val;
            }
            else if (val instanceof BigInteger) {
                return ((BigInteger) val).intValue();
            }
        }
        else if (expectedClass.equals(Long.class)) {
            if (val instanceof Integer) {
                return (Long) val;
            }
            else if (val instanceof Long) {
                return val;
            }
            else if (val instanceof BigInteger) {
                 return ((BigInteger) val).longValue();
            }
        }
        else if (expectedClass.equals(Float.class)) {
            if (val instanceof Float) {
                return val;
            }
            else if (val instanceof Double) {
                return (Float) val;
            }
            else if (val instanceof BigDecimal) {
                return ((BigDecimal) val).floatValue();
            }
        }
        else if (expectedClass.equals(Double.class)) {
            if (val instanceof Float) {
                return (Double) val;
            }
            else if (val instanceof Double) {
                return val;
            }
            else if (val instanceof BigDecimal) {
                return ((BigDecimal) val).doubleValue();
            }
        }
        else if (expectedClass.equals(Boolean.class)) {
            if (val instanceof Boolean) {
                return val;
            }
        }
        else if (expectedClass.equals(java.util.Date.class)) {
            if (val instanceof XMLGregorianCalendar) {
                return ((XMLGregorianCalendar) val).toGregorianCalendar().getTime();
            }
        }

        logger.debug("extractValue" + field + " " + expectedClass + " returned " + val.getClass());
        return val;        
    }    

    public static void writeRowTemplate(RowWriterBuilder rowWriterBuilder, Field field) 
    {
        ArrowType arrowType = field.getType();
        Types.MinorType minorType = Types.getMinorTypeForArrowType(arrowType);
        
        switch (minorType) {
            case BIT:
                rowWriterBuilder.withExtractor(field.getName(),
                        (BitExtractor) (Object context, NullableBitHolder value) -> {
                            Boolean val = (Boolean) extractValue(context, field, Boolean.class);
                            value.isSet = val == null ? 0 : 1;
                            if (val != null) {
                                value.value = val ? 1 : 0;
                            }
                        });
                break;

            case VARCHAR:
                rowWriterBuilder.withExtractor(field.getName(),
                        (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
                            String val = (String) extractValue(context, field, String.class);
                            value.isSet = val == null ? 0 : 1;
                            if (val != null) {
                                value.value = val;
                            }
                        });
                break;

            case DATEMILLI:

                rowWriterBuilder.withExtractor(field.getName(),
                        (DateMilliExtractor) (Object context, NullableDateMilliHolder value) -> {
                            java.util.Date val = (java.util.Date) extractValue(context, field, java.util.Date.class);
                            value.isSet = val == null ? 0 : 1;
                            if (val != null) {
                                value.value = val.getTime();
                            }
                        });
                break;

            case INT:
                rowWriterBuilder.withExtractor(field.getName(),
                        (IntExtractor) (Object context, NullableIntHolder value) -> {
                            Integer val = (Integer) extractValue(context, field, Integer.class);
                            value.isSet = val == null ? 0 : 1;
                            if (val != null) {
                                value.value = val;
                            }
                        });
                break;

            case BIGINT:
                rowWriterBuilder.withExtractor(field.getName(),
                        (BigIntExtractor) (Object context, NullableBigIntHolder value) -> {
                            Integer val = (Integer) extractValue(context, field, Integer.class);
                            value.isSet = val == null ? 0 : 1;
                            if (val != null) {
                                value.value = val;
                            }
                        });
                break;

            case FLOAT4:
                rowWriterBuilder.withExtractor(field.getName(),
                        (Float4Extractor) (Object context, NullableFloat4Holder value) -> {
                            Float val = (Float) extractValue(context, field, Float.class);
                            value.isSet = val == null ? 0 : 1;
                            if (val != null) {
                                value.value = val;
                            }
                        });
                break;

            case FLOAT8:
                rowWriterBuilder.withExtractor(field.getName(),
                        (Float8Extractor) (Object context, NullableFloat8Holder value) -> {
                            Double val = (Double) extractValue(context, field, Double.class);
                            value.isSet = val == null ? 0 : 1;
                            if (val != null) {
                                value.value = val;
                            }
                        });

                break;
        }
    }
}
