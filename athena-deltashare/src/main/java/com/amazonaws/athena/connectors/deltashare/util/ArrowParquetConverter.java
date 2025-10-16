/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare.util;

import com.amazonaws.athena.connector.lambda.data.Block;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Converts Parquet Group records to Arrow Block format
 * Separated from ParquetReaderUtil for better maintainability
 */
public class ArrowParquetConverter 
{
    private static final Logger logger = LoggerFactory.getLogger(ArrowParquetConverter.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    /**
     * Convert Parquet Group record to Arrow Block format
     */
    public static void writeParquetRecordToBlock(Block block, int rowNum, Group record, Schema schema)
    {
        writeParquetRecordToBlock(block, rowNum, record, schema, null);
    }
    
    /**
     * Convert Parquet Group record to Arrow Block format with partition value injection
     */
    public static void writeParquetRecordToBlock(Block block, int rowNum, Group record, Schema schema, 
                                               Map<String, String> partitionValues)
    {
        GroupType parquetSchema = record.getType();
        
        for (Field field : schema.getFields()) {
            String fieldName = field.getName();
            
            try {
                if (!parquetSchema.containsField(fieldName)) {
                    if (partitionValues != null && partitionValues.containsKey(fieldName)) {
                        String partitionValue = partitionValues.get(fieldName);
                        if (partitionValue != null) {
                            block.setValue(fieldName, rowNum, partitionValue);
                            continue;
                        }
                    }
                    setNullValue(block, field, rowNum);
                    continue;
                }
                
                int fieldIndex = parquetSchema.getFieldIndex(fieldName);
                if (record.getFieldRepetitionCount(fieldIndex) == 0) {
                    setNullValue(block, field, rowNum);
                    continue;
                }
                
                ArrowType arrowType = field.getType();
                
                if (arrowType instanceof ArrowType.Utf8) {
                    block.setValue(fieldName, rowNum, record.getString(fieldIndex, 0));
                } else if (arrowType instanceof ArrowType.Int) {
                    ArrowType.Int intType = (ArrowType.Int) arrowType;
                    if (intType.getBitWidth() == 32) {
                        block.setValue(fieldName, rowNum, record.getInteger(fieldIndex, 0));
                    } else {
                        block.setValue(fieldName, rowNum, record.getLong(fieldIndex, 0));
                    }
                } else if (arrowType instanceof ArrowType.FloatingPoint) {
                    ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) arrowType;
                    if (floatType.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE) {
                        block.setValue(fieldName, rowNum, record.getFloat(fieldIndex, 0));
                    } else {
                        block.setValue(fieldName, rowNum, record.getDouble(fieldIndex, 0));
                    }
                } else if (arrowType instanceof ArrowType.Bool) {
                    block.setValue(fieldName, rowNum, record.getBoolean(fieldIndex, 0));
                } else if (arrowType instanceof ArrowType.Date) {
                    ArrowType.Date dateType = (ArrowType.Date) arrowType;
                    
                    try {
                        handleDateTimestamp(record, fieldIndex, block, fieldName, rowNum, dateType, true);
                    } catch (Exception e) {
                        logger.warn("Error processing date field '{}': {}", fieldName, e.getMessage());
                        setNullValue(block, field, rowNum);
                    }
                } else if (arrowType instanceof ArrowType.Timestamp) {
                    try {
                        handleDateTimestamp(record, fieldIndex, block, fieldName, rowNum, null, false);
                    } catch (Exception e) {
                        logger.warn("Error processing timestamp field '{}': {}", fieldName, e.getMessage());
                        try {
                            block.setValue(fieldName, rowNum, record.getLong(fieldIndex, 0));
                        } catch (Exception e2) {
                            setNullValue(block, field, rowNum);
                        }
                    }
                } else if (arrowType instanceof ArrowType.Binary) {
                    Binary binary = record.getBinary(fieldIndex, 0);
                    block.setValue(fieldName, rowNum, binary.getBytes());
                } else {
                    block.setValue(fieldName, rowNum, record.getValueToString(fieldIndex, 0));
                }
                
            } catch (Exception e) {
                logger.warn("Error converting field '{}': {}", fieldName, e.getMessage());
                setNullValue(block, field, rowNum);
            }
        }
    }

    /**
     * Handle date/timestamp fields with Int96 awareness
     */
    private static void handleDateTimestamp(Group record, int fieldIndex, Block block, String fieldName, 
                                          int rowNum, ArrowType.Date dateType, boolean isDateField) throws Exception
    {
        try {
            String valueString = record.getValueToString(fieldIndex, 0);
            
            if (valueString.contains("Int96") && valueString.contains("Binary{")) {
                
                int startIdx = valueString.indexOf('[');
                int endIdx = valueString.indexOf(']');
                
                if (startIdx != -1 && endIdx != -1) {
                    String bytesStr = valueString.substring(startIdx + 1, endIdx);
                    String[] byteStrings = bytesStr.split(",\\s*");
                    
                    if (byteStrings.length == 12) {
                        byte[] int96Bytes = new byte[12];
                        for (int i = 0; i < 12; i++) {
                            int96Bytes[i] = (byte) Integer.parseInt(byteStrings[i].trim());
                        }
                        
                        long epochMillis = DateTimeConverter.convertInt96BytesToEpochMillis(int96Bytes);
                        
                        if (isDateField) {
                            if (dateType.getUnit() == org.apache.arrow.vector.types.DateUnit.MILLISECOND) {
                                ((DateMilliVector) block.getFieldVector(fieldName)).setSafe(rowNum, epochMillis);
                            } else {
                                int epochDays = (int) (epochMillis / (24 * 60 * 60 * 1000L));
                                ((DateDayVector) block.getFieldVector(fieldName)).setSafe(rowNum, epochDays);
                            }
                        } else {
                            block.setValue(fieldName, rowNum, epochMillis);
                        }
                        return;
                    }
                }
                
                logger.warn("Failed to parse Int96 binary data for field '{}' - setting null", fieldName);
                if (isDateField) {
                    setNullValueForDateField(block, fieldName, rowNum, dateType);
                } else {
                    block.setValue(fieldName, rowNum, null);
                }
                return;
            }
            
            if (isDateField) {
                handleDateAsIntegerOrString(record, fieldIndex, block, fieldName, rowNum, dateType);
            } else {
                block.setValue(fieldName, rowNum, record.getLong(fieldIndex, 0));
            }
            
        } catch (ClassCastException e) {
            logger.warn("ClassCastException for field '{}': {} - setting null", fieldName, e.getMessage());
            if (isDateField) {
                setNullValueForDateField(block, fieldName, rowNum, dateType);
            } else {
                block.setValue(fieldName, rowNum, null);
            }
        } catch (Exception e) {
            logger.warn("Error processing field '{}': {} - setting null", fieldName, e.getMessage());
            if (isDateField) {
                setNullValueForDateField(block, fieldName, rowNum, dateType);
            } else {
                block.setValue(fieldName, rowNum, null);
            }
        }
    }

    /**
     * Set null value specifically for date fields
     */
    private static void setNullValueForDateField(Block block, String fieldName, int rowNum, ArrowType.Date dateType)
    {
        try {
            if (dateType.getUnit() == org.apache.arrow.vector.types.DateUnit.MILLISECOND) {
                ((DateMilliVector) block.getFieldVector(fieldName)).setNull(rowNum);
            } else {
                ((DateDayVector) block.getFieldVector(fieldName)).setNull(rowNum);
            }
        } catch (Exception e) {
            logger.warn("Failed to set null value for date field '{}': {}", fieldName, e.getMessage());
        }
    }

    /**
     * Helper method to handle dates as integer (days since epoch) or string
     */
    private static void handleDateAsIntegerOrString(Group record, int fieldIndex, Block block, String fieldName, 
                                                  int rowNum, ArrowType.Date dateType) throws Exception
    {
        try {
            int epochDays = record.getInteger(fieldIndex, 0);
            
            if (dateType.getUnit() == org.apache.arrow.vector.types.DateUnit.MILLISECOND) {
                long epochMillis = epochDays * 24L * 60 + 60 * 1000;
                ((DateMilliVector) block.getFieldVector(fieldName)).setSafe(rowNum, epochMillis);
            } else {
                ((DateDayVector) block.getFieldVector(fieldName)).setSafe(rowNum, epochDays);
            }
        } catch (Exception e) {
            try {
                String dateStr = record.getString(fieldIndex, 0);
                LocalDate date = LocalDate.parse(dateStr, DATE_FORMATTER);
                
                if (dateType.getUnit() == org.apache.arrow.vector.types.DateUnit.MILLISECOND) {
                    long epochMillis = date.atStartOfDay().toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
                    ((DateMilliVector) block.getFieldVector(fieldName)).setSafe(rowNum, epochMillis);
                } else {
                    int epochDays = (int) date.toEpochDay();
                    ((DateDayVector) block.getFieldVector(fieldName)).setSafe(rowNum, epochDays);
                }
            } catch (Exception e2) {
                logger.warn("Failed to parse date field '{}' as integer or string: {}", fieldName, e2.getMessage());
                throw e2;
            }
        }
    }

    /**
     * Set null value for a field based on its type
     */
    private static void setNullValue(Block block, Field field, int rowNum)
    {
        try {
            ArrowType arrowType = field.getType();
            String fieldName = field.getName();
            
            if (arrowType instanceof ArrowType.Utf8) {
                ((VarCharVector) block.getFieldVector(fieldName)).setNull(rowNum);
            } else if (arrowType instanceof ArrowType.Int) {
                ArrowType.Int intType = (ArrowType.Int) arrowType;
                if (intType.getBitWidth() == 32) {
                    ((IntVector) block.getFieldVector(fieldName)).setNull(rowNum);
                } else {
                    ((BigIntVector) block.getFieldVector(fieldName)).setNull(rowNum);
                }
            } else if (arrowType instanceof ArrowType.FloatingPoint) {
                ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) arrowType;
                if (floatType.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE) {
                    ((Float4Vector) block.getFieldVector(fieldName)).setNull(rowNum);
                } else {
                    ((Float8Vector) block.getFieldVector(fieldName)).setNull(rowNum);
                }
            } else if (arrowType instanceof ArrowType.Date) {
                ArrowType.Date dateType = (ArrowType.Date) arrowType;
                if (dateType.getUnit() == org.apache.arrow.vector.types.DateUnit.MILLISECOND) {
                    ((DateMilliVector) block.getFieldVector(fieldName)).setNull(rowNum);
                } else {
                    ((DateDayVector) block.getFieldVector(fieldName)).setNull(rowNum);
                }
            } else {
                block.setValue(fieldName, rowNum, null);
            }
        } catch (Exception e) {
            logger.warn("Failed to set null value for field '{}': {}", field.getName(), e.getMessage());
        }
    }
}
