/*-
 * #%L
 * athena-hbase
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
package com.amazonaws.athena.connectors.hbase;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connectors.hbase.connection.HBaseConnection;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Collection of helpful utilities that handle HBase schema inference, type, and naming conversion.
 */
public class HbaseSchemaUtils
{
    //Field name for the special 'row' column which represets the HBase key used to store a given row.
    protected static final String ROW_COLUMN_NAME = "row";
    //The HBase namespce qualifier character which commonly separates namespaces and column families from tables and columns.
    protected static final String NAMESPACE_QUALIFIER = ":";
    private static final Logger logger = LoggerFactory.getLogger(HbaseSchemaUtils.class);

    private HbaseSchemaUtils() {}

    /**
     * This method will produce an Apache Arrow Schema for the given TableName and HBase connection
     * by scanning up to the requested number of rows and using basic schema inference to determine
     * data types.
     *
     * @param client The HBase connection to use for the scan operation.
     * @param tableName The HBase TableName for which to produce an Apache Arrow Schema.
     * @param numToScan The number of records to scan as part of producing the Schema.
     * @return An Apache Arrow Schema representing the schema of the HBase table.
     */
    public static Schema inferSchema(HBaseConnection client, TableName tableName, int numToScan)
    {
        Scan scan = new Scan().setMaxResultSize(numToScan).setFilter(new PageFilter(numToScan));
        org.apache.hadoop.hbase.TableName hbaseTableName = org.apache.hadoop.hbase.TableName.valueOf(getQualifiedTableName(tableName));
        return client.scanTable(hbaseTableName, scan, (ResultScanner scanner) -> {
            try {
                return scanAndInferSchema(scanner);
            }
            catch (java.io.UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * This helper method is used in conjunction with the scan facility provided
     *
     * @param scanner The HBase ResultScanner to read results from while inferring schema.
     * @return An Apache Arrow Schema representing the schema of the HBase table.
     * @note The resulting schema is a union of the schema of every row that is scanned. Any time two rows
     * have a field with the same name but different inferred type the code will default the type of
     * that field in the resulting schema to a VARCHAR. This approach is not perfect and can struggle
     * to produce a usable schema if the table has a significant mix of entities.
     */
    private static Schema scanAndInferSchema(ResultScanner scanner) throws java.io.UnsupportedEncodingException
    {
        Map<String, Map<String, ArrowType>> schemaInference = new HashMap<>();
        int rowCount = 0;
        int fieldCount = 0;

        for (Result result : scanner) {
            rowCount++;
            for (Cell cell : result.listCells()) {
                fieldCount++;
                String family = Bytes.toStringBinary(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toStringBinary(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

                Map<String, ArrowType> schemaForFamily = schemaInference.get(family);
                if (schemaForFamily == null) {
                    schemaForFamily = new HashMap<>();
                    schemaInference.put(family, schemaForFamily);
                }

                //Get the previously inferred type for this column if we've seen it on a past row
                ArrowType prevInferredType = schemaForFamily.get(column);

                //Infer the type of the column from the value on the current row.
                Types.MinorType inferredType = inferType(Bytes.toStringBinary(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));

                //Check if the previous and currently inferred types match
                if (prevInferredType != null && Types.getMinorTypeForArrowType(prevInferredType) != inferredType) {
                    logger.info("inferSchema: Type changed detected for field, using VARCHAR - family: {} col: {} previousType: {} newType: {}",
                            family, column, prevInferredType, inferredType);
                    schemaForFamily.put(column, Types.MinorType.VARCHAR.getType());
                }
                else {
                    schemaForFamily.put(column, inferredType.getType());
                }

                logger.info("inferSchema: family: {} col: {} inferredType: {}", family, column, inferredType);
            }
        }

        logger.info("inferSchema: Evaluated {} field values across {} rows.", fieldCount, rowCount);

        //Used the union of all row's to produce our resultant Apache Arrow Schema.
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        for (Map.Entry<String, Map<String, ArrowType>> nextFamily : schemaInference.entrySet()) {
            String family = nextFamily.getKey();
            for (Map.Entry<String, ArrowType> nextCol : nextFamily.getValue().entrySet()) {
                schemaBuilder.addField(family + NAMESPACE_QUALIFIER + nextCol.getKey(), nextCol.getValue());
            }
        }

        Schema schema = schemaBuilder.build();
        if (schema.getFields().isEmpty()) {
            throw new RuntimeException("No columns found after scanning " + fieldCount + " values across " +
                    rowCount + " rows. Please ensure the table is not empty and contains at least 1 supported column type.");
        }
        return schema;
    }

    /**
     * Helper which goes from an Athena Federation SDK TableName to an HBase table name string.
     *
     * @param tableName An Athena Federation SDK TableName.
     * @return The corresponding HBase table name string.
     */
    public static String getQualifiedTableName(TableName tableName)
    {
        return tableName.getSchemaName() + NAMESPACE_QUALIFIER + tableName.getTableName();
    }

    /**
     * Helper which goes from an Athena Federation SDK TableName to an HBase TableName.
     *
     * @param tableName An Athena Federation SDK TableName.
     * @return The corresponding HBase TableName.
     */
    public static org.apache.hadoop.hbase.TableName getQualifiedTable(TableName tableName)
    {
        return org.apache.hadoop.hbase.TableName.valueOf(tableName.getSchemaName() + NAMESPACE_QUALIFIER + tableName.getTableName());
    }

    /**
     * Given a value from HBase attempt to infer it's type.
     *
     * @param value An HBase value.
     * @return The Apache Arrow Minor Type most closely associated with the provided value.
     * @note This method of inference is very naive and only works if the values are stored in HBase
     * as Strings. It uses VARCHAR as its fallback if it can't not parse our the value to
     * one of the other supported inferred types. It is expected that customers of this connector
     * may want to customize this logic or rely on explicit Schema in Glue.
     */
    public static Types.MinorType inferType(String strVal)
    {
        try {
            Long.valueOf(strVal);
            return Types.MinorType.BIGINT;
        }
        catch (RuntimeException ex) {
        }

        try {
            Double.valueOf(strVal);
            return Types.MinorType.FLOAT8;
        }
        catch (RuntimeException ex) {
        }

        return Types.MinorType.VARCHAR;
    }

    /**
     * Helper that can coerce the given HBase value to the requested Apache Arrow type.
     *
     * @param isNative If True, the HBase value is stored using native bytes. If False, the value is serialized as a String.
     * @param type The Apache Arrow Type that the value should be coerced to before returning.
     * @param value The HBase value to coerce.
     * @return The coerced value which is now allowed with the provided Apache Arrow type.
     */
    public static Object coerceType(boolean isNative, ArrowType type, byte[] value)
    {
        if (value == null) {
            return null;
        }

        Types.MinorType minorType = Types.getMinorTypeForArrowType(type);
        switch (minorType) {
            case VARCHAR:
                return Bytes.toString(value);
            case INT:
                return isNative ? ByteBuffer.wrap(value).getInt() : Integer.parseInt(Bytes.toString(value));
            case BIGINT:
                return isNative ? ByteBuffer.wrap(value).getLong() : Long.parseLong(Bytes.toString(value));
            case FLOAT4:
                return isNative ? ByteBuffer.wrap(value).getFloat() : Float.parseFloat(Bytes.toString(value));
            case FLOAT8:
                return isNative ? ByteBuffer.wrap(value).getDouble() : Double.parseDouble(Bytes.toString(value));
            case BIT:
                if (isNative) {
                    return (value[0] != 0);
                }
                else {
                    return Boolean.parseBoolean(Bytes.toString(value));
                }
            case VARBINARY:
                return value;
            default:
                throw new IllegalArgumentException(type + " with minorType[" + minorType + "] is not supported.");
        }
    }

    /**
     * Helper which can go from a Glue/Apache Arrow column name to its HBase family + column.
     *
     * @param glueColumnName The input column name in format "family:column".
     * @return
     */
    public static String[] extractColumnParts(String glueColumnName)
    {
        return glueColumnName.split(NAMESPACE_QUALIFIER);
    }

    /**
     * Used to convert from Apache Arrow typed values to HBase values.
     *
     * @param isNative If True, the HBase value should be stored using native bytes.
     * If False, the value should be serialized as a String before storing it.
     * @param value The value to convert.
     * @return The HBase byte representation of the value.
     * @note This is commonly used when attempting to push constraints into HBase which requires converting a small
     * number of values from Apache Arrow's Type system to HBase compatible representations for comparisons.
     */
    public static byte[] toBytes(boolean isNative, Object value)
    {
        if (value == null || value instanceof byte[]) {
            return (byte[]) value;
        }

        if (value instanceof String) {
            return ((String) value).getBytes();
        }

        if (value instanceof Text) {
            return ((Text) value).toString().getBytes();
        }

        if (!isNative) {
            return String.valueOf(value).getBytes();
        }

        if (value instanceof Integer) {
            return ByteBuffer.allocate(4).putInt((int) value).array();
        }

        if (value instanceof Long) {
            return ByteBuffer.allocate(8).putLong((long) value).array();
        }

        if (value instanceof Float) {
            return ByteBuffer.allocate(4).putFloat((float) value).array();
        }

        if (value instanceof Double) {
            return ByteBuffer.allocate(8).putDouble((double) value).array();
        }

        if (value instanceof Boolean) {
            return ByteBuffer.allocate(1).put((byte) ((boolean) value ? 1 : 0)).array();
        }

        String className = (value == null || value.getClass() == null) ? "null" : value.getClass().getName();
        throw new RuntimeException("Unsupported object type for " + value + " with class name " + className);
    }
}
