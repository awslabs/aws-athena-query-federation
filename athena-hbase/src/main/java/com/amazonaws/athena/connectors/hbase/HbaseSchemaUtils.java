package com.amazonaws.athena.connectors.hbase;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.lang.BooleanUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HbaseSchemaUtils
{
    private static final Logger logger = LoggerFactory.getLogger(HbaseSchemaUtils.class);
    protected static final String ROW_COLUMN_NAME = "row";
    protected static final String NAMESPACE_QUALIFIER = ":";

    private HbaseSchemaUtils() {}

    public static Schema inferSchema(Connection client, TableName tableName, int numToScan)
    {
        Map<String, Map<String, ArrowType>> schemaInference = new HashMap<>();
        Scan scan = new Scan().setMaxResultSize(numToScan).setFilter(new PageFilter(numToScan));
        try (Table table = client.getTable(org.apache.hadoop.hbase.TableName.valueOf(getQualifiedTableName(tableName)));
                ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                for (KeyValue keyValue : result.list()) {
                    String family = new String(keyValue.getFamily());
                    String column = new String(keyValue.getQualifier());

                    Map<String, ArrowType> schemaForFamily = schemaInference.get(family);
                    if (schemaForFamily == null) {
                        schemaForFamily = new HashMap<>();
                        schemaInference.put(family, schemaForFamily);
                    }

                    ArrowType prevInferredType = schemaForFamily.get(column);
                    Types.MinorType inferredType = inferType(keyValue.getValue());
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

            SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
            for (Map.Entry<String, Map<String, ArrowType>> nextFamily : schemaInference.entrySet()) {
                String family = nextFamily.getKey();
                for (Map.Entry<String, ArrowType> nextCol : nextFamily.getValue().entrySet()) {
                    schemaBuilder.addField(family + NAMESPACE_QUALIFIER + nextCol.getKey(), nextCol.getValue());
                }
            }

            return schemaBuilder.build();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static String getQualifiedTableName(TableName tableName)
    {
        return tableName.getSchemaName() + NAMESPACE_QUALIFIER + tableName.getTableName();
    }

    public static org.apache.hadoop.hbase.TableName getQualifiedTable(TableName tableName)
    {
        return org.apache.hadoop.hbase.TableName.valueOf(tableName.getSchemaName() + NAMESPACE_QUALIFIER + tableName.getTableName());
    }

    public static Types.MinorType inferType(byte[] value)
    {
        String strVal = Bytes.toString(value);
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

    public static String[] extractColumnParts(String glueColumnName)
    {
        return glueColumnName.split(NAMESPACE_QUALIFIER);
    }

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

        throw new RuntimeException("Unsupported object type for " + value + " " + value.getClass().getName());
    }
}
