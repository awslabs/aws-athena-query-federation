package com.amazonaws.athena.connectors.hbase;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
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

    public static Object coerceType(ArrowType type, byte[] value)
    {
        if (value == null) {
            return null;
        }

        Types.MinorType minorType = Types.getMinorTypeForArrowType(type);
        String strVal = Bytes.toString(value);
        switch (minorType) {
            case VARCHAR:
                return strVal;
            case INT:
                return Integer.parseInt(strVal);
            case BIGINT:
                return Long.parseLong(strVal);
            case FLOAT4:
                return Float.parseFloat(strVal);
            case FLOAT8:
                return Double.parseDouble(strVal);
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
}
