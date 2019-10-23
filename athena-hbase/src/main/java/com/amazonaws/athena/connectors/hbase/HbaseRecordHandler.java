package com.amazonaws.athena.connectors.hbase;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.amazonaws.athena.connectors.hbase.HbaseMetadataHandler.END_KEY_FIELD;
import static com.amazonaws.athena.connectors.hbase.HbaseMetadataHandler.HBASE_CONN_STR;
import static com.amazonaws.athena.connectors.hbase.HbaseMetadataHandler.HBASE_NATIVE_STORAGE_FLAG;
import static com.amazonaws.athena.connectors.hbase.HbaseMetadataHandler.START_KEY_FIELD;
import static java.nio.charset.StandardCharsets.UTF_8;

public class HbaseRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(HbaseRecordHandler.class);

    private static final String SOURCE_TYPE = "hbase";

    private final AmazonS3 amazonS3;
    private final HbaseConnectionFactory connectionFactory;

    public HbaseRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                new HbaseConnectionFactory());
    }

    @VisibleForTesting
    protected HbaseRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, HbaseConnectionFactory connectionFactory)
    {
        super(amazonS3, secretsManager, SOURCE_TYPE);
        this.amazonS3 = amazonS3;
        this.connectionFactory = connectionFactory;
    }

    private Connection getOrCreateConn(String conStr)
    {
        String endpoint = resolveSecrets(conStr);
        return connectionFactory.getOrCreateConn(endpoint);
    }

    @Override
    protected void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller blockSpiller, ReadRecordsRequest request)
            throws IOException
    {
        Schema projection = request.getSchema();
        Split split = request.getSplit();
        String conStr = split.getProperty(HBASE_CONN_STR);
        boolean isNative = projection.getCustomMetadata().get(HBASE_NATIVE_STORAGE_FLAG) != null;
        Scan scan = new Scan(split.getProperty(START_KEY_FIELD).getBytes(), split.getProperty(END_KEY_FIELD).getBytes());
        scan.setFilter(pushdownPredicate(isNative, request.getConstraints()));

        //setup the projection so we only pull columns/families that we need
        for (Field next : request.getSchema().getFields()) {
            convertField(scan, next);
        }

        Connection conn = getOrCreateConn(conStr);
        Table table = conn.getTable(HbaseSchemaUtils.getQualifiedTable(request.getTableName()));

        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result row : scanner) {
                blockSpiller.writeRows((Block block, int rowNum) -> {
                    boolean match = true;
                    for (Field field : projection.getFields()) {
                        FieldVector vector = block.getFieldVector(field.getName());
                        if (match) {
                            match &= writeField(constraintEvaluator, vector, isNative, row, rowNum);
                        }
                    }
                    return match ? 1 : 0;
                });
            }
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private boolean writeField(ConstraintEvaluator constraintEvaluator, FieldVector vector, boolean isNative, Result row, int rowNum)
    {
        String fieldName = vector.getField().getName();
        ArrowType type = vector.getField().getType();
        Types.MinorType minorType = Types.getMinorTypeForArrowType(type);
        try {
            //Is this field the special 'row' field
            if (HbaseSchemaUtils.ROW_COLUMN_NAME.equals(fieldName)) {
                String value = Bytes.toString(row.getRow());
                BlockUtils.setValue(vector,
                        rowNum,
                        value);
                return constraintEvaluator.apply(fieldName, value);
            }

            switch (minorType) {
                case STRUCT:
                    //Column is actually a Column Family
                    BlockUtils.setComplexValue(vector,
                            rowNum,
                            HbaseFieldResolver.resolver(isNative, fieldName),
                            row);

                    //Constraints on complex types are not supported yet
                    return true;
                default:
                    //We expect the column name format to be <FAMILY>:<QUALIFIER>
                    String[] columnParts = HbaseSchemaUtils.extractColumnParts(fieldName);
                    byte[] rawValue = row.getValue(columnParts[0].getBytes(), columnParts[1].getBytes());
                    Object value = HbaseSchemaUtils.coerceType(isNative, type, rawValue);
                    BlockUtils.setValue(vector, rowNum, value);
                    return constraintEvaluator.apply(fieldName, value);
            }
        }
        catch (RuntimeException ex) {
            throw new RuntimeException("Exception while processing field " + fieldName + " type " + minorType, ex);
        }
    }

    private void convertField(Scan scan, Field field)
    {
        //ignore the special 'row' column
        if (HbaseSchemaUtils.ROW_COLUMN_NAME.equalsIgnoreCase(field.getName())) {
            return;
        }

        Types.MinorType columnType = Types.getMinorTypeForArrowType(field.getType());
        switch (columnType) {
            case STRUCT:
                for (Field child : field.getChildren()) {
                    scan.addColumn(field.getName().getBytes(UTF_8), child.getName().getBytes(UTF_8));
                }
                return;
            default:
                String[] nameParts = HbaseSchemaUtils.extractColumnParts(field.getName());
                if (nameParts.length != 2) {
                    throw new RuntimeException("Column name " + field.getName() + " does not meet family:column hbase convention.");
                }
                scan.addColumn(nameParts[0].getBytes(UTF_8), nameParts[1].getBytes(UTF_8));
        }
    }

    private Filter pushdownPredicate(boolean isNative, Constraints constraints)
    {
        for (Map.Entry<String, ValueSet> next : constraints.getSummary().entrySet()) {
            if (next.getValue().isSingleValue()) {
                String[] colParts = HbaseSchemaUtils.extractColumnParts(next.getKey());
                return new SingleColumnValueFilter(colParts[0].getBytes(),
                        colParts[1].getBytes(),
                        CompareFilter.CompareOp.EQUAL,
                        HbaseSchemaUtils.toBytes(isNative, next.getValue().getSingleValue()));
            }
        }

        return null;
    }
}
