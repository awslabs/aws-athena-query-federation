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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
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
import org.apache.hadoop.hbase.client.Connection;
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

/**
 * Handles data read record requests for the Athena HBase Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Supporting String and native 'byte[]' storage.
 * 2. Attempts to resolve sensitive configuration fields such as HBase connection string via SecretsManager so that you can
 * substitute variables with values from by doing something like hostname:port:password=${my_secret}
 */
public class HbaseRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(HbaseRecordHandler.class);

    //Used to denote the 'type' of this connector for diagnostic purposes.
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

    /**
     * Scans HBase using the scan settings set on the requested Split by HbaseMetadataHandler.
     *
     * @see RecordHandler
     */
    @Override
    protected void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller blockSpiller, ReadRecordsRequest request)
            throws IOException
    {
        Schema projection = request.getSchema();
        Split split = request.getSplit();
        String conStr = split.getProperty(HBASE_CONN_STR);
        boolean isNative = projection.getCustomMetadata().get(HBASE_NATIVE_STORAGE_FLAG) != null;

        //setup the scan so that we only read the key range associated with the region represented by our Split.
        Scan scan = new Scan(split.getProperty(START_KEY_FIELD).getBytes(), split.getProperty(END_KEY_FIELD).getBytes());

        //attempts to push down a partial predicate using HBase Filters
        scan.setFilter(pushdownPredicate(isNative, request.getConstraints()));

        //setup the projection so we only pull columns/families that we need
        for (Field next : request.getSchema().getFields()) {
            addToProjection(scan, next);
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

    /**
     * Used to filter and write field values from the HBase scan to the response block.
     *
     * @param constraintEvaluator Used to applied constraints (predicates) to field values before writing them.
     * @param vector The Apache Arrow vector for the field we need to write.
     * @param isNative Boolean indicating if the HBase value is stored as a String (false) or as Native byte[] (true).
     * @param row The HBase row from which we should extract a value for the field denoted by vector.
     * @param rowNum The rowNumber to write into on the vector.
     * @return True if the value passed the ConstraintEvaluator's test.
     */
    private boolean writeField(ConstraintEvaluator constraintEvaluator, FieldVector vector, boolean isNative, Result row, int rowNum)
    {
        String fieldName = vector.getField().getName();
        ArrowType type = vector.getField().getType();
        Types.MinorType minorType = Types.getMinorTypeForArrowType(type);
        try {
            //Is this field the special 'row' field that can be used to group column families that may
            //have been spread across different region servers if they are needed in the same query.
            if (HbaseSchemaUtils.ROW_COLUMN_NAME.equals(fieldName)) {
                String value = Bytes.toString(row.getRow());
                BlockUtils.setValue(vector,
                        rowNum,
                        value);
                return constraintEvaluator.apply(fieldName, value);
            }

            switch (minorType) {
                case STRUCT:
                    //Column is actually a Column Family stored as a STRUCT.
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

    /**
     * Addes the specified Apache Arrow field to the Scan to satisfy the requested projection.
     *
     * @param scan The scan object that will be used to read data from HBase.
     * @param field The field to be added to the scan.
     */
    private void addToProjection(Scan scan, Field field)
    {
        //ignore the special 'row' column since we get that by default.
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

    /**
     * Attempts to push down at basic Filter predicate into HBase.
     *
     * @param isNative True if the values are stored in HBase using native byte[] vs being serialized as Strings.
     * @param constraints The constraints that we can attempt to push into HBase as part of the scan.
     * @return A filter if we found a predicate we can push down, null otherwise/
     * @note Currently this method only supports constraints that can be represented by HBase's SingleColumnValueFilter
     * and CompareOp of EQUAL. In the future we can add > and < for certain field types.
     */
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
