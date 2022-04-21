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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.hbase.connection.HBaseConnection;
import com.amazonaws.athena.connectors.hbase.connection.HbaseConnectionFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
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
                AmazonAthenaClientBuilder.defaultClient(),
                new HbaseConnectionFactory());
    }

    @VisibleForTesting
    protected HbaseRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena athena, HbaseConnectionFactory connectionFactory)
    {
        super(amazonS3, secretsManager, athena, SOURCE_TYPE);
        this.amazonS3 = amazonS3;
        this.connectionFactory = connectionFactory;
    }

    private HBaseConnection getOrCreateConn(String conStr)
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
    protected void readWithConstraint(BlockSpiller blockSpiller, ReadRecordsRequest request, QueryStatusChecker queryStatusChecker)
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

        getOrCreateConn(conStr).scanTable(HbaseSchemaUtils.getQualifiedTable(request.getTableName()),
                scan,
                (ResultScanner scanner) -> scanFilterProject(scanner, request, blockSpiller, queryStatusChecker));
    }

    private boolean scanFilterProject(ResultScanner scanner, ReadRecordsRequest request, BlockSpiller blockSpiller, QueryStatusChecker queryStatusChecker)
    {
        Schema projection = request.getSchema();
        boolean isNative = projection.getCustomMetadata().get(HBASE_NATIVE_STORAGE_FLAG) != null;

        for (Result row : scanner) {
            if (!queryStatusChecker.isQueryRunning()) {
                return true;
            }
            blockSpiller.writeRows((Block block, int rowNum) -> {
                boolean match = true;
                for (Field field : projection.getFields()) {
                    if (match) {
                        match &= writeField(block, field, isNative, row, rowNum);
                    }
                }
                return match ? 1 : 0;
            });
        }
        return true;
    }

    /**
     * Used to filter and write field values from the HBase scan to the response block.
     *
     * @param block The Block we should write to.
     * @param field The Apache Arrow Field we need to write.
     * @param isNative Boolean indicating if the HBase value is stored as a String (false) or as Native byte[] (true).
     * @param row The HBase row from which we should extract a value for the field denoted by vector.
     * @param rowNum The rowNumber to write into on the vector.
     * @return True if the value passed the ConstraintEvaluator's test.
     */
    private boolean writeField(Block block, Field field, boolean isNative, Result row, int rowNum)
    {
        String fieldName = field.getName();
        ArrowType type = field.getType();
        Types.MinorType minorType = Types.getMinorTypeForArrowType(type);
        try {
            //Is this field the special 'row' field that can be used to group column families that may
            //have been spread across different region servers if they are needed in the same query.
            if (HbaseSchemaUtils.ROW_COLUMN_NAME.equals(fieldName)) {
                String value = Bytes.toString(row.getRow());
                return block.offerValue(fieldName, rowNum, value);
            }

            switch (minorType) {
                case STRUCT:
                    //Column is actually a Column Family stored as a STRUCT.
                    return block.offerComplexValue(fieldName,
                            rowNum,
                            HbaseFieldResolver.resolver(isNative, fieldName),
                            row);
                default:
                    //We expect the column name format to be <FAMILY>:<QUALIFIER>
                    String[] columnParts = HbaseSchemaUtils.extractColumnParts(fieldName);
                    byte[] rawValue = row.getValue(columnParts[0].getBytes(), columnParts[1].getBytes());
                    Object value = HbaseSchemaUtils.coerceType(isNative, type, rawValue);
                    return block.offerValue(fieldName, rowNum, value);
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
     * or RowFilter and CompareOp of EQUAL. In the future we can add > and < for certain field types.
     */
    private Filter pushdownPredicate(boolean isNative, Constraints constraints)
    {
        for (Map.Entry<String, ValueSet> next : constraints.getSummary().entrySet()) {
            if (next.getValue().isSingleValue() && !next.getValue().isNullAllowed()) {
                byte[] value = HbaseSchemaUtils.toBytes(isNative, next.getValue().getSingleValue());
                String[] colParts = HbaseSchemaUtils.extractColumnParts(next.getKey());
                CompareFilter.CompareOp compareOp = CompareFilter.CompareOp.EQUAL;
                boolean isRowKey = next.getKey().equals(HbaseSchemaUtils.ROW_COLUMN_NAME);

                return isRowKey ?
                       new RowFilter(compareOp, new BinaryComparator(value)) :
                       new SingleColumnValueFilter(colParts[0].getBytes(), colParts[1].getBytes(), compareOp, value);
            }
        }

        return null;
    }
}
