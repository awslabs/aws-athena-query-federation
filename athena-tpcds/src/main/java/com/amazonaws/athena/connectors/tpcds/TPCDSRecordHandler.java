/*-
 * #%L
 * athena-tpcds
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
package com.amazonaws.athena.connectors.tpcds;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.teradata.tpcds.Results;
import com.teradata.tpcds.Session;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.Column;
import com.teradata.tpcds.column.ColumnType;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler.SPLIT_NUMBER_FIELD;
import static com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler.SPLIT_SCALE_FACTOR_FIELD;
import static com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler.SPLIT_TOTAL_NUMBER_FIELD;
import static com.teradata.tpcds.Results.constructResults;

/**
 * Handles data read record requests for the Athena TPC-DS Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Generates data for the requested table on the fly.
 * 2. Applies constraints to the data as it is generated, emulating predicate-pushdown.
 */
public class TPCDSRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(TPCDSRecordHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "tpcds";

    public TPCDSRecordHandler(java.util.Map<String, String> configOptions)
    {
        super(AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), AmazonAthenaClientBuilder.defaultClient(), SOURCE_TYPE, configOptions);
    }

    @VisibleForTesting
    protected TPCDSRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena athena, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, SOURCE_TYPE, configOptions);
    }

    /**
     * Generated TPCDS data for the given Table and scale factor as defined by the requested Split.
     *
     * @see RecordHandler
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
            throws IOException
    {
        Split split = recordsRequest.getSplit();
        int splitNumber = Integer.parseInt(split.getProperty(SPLIT_NUMBER_FIELD));
        int totalNumSplits = Integer.parseInt(split.getProperty(SPLIT_TOTAL_NUMBER_FIELD));
        int scaleFactor = Integer.parseInt(split.getProperty(SPLIT_SCALE_FACTOR_FIELD));
        Table table = validateTable(recordsRequest.getTableName());

        Session session = Session.getDefaultSession()
                .withScale(scaleFactor)
                .withParallelism(totalNumSplits)
                .withChunkNumber(splitNumber + 1)
                .withTable(table)
                .withNoSexism(true);

        Results results = constructResults(table, session);
        Iterator<List<List<String>>> itr = results.iterator();

        Map<Integer, CellWriter> writers = makeWriters(recordsRequest.getSchema(), table);
        while (itr.hasNext() && queryStatusChecker.isQueryRunning()) {
            List<String> row = itr.next().get(0);
            spiller.writeRows((Block block, int numRow) -> {
                boolean matched = true;
                for (Map.Entry<Integer, CellWriter> nextWriter : writers.entrySet()) {
                    matched &= nextWriter.getValue().write(block, numRow, row.get(nextWriter.getKey()));
                }
                return matched ? 1 : 0;
            });
        }
    }

    /**
     * Required that the requested Table be present in the TPCDS generated schema.
     *
     * @param tableName The fully qualified name of the requested table.
     * @return The TPCDS table, if present, otherwise the method throws.
     */
    private Table validateTable(TableName tableName)
    {
        Optional<Table> table = Table.getBaseTables().stream()
                .filter(next -> next.getName().equals(tableName.getTableName()))
                .findFirst();

        if (!table.isPresent()) {
            throw new RuntimeException("Unknown table " + tableName);
        }

        return table.get();
    }

    /**
     * Generates the CellWriters used to convert the TPCDS Generators data to Apache Arrow.
     *
     * @param schemaForRead The schema to read/project.
     * @param table The TPCDS Table we are reading from.
     * @return Map<Integer, CellWriter> where integer is the Column position in the TPCDS data set and the CellWriter
     * can be used to read,convert,write the value at that position for any row into the correct position and type
     * in our Apache Arrow response.
     */
    private Map<Integer, CellWriter> makeWriters(Schema schemaForRead, Table table)
    {
        Map<String, Column> columnPositions = new HashMap<>();
        for (Column next : table.getColumns()) {
            columnPositions.put(next.getName(), next);
        }

        //We use this approach to reduce the overhead of field lookups. This isn't as good as true columnar processing
        //using Arrow but it gets us ~80% of the way there from a rows/second per cpu-cycle perspective.
        Map<Integer, CellWriter> writers = new HashMap<>();
        for (Field nextField : schemaForRead.getFields()) {
            Column column = columnPositions.get(nextField.getName());
            writers.put(column.getPosition(), makeWriter(nextField, column));
        }
        return writers;
    }

    /**
     * Makes a CellWriter for the provided Apache Arrow Field and TPCDS Column.
     *
     * @param field The Apache Arrow Field.
     * @param column The corresponding TPCDS Column.
     * @return The CellWriter that can be used to convert and write values for the provided Field/Column pair.
     */
    private CellWriter makeWriter(Field field, Column column)
    {
        ColumnType type = column.getType();
        switch (type.getBase()) {
            case IDENTIFIER:
                return (Block block, int rowNum, String rawValue) -> {
                    Long value = (rawValue != null) ? Long.parseLong(rawValue) : null;
                    return block.setValue(field.getName(), rowNum, value);
                };
            case INTEGER:
                return (Block block, int rowNum, String rawValue) -> {
                    Integer value = (rawValue != null) ? Integer.parseInt(rawValue) : null;
                    return block.setValue(field.getName(), rowNum, value);
                };
            case DATE:
                return (Block block, int rowNum, String rawValue) -> {
                    LocalDate value = (rawValue != null) ? LocalDate.parse(rawValue) : null;
                    return block.setValue(field.getName(), rowNum, value);
                };
            case DECIMAL:
                return (Block block, int rowNum, String rawValue) -> {
                    BigDecimal value = (rawValue != null) ? new BigDecimal(rawValue) : null;
                    return block.setValue(field.getName(), rowNum, value);
                };
            case TIME:
            case CHAR:
            case VARCHAR:
                return (Block block, int rowNum, String rawValue) -> {
                    return block.setValue(field.getName(), rowNum, rawValue);
                };
        }
        throw new IllegalArgumentException("Unsupported TPC-DS type " + column.getName() + ":" + column.getType().getBase());
    }

    public interface CellWriter
    {
        /**
         * Converts a value from TPCDS' string representation into the appropriate Apache Arrow type
         * and writes it to the correct field in the provided Block and row. The implementation should
         * also apply constraints as an optimization.
         *
         * @param block The Apache Arrow Block to write into.
         * @param rowNum The row number in the Arrow Block to write into.
         * @param value The value to convert and write into the Apache Arrow Block.
         * @return True if the value passed all Contraints.
         */
        boolean write(Block block, int rowNum, String value);
    }
}
