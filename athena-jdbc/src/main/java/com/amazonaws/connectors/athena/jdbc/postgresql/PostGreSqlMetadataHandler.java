/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.connectors.athena.jdbc.postgresql;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.connectors.athena.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.connectors.athena.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.connectors.athena.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.connectors.athena.jdbc.manager.JDBCUtil;
import com.amazonaws.connectors.athena.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.connectors.athena.jdbc.manager.PreparedStatementBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handles metadata for PostGreSql. User must have access to `schemata`, `tables`, `columns`, `partitions` tables in
 * information_schema.
 */
public class PostGreSqlMetadataHandler
        extends JdbcMetadataHandler
{
    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
    static final String GET_PARTITIONS_QUERY = "SELECT nmsp_child.nspname AS child_schema, child.relname AS child FROM pg_inherits JOIN pg_class parent " +
            "ON pg_inherits.inhparent = parent.oid JOIN pg_class child ON pg_inherits.inhrelid = child.oid JOIN pg_namespace nmsp_parent " +
            "ON nmsp_parent.oid = parent.relnamespace JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace where nmsp_parent.nspname = ? " +
            "AND parent.relname = ?";
    static final String BLOCK_PARTITION_COLUMN_NAME = "partition_name";
    static final String BLOCK_PARTITION_SCHEMA_COLUMN_NAME = "partition_schema_name";
    static final String ALL_PARTITIONS = "*";
    private static final Logger LOGGER = LoggerFactory.getLogger(PostGreSqlMetadataHandler.class);
    private static final String PARTITION_SCHEMA_NAME = "child_schema";
    private static final String PARTITION_NAME = "child";
    private static final int MAX_SPLITS_PER_REQUEST = 1000_000;

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link com.amazonaws.connectors.athena.jdbc.MultiplexingJdbcCompositeHandler} instead.
     */
    public PostGreSqlMetadataHandler()
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(JdbcConnectionFactory.DatabaseEngine.POSTGRES));
    }

    public PostGreSqlMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig)
    {
        super(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES));
    }

    @VisibleForTesting
    protected PostGreSqlMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final AWSSecretsManager secretsManager,
            final AmazonAthena athena, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory);
    }

    @Override
    public Schema getPartitionSchema(final String catalogName)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(BLOCK_PARTITION_SCHEMA_COLUMN_NAME, Types.MinorType.VARCHAR.getType())
                .addField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
    }

    @Override
    public void getPartitions(final BlockWriter blockWriter, final GetTableLayoutRequest getTableLayoutRequest, QueryStatusChecker queryStatusChecker)
    {
        LOGGER.info("{}: Catalog {}, table {}", getTableLayoutRequest.getQueryId(), getTableLayoutRequest.getTableName().getSchemaName(),
                getTableLayoutRequest.getTableName().getTableName());
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            List<String> parameters = Arrays.asList(getTableLayoutRequest.getTableName().getSchemaName(),
                    getTableLayoutRequest.getTableName().getTableName());
            try (PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection).withQuery(GET_PARTITIONS_QUERY).withParameters(parameters).build();
                    ResultSet resultSet = preparedStatement.executeQuery()) {
                // Return a single partition if no partitions defined
                if (!resultSet.next()) {
                    blockWriter.writeRows((Block block, int rowNum) -> {
                        block.setValue(BLOCK_PARTITION_SCHEMA_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                        block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                        //we wrote 1 row so we return 1
                        return 1;
                    });
                }
                else {
                    do {
                        final String partitionSchemaName = resultSet.getString(PARTITION_SCHEMA_NAME);
                        final String partitionName = resultSet.getString(PARTITION_NAME);

                        // 1. Returns all partitions of table, we are not supporting constraints push down to filter partitions.
                        // 2. This API is not paginated, we could use order by and limit clause with offsets here.
                        blockWriter.writeRows((Block block, int rowNum) -> {
                            block.setValue(BLOCK_PARTITION_SCHEMA_COLUMN_NAME, rowNum, partitionSchemaName);
                            block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, partitionName);
                            //we wrote 1 row so we return 1
                            return 1;
                        });
                    }
                    while (resultSet.next());
                }
            }
        }
        catch (SQLException sqlException) {
            throw new RuntimeException(sqlException.getErrorCode() + ": " + sqlException.getMessage(), sqlException);
        }
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
    {
        LOGGER.info("{}: Catalog {}, table {}", getSplitsRequest.getQueryId(), getSplitsRequest.getTableName().getSchemaName(), getSplitsRequest.getTableName().getTableName());
        int partitionContd = decodeContinuationToken(getSplitsRequest);
        Set<Split> splits = new HashSet<>();
        Block partitions = getSplitsRequest.getPartitions();

        boolean splitterUsed = false;
        if (partitions.getRowCount() == 1) {
            FieldReader partitionsSchemaFieldReader = partitions.getFieldReader(BLOCK_PARTITION_SCHEMA_COLUMN_NAME);
            partitionsSchemaFieldReader.setPosition(0);
            FieldReader partitionsFieldReader = partitions.getFieldReader(BLOCK_PARTITION_COLUMN_NAME);
            partitionsFieldReader.setPosition(0);

            if (ALL_PARTITIONS.equals(partitionsSchemaFieldReader.readText().toString()) && ALL_PARTITIONS.equals(partitionsFieldReader.readText().toString())) {
                for (String splitClause : getSplitClauses(getSplitsRequest.getTableName())) {
                    //Every split must have a unique location if we wish to spill to avoid failures
                    SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);

                    Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                            .add(BLOCK_PARTITION_SCHEMA_COLUMN_NAME, String.valueOf(partitionsSchemaFieldReader.readText()))
                            .add(BLOCK_PARTITION_COLUMN_NAME, String.valueOf(splitClause));

                    splits.add(splitBuilder.build());

                    if (splits.size() >= MAX_SPLITS_PER_REQUEST) {
                        throw new RuntimeException("Max splits supported with splitter " + MAX_SPLITS_PER_REQUEST);
                    }

                    splitterUsed = true;
                }
            }
        }

        if (!splitterUsed) {
            for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
                FieldReader partitionsSchemaFieldReader = partitions.getFieldReader(BLOCK_PARTITION_SCHEMA_COLUMN_NAME);
                partitionsSchemaFieldReader.setPosition(curPartition);
                FieldReader partitionsFieldReader = partitions.getFieldReader(BLOCK_PARTITION_COLUMN_NAME);
                partitionsFieldReader.setPosition(curPartition);

                //Every split must have a unique location if we wish to spill to avoid failures
                SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);

                LOGGER.info("{}: Input partition is {}", getSplitsRequest.getQueryId(), String.valueOf(partitionsFieldReader.readText()));
                Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                        .add(BLOCK_PARTITION_SCHEMA_COLUMN_NAME, String.valueOf(partitionsSchemaFieldReader.readText()))
                        .add(BLOCK_PARTITION_COLUMN_NAME, String.valueOf(partitionsFieldReader.readText()));

                splits.add(splitBuilder.build());

                if (splits.size() >= MAX_SPLITS_PER_REQUEST) {
                    //We exceeded the number of split we want to return in a single request, return and provide a continuation token.
                    return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, encodeContinuationToken(curPartition));
                }
            }
        }

        return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, null);
    }

    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.valueOf(request.getContinuationToken());
        }

        //No continuation token present
        return 0;
    }

    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }
}
