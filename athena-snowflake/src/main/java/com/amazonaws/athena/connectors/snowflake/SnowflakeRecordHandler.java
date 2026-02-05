/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.athena.connectors.snowflake.connection.SnowflakeConnectionFactory;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.utils.Validate;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.BLOCK_PARTITION_COLUMN_NAME;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.JDBC_PROPERTIES;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_EXPORT_BUCKET;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_OBJECT_KEY;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_QUERY_ID;

public class SnowflakeRecordHandler extends JdbcRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeRecordHandler.class);
    private final JdbcConnectionFactory jdbcConnectionFactory;
    private static final int EXPORT_READ_BATCH_SIZE_BYTE = 32768;
    private static final int FETCH_SIZE = 1000;
    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;

    /**
     * Instantiates handler to be used by Lambda function directly.
     * <p>
     */
    public SnowflakeRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(SnowflakeConstants.SNOWFLAKE_NAME, configOptions), configOptions);
    }

    public SnowflakeRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig,
                new SnowflakeConnectionFactory(databaseConnectionConfig, SnowflakeEnvironmentProperties.getSnowFlakeParameter(JDBC_PROPERTIES, configOptions),
                new DatabaseConnectionInfo(SnowflakeConstants.SNOWFLAKE_DRIVER_CLASS,
                        SnowflakeConstants.SNOWFLAKE_DEFAULT_PORT)), configOptions);
    }

    public SnowflakeRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, GenericJdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, S3Client.create(), SecretsManagerClient.create(), AthenaClient.create(),
                jdbcConnectionFactory, new SnowflakeQueryStringBuilder(SnowflakeConstants.DOUBLE_QUOTE_CHAR, new SnowflakeFederationExpressionParser(SnowflakeConstants.DOUBLE_QUOTE_CHAR)), configOptions);
    }

    @VisibleForTesting
    public SnowflakeRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, S3Client amazonS3, SecretsManagerClient secretsManager, AthenaClient athena, JdbcConnectionFactory jdbcConnectionFactory, JdbcSplitQueryBuilder jdbcSplitQueryBuilder, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory, configOptions);
        this.jdbcConnectionFactory = jdbcConnectionFactory;
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null");
    }

    /**
     * Used to handle data transfer between Snowflake and Athena, supporting both direct query and S3 export paths,
     * converts to arrow format, and manages spillover logic.
     *
     * @param spiller            A BlockSpiller that should be used to write the row data associated with this Split.
     *                           The BlockSpiller automatically handles chunking the response, encrypting, and spilling to S3.
     * @param recordsRequest     Details of the read request, including:
     *                           1. The Split
     *                           2. The Catalog, Database, and Table the read request is for.
     *                           3. The filtering predicate (if any)
     *                           4. The columns required for projection.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @throws Exception       Throws an Exception
     */
    @Override
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        if (SnowflakeConstants.isS3ExportEnabled(configOptions)) {
            // Use S3 export path for data transfer
            handleS3ExportRead(spiller, recordsRequest, queryStatusChecker);
        }
        else {
            // Use traditional direct query path
            handleDirectRead(spiller, recordsRequest, queryStatusChecker);
        }
    }

    private void handleS3ExportRead(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker) throws IOException
    {
        LOGGER.info("handleS3ExportRead: schema[{}] tableName[{}]", recordsRequest.getSchema(), recordsRequest.getTableName());
        Split split = recordsRequest.getSplit();
        String id = split.getProperty(SNOWFLAKE_SPLIT_QUERY_ID);
        String exportBucket = split.getProperty(SNOWFLAKE_SPLIT_EXPORT_BUCKET);
        String s3ObjectKey = split.getProperty(SNOWFLAKE_SPLIT_OBJECT_KEY);

        if (s3ObjectKey.isEmpty()) {
            LOGGER.debug("S3 object key is empty from request, skip read from S3");
            return;
        }

        String s3path = constructS3Uri(exportBucket, s3ObjectKey);
        try (ArrowReader reader = constructArrowReader(s3path, recordsRequest.getSchema())) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                // use writeRows method as it handle the spilling to s3.
                spiller.writeRows((Block block, int startRowNum) -> {
                    // Copy vectors directly to the block
                    // Write all rows in the batch at once
                    for (Field field : root.getSchema().getFields()) {
                        if (recordsRequest.getSchema().findField(field.getName()) != null) {
                            FieldVector originalVector = block.getFieldVector(field.getName());
                            FieldVector toAppend = root.getVector(field.getName());

                            // to_append block, both vector must be same time
                            // However, Athena treat TSWithTZ as DateTimeMilli(UTC), hence we will need a conversion from TimeStampMilliTZ to DateTimeMilli
                            if (toAppend instanceof TimeStampMilliTZVector) {
                               toAppend = convertTimestampTZMilliToDateMilliFast((TimeStampMilliTZVector) toAppend, toAppend.getAllocator());
                            }

                            VectorAppender appender = new VectorAppender(originalVector);
                            toAppend.accept(appender, null);
                        }
                    }
                    return root.getRowCount();
                });
            }
        }
        catch (Exception e) {
            throw new AthenaConnectorException("Error in object content for object : " + s3path + " " + e.getMessage(), e,
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }
    }

    private void handleDirectRead(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
      super.readWithConstraint(spiller, recordsRequest, queryStatusChecker);
    }

    @VisibleForTesting
    protected ArrowReader constructArrowReader(String uri, Schema schema)
    {
        LOGGER.debug("URI {}", uri);
        BufferAllocator allocator = new RootAllocator();
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                allocator,
                NativeMemoryPool.getDefault(),
                FileFormat.PARQUET,
                uri);
        Dataset dataset = datasetFactory.finish();

        // do a scan projection, only getting the column we want
        ScanOptions options = new ScanOptions(/*batchSize*/ EXPORT_READ_BATCH_SIZE_BYTE,
                Optional.of(schema.getFields().stream()
                        .map(Field::getName)
                        .filter(name -> !name.equalsIgnoreCase(BLOCK_PARTITION_COLUMN_NAME))
                        .toArray(String[]::new))); // Project the column we needed only.

        Scanner scanner = dataset.newScan(options);
        return scanner.scanBatches();
    }

    private static String constructS3Uri(String bucket, String key)
    {
        return "s3://" + bucket + "/" + key;
    }

    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableNameInput, Schema schema, Constraints constraints, Split split) throws SQLException
    {
        PreparedStatement preparedStatement;
        try {
            if (constraints.isQueryPassThrough()) {
                preparedStatement = buildQueryPassthroughSql(jdbcConnection, constraints);
            }
            else {
                preparedStatement = jdbcSplitQueryBuilder.buildSql(jdbcConnection, null, tableNameInput.getSchemaName(), tableNameInput.getTableName(), schema, constraints, split);
            }

            // Disable fetching all rows.
            preparedStatement.setFetchSize(FETCH_SIZE);
        }
        catch (SQLException e) {
            throw new AthenaConnectorException(e.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }
        return preparedStatement;
    }

    // TSmilli and DateTimeMilli vector both have same width of 8bytes and same data type(long)
    // direct copy the value.
     static DateMilliVector convertTimestampTZMilliToDateMilliFast(
            TimeStampMilliTZVector tsVector,
            BufferAllocator allocator)
     {
        // record's timezone must be in UTC
        if (!tsVector.getTimeZone().equalsIgnoreCase("UTC")) {
            throw new IllegalArgumentException("Athena S3 Export only support Timezone with UTC");
        }

        int rowCount = tsVector.getValueCount();
        DateMilliVector resultVector = new DateMilliVector(tsVector.getName(), allocator);
        resultVector.allocateNew(rowCount);

        // Copy data buffer directly to save time
        ArrowBuf srcData = tsVector.getDataBuffer();
        ArrowBuf dstData = resultVector.getDataBuffer();
        long bytes = (long) rowCount * Long.BYTES;

        // copy the data value into destination
        dstData.setBytes(0, srcData, 0, bytes);

        // copy the bitmap as well, otherwise it will show as empty(no value present)
        ArrowBuf srcValidity = tsVector.getValidityBuffer();
        ArrowBuf dstValidity = resultVector.getValidityBuffer();

        dstValidity.setBytes(
                0,
                srcValidity,
                0,
                BitVectorHelper.getValidityBufferSize(rowCount)
        );

        // finalized the actual value, without this vector will see no data.
        resultVector.setValueCount(rowCount);

        return resultVector;
    }

    @Override
    public CredentialsProvider createCredentialsProvider(String secretName, AwsRequestOverrideConfiguration requestOverrideConfiguration)
    {
        return new SnowflakeCredentialsProvider(secretName, requestOverrideConfiguration);
    }
}
