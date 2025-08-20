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
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateDayExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.SmallIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.TinyIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarBinaryExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.awssdk.utils.Validate;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;

import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_EXPORT_BUCKET;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_OBJECT_KEY;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_QUERY_ID;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeMetadataHandler.JDBC_PROPERTIES;

public class SnowflakeRecordHandler extends JdbcRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeRecordHandler.class);
    private final JdbcConnectionFactory jdbcConnectionFactory;
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
                jdbcConnectionFactory, new SnowflakeQueryStringBuilder(SnowflakeConstants.SNOWFLAKE_QUOTE_CHARACTER, new SnowflakeFederationExpressionParser(SnowflakeConstants.SNOWFLAKE_QUOTE_CHARACTER)), configOptions);
    }

    @VisibleForTesting
    SnowflakeRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, S3Client amazonS3, SecretsManagerClient secretsManager, AthenaClient athena, JdbcConnectionFactory jdbcConnectionFactory, JdbcSplitQueryBuilder jdbcSplitQueryBuilder, java.util.Map<String, String> configOptions)
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
        SnowflakeEnvironmentProperties envProperties = new SnowflakeEnvironmentProperties(System.getenv());
        
        if (envProperties.isS3ExportEnabled()) {
            // Use S3 export path for data transfer
            handleS3ExportRead(spiller, recordsRequest, queryStatusChecker);
        }
        else {
            // Use traditional direct query path
            handleDirectRead(spiller, recordsRequest, queryStatusChecker);
        }
    }

    private void handleS3ExportRead(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
    {
        LOGGER.info("handleS3ExportRead: schema[{}] tableName[{}]", recordsRequest.getSchema(), recordsRequest.getTableName());

        Schema schemaName = recordsRequest.getSchema();
        Split split = recordsRequest.getSplit();
        String id = split.getProperty(SNOWFLAKE_SPLIT_QUERY_ID);
        String exportBucket = split.getProperty(SNOWFLAKE_SPLIT_EXPORT_BUCKET);
        String s3ObjectKey = split.getProperty(SNOWFLAKE_SPLIT_OBJECT_KEY);

        if (!s3ObjectKey.isEmpty()) {
            //get column name and type from the Schema
            HashMap<String, Types.MinorType> mapOfNamesAndTypes = new HashMap<>();
            HashMap<String, Object> mapOfCols = new HashMap<>();

            for (Field field : schemaName.getFields()) {
                Types.MinorType minorTypeForArrowType = Types.getMinorTypeForArrowType(field.getType());
                mapOfNamesAndTypes.put(field.getName(), minorTypeForArrowType);
                mapOfCols.put(field.getName(), null);
            }

            // creating a RowContext class to hold the column name and value.
            final RowContext rowContext = new RowContext(id);

            //Generating the RowWriter and Extractor
            GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());
            for (Field next : recordsRequest.getSchema().getFields()) {
                Extractor extractor = makeExtractor(next, mapOfNamesAndTypes, mapOfCols);
                builder.withExtractor(next.getName(), extractor);
            }
            GeneratedRowWriter rowWriter = builder.build();

            /*
            Using Arrow Dataset to read the S3 Parquet file generated in the split
            */
            try (ArrowReader reader = constructArrowReader(constructS3Uri(exportBucket, s3ObjectKey))) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    for (int row = 0; row < root.getRowCount(); row++) {
                        HashMap<String, Object> map = new HashMap<>();
                        for (Field field : root.getSchema().getFields()) {
                            map.put(field.getName(), root.getVector(field).getObject(row));
                        }
                        rowContext.setNameValue(map);

                        //Passing the RowContext to BlockWriter;
                        spiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, rowContext) ? 1 : 0);
                    }
                }
            }
            catch (Exception e) {
                throw new AthenaConnectorException("Error in object content for object : " + s3ObjectKey + " " + e.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
            }
        }
    }

    private void handleDirectRead(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
      super.readWithConstraint(spiller, recordsRequest, queryStatusChecker);
    }

    /**
     * Creates an Extractor for the given field.
     */
    private Extractor makeExtractor(Field field, HashMap<String, Types.MinorType> mapOfNamesAndTypes, HashMap<String, Object> mapOfcols)
    {
        String fieldName = field.getName();
        Types.MinorType fieldType = mapOfNamesAndTypes.get(fieldName);
        switch (fieldType) {
            case BIT:
                return (BitExtractor) (Object context, NullableBitHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if (value == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = ((boolean) value) ? 1 : 0;
                        dst.isSet = 1;
                    }
                };
            case TINYINT:
                return (TinyIntExtractor) (Object context, NullableTinyIntHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if (value == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = Byte.parseByte(value.toString());
                        dst.isSet = 1;
                    }
                };
            case SMALLINT:
                return (SmallIntExtractor) (Object context, NullableSmallIntHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if (value == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = Short.parseShort(value.toString());
                        dst.isSet = 1;
                    }
                };
            case INT:
            case BIGINT:
                return (BigIntExtractor) (Object context, NullableBigIntHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if (value == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = Long.parseLong(value.toString());
                        dst.isSet = 1;
                    }
                };
            case FLOAT4:
                return (Float4Extractor) (Object context, NullableFloat4Holder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if (value == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = Float.parseFloat(value.toString());
                        dst.isSet = 1;
                    }
                };
            case FLOAT8:
                return (Float8Extractor) (Object context, NullableFloat8Holder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if (value == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = Double.parseDouble(value.toString());
                        dst.isSet = 1;
                    }
                };
            case DECIMAL:
                return (DecimalExtractor) (Object context, NullableDecimalHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if (value == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = new BigDecimal(value.toString());
                        dst.isSet = 1;
                    }
                };
            case DATEDAY:
                return (DateDayExtractor) (Object context, NullableDateDayHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if (value == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = (int) value;
                        dst.isSet = 1;
                    }
                };
            case DATEMILLI:
                return (DateMilliExtractor) (Object context, NullableDateMilliHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if (value == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = (long) value;
                        dst.isSet = 1;
                    }
                };
            case VARCHAR:
                return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if (value == null) {
                        dst.isSet = 0;
                    }
                    else {
                        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");
                        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
                        try {
                            // Try parsing the input as a datetime string
                            LocalDateTime dateTime = LocalDateTime.parse(value.toString(), inputFormatter);
                            // If successful, return formatted time
                            dst.value = dateTime.toLocalTime().format(outputFormatter);
                        }
                        catch (DateTimeParseException e) {
                            // If parsing fails, return input as is
                            dst.value = value.toString();
                        }
                        dst.isSet = 1;
                    }
                };
            case VARBINARY:
                return (VarBinaryExtractor) (Object context, NullableVarBinaryHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if (value == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = value.toString().getBytes();
                        dst.isSet = 1;
                    }
                };
            default:
                throw new AthenaConnectorException("Unhandled type " + fieldType, ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }
    }

    private static class RowContext
    {
        private final String queryId;
        private HashMap<String, Object> nameValue;

        public RowContext(String queryId)
        {
            this.queryId = queryId;
        }

        public void setNameValue(HashMap<String, Object> map)
        {
            this.nameValue = map;
        }

        public HashMap<String, Object> getNameValue()
        {
            return this.nameValue;
        }
    }

    @VisibleForTesting
    protected ArrowReader constructArrowReader(String uri)
    {
        LOGGER.debug("URI {}", uri);
        BufferAllocator allocator = new RootAllocator();
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                allocator,
                NativeMemoryPool.getDefault(),
                FileFormat.PARQUET,
                uri);
        Dataset dataset = datasetFactory.finish();
        ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
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

    @Override
    protected CredentialsProvider getCredentialProvider()
    {
        final String secretName = getDatabaseConnectionConfig().getSecret();
        if (StringUtils.isNotBlank(secretName)) {
            return new SnowflakeCredentialsProvider(secretName);
        }

        return null;
    }
}
