
/*-
 * #%L
 * athena-google-bigquery
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.google.bigquery.qpt.BigQueryQueryPassthrough;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static com.amazonaws.athena.connectors.google.bigquery.BigQueryExceptionFilter.EXCEPTION_FILTER;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryUtils.fixCaseForDatasetName;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryUtils.fixCaseForTableName;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryUtils.getObjectFromFieldValue;
import static org.apache.arrow.vector.types.Types.getMinorTypeForArrowType;

/**
 * This record handler is an example of how you can implement a lambda that calls bigquery and pulls data.
 * This Lambda requires that your BigQuery table is small enough so that a table scan can be completed
 * within 5-10 mins or this lambda will time out and it will fail.
 */
public class BigQueryRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(BigQueryRecordHandler.class);
    private final ThrottlingInvoker invoker;
    BufferAllocator allocator;

    private final BigQueryQueryPassthrough queryPassthrough = new BigQueryQueryPassthrough();

    BigQueryRecordHandler(java.util.Map<String, String> configOptions, BufferAllocator allocator)
    {
        this(AmazonS3ClientBuilder.defaultClient(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient(), configOptions, allocator);
    }

    @VisibleForTesting
    public BigQueryRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena athena, java.util.Map<String, String> configOptions, BufferAllocator allocator)
    {
        super(amazonS3, secretsManager, athena, BigQueryConstants.SOURCE_TYPE, configOptions);
        this.invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER, configOptions).build();
        this.allocator = allocator;
    }

    @Override
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker) throws Exception
    {
        List<QueryParameterValue> parameterValues = new ArrayList<>();
        invoker.setBlockSpiller(spiller);
        BigQuery bigQueryClient = BigQueryUtils.getBigQueryClient(configOptions);

        if (recordsRequest.getConstraints().isQueryPassThrough()) {
            handleQueryPassthrough(spiller, recordsRequest, queryStatusChecker, parameterValues, bigQueryClient);
        }
        else {
            handleStandardQuery(spiller, recordsRequest, queryStatusChecker, parameterValues, bigQueryClient);
        }
    }

    private void handleStandardQuery(BlockSpiller spiller,
                                     ReadRecordsRequest recordsRequest,
                                     QueryStatusChecker queryStatusChecker,
                                     List<QueryParameterValue> parameterValues,
                                     BigQuery bigQueryClient) throws Exception
    {
        String projectName = configOptions.get(BigQueryConstants.GCP_PROJECT_ID);
        String datasetName = fixCaseForDatasetName(projectName, recordsRequest.getTableName().getSchemaName(), bigQueryClient);
        String tableName = fixCaseForTableName(projectName, datasetName, recordsRequest.getTableName().getTableName(), bigQueryClient);

        TableId tableId = TableId.of(projectName, datasetName, tableName);
        TableDefinition.Type type = bigQueryClient.getTable(tableId).getDefinition().getType();

        if (type.equals(TableDefinition.Type.TABLE)) {
            getTableData(spiller, recordsRequest, parameterValues, projectName, datasetName, tableName);
        }
        else {
            getData(spiller, recordsRequest, queryStatusChecker, parameterValues, bigQueryClient, datasetName, tableName);
        }
    }

    private void handleQueryPassthrough(BlockSpiller spiller,
                         ReadRecordsRequest recordsRequest,
                         QueryStatusChecker queryStatusChecker,
                         List<QueryParameterValue> parameterValues,
                         BigQuery bigQueryClient) throws TimeoutException
    {
        Map<String, String> queryPassthroughArgs = recordsRequest.getConstraints().getQueryPassthroughArguments();
        queryPassthrough.verify(queryPassthroughArgs);
        String query = queryPassthroughArgs.get(BigQueryQueryPassthrough.QUERY);
        getData(spiller, recordsRequest, queryStatusChecker, parameterValues, bigQueryClient, query);
    }

    private void getData(BlockSpiller spiller,
                         ReadRecordsRequest recordsRequest,
                         QueryStatusChecker queryStatusChecker,
                         List<QueryParameterValue> parameterValues,
                         BigQuery bigQueryClient,
                         String datasetName, String tableName) throws TimeoutException
    {
        String query = BigQuerySqlUtils.buildSql(new TableName(datasetName, tableName),
                recordsRequest.getSchema(), recordsRequest.getConstraints(), parameterValues);
        getData(spiller, recordsRequest, queryStatusChecker, parameterValues, bigQueryClient, query);
    }

    private void getData(BlockSpiller spiller,
                         ReadRecordsRequest recordsRequest,
                         QueryStatusChecker queryStatusChecker,
                         List<QueryParameterValue> parameterValues,
                         BigQuery bigQueryClient,
                         String query) throws TimeoutException
    {
        logger.debug("Got Request with constraints: {}", recordsRequest.getConstraints());
        logger.debug("Executing SQL Query: {} for Split: {}", query, recordsRequest.getSplit());
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).setPositionalParameters(parameterValues).build();
        Job queryJob;
        try {
            JobId jobId = JobId.of(UUID.randomUUID().toString());
            queryJob = bigQueryClient.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
        }
        catch (BigQueryException bqe) {
            if (bqe.getMessage().contains("Already Exists: Job")) {
                logger.info("Caught exception that this job is already running. ");
                //Return silently because another lambda is already processing this.
                //Ideally when this happens, we would want to get the existing queryJob.
                //This would allow this Lambda to timeout while waiting for the query.
                //and rejoin it. This would provide much more time for Lambda to wait for
                //BigQuery to finish its query for up to 15 mins * the number of retries.
                //However, Presto is creating multiple splits, even if we return a single split.
                return;
            }
            throw bqe;
        }

        TableResult result = null;
        try {
            while (true) {
                if (queryJob.isDone()) {
                    Thread.sleep(1000);
                    result = invoker.invoke(() ->
                            queryJob.getQueryResults());
                    break;
                }
                else if (!queryStatusChecker.isQueryRunning()) {
                    queryJob.cancel();
                }
                else {
                    Thread.sleep(1000);
                }
            }
        }
        catch (InterruptedException ie) {
            logger.info("Got interrupted waiting for Big Query to finish the query.");
            Thread.currentThread().interrupt();
        }
        outputResultsView(spiller, recordsRequest, result);
    }

    private void getTableData(BlockSpiller spiller, ReadRecordsRequest recordsRequest, List<QueryParameterValue> parameterValues, String projectName, String datasetName, String tableName) throws IOException
    {
        try (BigQueryReadClient client = BigQueryReadClient.create()) {
            String parent = String.format("projects/%s", projectName);

            String srcTable =
                    String.format(
                            "projects/%s/datasets/%s/tables/%s",
                            projectName, datasetName, tableName);

            List<String> fields = new ArrayList<>();
            for (Field field : recordsRequest.getSchema().getFields()) {
                fields.add(field.getName());
            }
            // We specify the columns to be projected by adding them to the selected fields,
            // and set a simple filter to restrict which rows are transmitted.
            ReadSession.TableReadOptions.Builder optionsBuilder =
                    ReadSession.TableReadOptions.newBuilder()
                            .addAllSelectedFields(fields);
            ReadSession.TableReadOptions options = BigQueryStorageApiUtils.setConstraints(optionsBuilder, recordsRequest.getSchema(), recordsRequest.getConstraints()).build();

            // Start specifying the read session we want created.
            ReadSession.Builder sessionBuilder =
                    ReadSession.newBuilder()
                            .setTable(srcTable)
                            // This API can also deliver data serialized in Apache Avro format.
                            // This example leverages Apache Arrow.
                            .setDataFormat(DataFormat.ARROW)
                            .setReadOptions(options);

            // Begin building the session creation request.
            CreateReadSessionRequest.Builder builder =
                    CreateReadSessionRequest.newBuilder()
                            .setParent(parent)
                            .setReadSession(sessionBuilder)
                            .setMaxStreamCount(1);

            ReadSession session = client.createReadSession(builder.build());
            // Setup a simple reader and start a read session.
            try (BigQueryRowReader reader = new BigQueryRowReader(session.getArrowSchema(), allocator)) {
                // Assert that there are streams available in the session.  An empty table may not have
                // data available.  If no sessions are available for an anonymous (cached) table, consider
                // writing results of a query to a named table rather than consuming cached results
                // directly.
                Preconditions.checkState(session.getStreamsCount() > 0);

                // Use the first stream to perform reading.
                String streamName = session.getStreams(0).getName();

                ReadRowsRequest readRowsRequest =
                        ReadRowsRequest.newBuilder().setReadStream(streamName).build();

                // Process each block of rows as they arrive and decode using our simple row reader.
                ServerStream<ReadRowsResponse> stream = client.readRowsCallable().call(readRowsRequest);
                for (ReadRowsResponse response : stream) {
                    Preconditions.checkState(response.hasArrowRecordBatch());
                    VectorSchemaRoot root = reader.processRows(response.getArrowRecordBatch());
                    long rowLimit = (recordsRequest.getConstraints().getLimit() > 0 && recordsRequest.getConstraints().getLimit() < root.getRowCount()) ? recordsRequest.getConstraints().getLimit() : root.getRowCount();
                    for (int rowIndex = 0; rowIndex < rowLimit; rowIndex++) {
                        outputResults(spiller, recordsRequest, root, rowIndex);
                    }
                }
            }
        }
    }

    /**
     * Iterates through all the results that comes back from BigQuery and saves the result to be read by the Athena Connector.
     *
     * @param spiller        The {@link BlockSpiller} provided when readWithConstraints() is called.
     * @param recordsRequest The {@link ReadRecordsRequest} provided when readWithConstraints() is called.
     * @param result         The {@link TableResult} provided by {@link BigQuery} client after a query has completed executing.
     */
    private void outputResults(BlockSpiller spiller, ReadRecordsRequest recordsRequest, VectorSchemaRoot result, int rowIndex)
    {
        if (result != null) {
            spiller.writeRows((Block block, int rowNum) -> {
                for (FieldVector vector : result.getFieldVectors()) {
                    boolean isMatched = true;
                    Object value = vector.getObject(rowIndex);
                    switch (vector.getMinorType()) {
                        case LIST:
                        case STRUCT:
                            isMatched &= block.offerComplexValue(vector.getField().getName(), rowNum, FieldResolver.DEFAULT, value);
                            break;
                        default:
                            isMatched &= block.offerValue(vector.getField().getName(), rowNum, BigQueryUtils.coerce(vector, value));
                            break;
                    }
                    if (!isMatched) {
                        return 0;
                    }
                }
                return 1;
            });
        }
    }

    /**
     * Iterates through all the results that comes back from BigQuery and saves the result to be read by the Athena Connector.
     *
     * @param spiller        The {@link BlockSpiller} provided when readWithConstraints() is called.
     * @param recordsRequest The {@link ReadRecordsRequest} provided when readWithConstraints() is called.
     * @param result         The {@link TableResult} provided by {@link BigQuery} client after a query has completed executing.
     */
    private void outputResultsView(BlockSpiller spiller, ReadRecordsRequest recordsRequest, TableResult result)
    {
        logger.info("Inside outputResults: ");
        String timeStampColsList = Objects.toString(recordsRequest.getSchema().getCustomMetadata().get("timeStampCols"), "");
        logger.info("timeStampColsList: " + timeStampColsList);
        if (result != null) {
            for (FieldValueList row : result.iterateAll()) {
                spiller.writeRows((Block block, int rowNum) -> {
                    boolean isMatched = true;
                    for (Field field : recordsRequest.getSchema().getFields()) {
                        FieldValue fieldValue = row.get(field.getName());
                        Object val;
                        switch (getMinorTypeForArrowType(field.getFieldType().getType())) {
                            case LIST:
                            case STRUCT:
                                val = BigQueryUtils.getComplexObjectFromFieldValue(field, fieldValue, timeStampColsList.contains(field.getName()));
                                isMatched &= block.offerComplexValue(field.getName(), rowNum, FieldResolver.DEFAULT, val);
                                break;
                            default:
                                val = getObjectFromFieldValue(field.getName(), fieldValue,
                                        field, timeStampColsList.contains(field.getName()));
                                isMatched &= block.offerValue(field.getName(), rowNum, val);
                                break;
                        }
                        if (!isMatched) {
                            return 0;
                        }
                    }
                    return 1;
                });
            }
        }
    }
}
