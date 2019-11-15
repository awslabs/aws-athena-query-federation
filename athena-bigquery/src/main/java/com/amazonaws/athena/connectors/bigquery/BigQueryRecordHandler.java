/*-
 * #%L
 * athena-bigquery
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

package com.amazonaws.athena.connectors.bigquery;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.TableResult;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.amazonaws.athena.connectors.bigquery.BigQueryUtils.fixCaseForDatasetName;
import static com.amazonaws.athena.connectors.bigquery.BigQueryUtils.fixCaseForTableName;
import static com.amazonaws.athena.connectors.bigquery.BigQueryUtils.getObjectFromFieldValue;

/**
 * This record handler is an example of how you can implement a lambda that calls bigquery and pulls data.
 * This Lambda requires that your BigQuery table is small enough so that a table scan can be completed
 * within 5-10 mins or this lambda will time out and it will fail.
 */
public class BigQueryRecordHandler
    extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(BigQueryRecordHandler.class);

    /**
     * The {@link BigQuery} client to interact with the BigQuery Service.
     */
    private final BigQuery bigQueryClient;

    BigQueryRecordHandler()
        throws IOException
    {
        this(AmazonS3ClientBuilder.defaultClient(),
            AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient(),
            BigQueryUtils.getBigQueryClient()
        );
    }

    @VisibleForTesting
    BigQueryRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena athena, BigQuery bigQueryClient)
    {
        super(amazonS3, secretsManager, athena, BigQueryConstants.SOURCE_TYPE);
        this.bigQueryClient = bigQueryClient;
    }

    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
        throws Exception
    {
        final String projectName = BigQueryUtils.getProjectName(recordsRequest.getCatalogName());
        final String datasetName = fixCaseForDatasetName(projectName, recordsRequest.getTableName().getSchemaName(), bigQueryClient);
        final String tableName = fixCaseForTableName(projectName, datasetName, recordsRequest.getTableName().getTableName(),
            bigQueryClient);

        logger.info("Got Request with constraints: {}", recordsRequest.getConstraints());

        List<QueryParameterValue> parameterValues = new ArrayList<>();
        final String sqlToExecute = BigQuerySqlUtils.buildSqlFromSplit(new TableName(datasetName, tableName),
            recordsRequest.getSchema(), recordsRequest.getConstraints(), recordsRequest.getSplit(), parameterValues);

        QueryJobConfiguration queryConfig =
            QueryJobConfiguration.newBuilder(sqlToExecute)
                // Use standard SQL syntax for queries.
                // See: https://cloud.google.com/bigquery/sql-reference/
                .setUseLegacySql(false)
                .setPositionalParameters(parameterValues)
                .build();

        logger.info("Executing SQL Query: {} for Split: {}", sqlToExecute, recordsRequest.getSplit());

        Job queryJob;
        try {
            JobId jobId = JobId.of(fixRequestId(recordsRequest.getQueryId()));
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

        TableResult result;
        try {
            while (true) {
                if (queryJob.isDone()) {
                    result = queryJob.getQueryResults();
                    break;
                }
                else if (!queryStatusChecker.isQueryRunning()) {
                    queryJob.cancel();
                }
                else {
                    Thread.sleep(10);
                }
            }
        }
        catch (InterruptedException ie) {
            throw new IllegalStateException("Got interrupted waiting for Big Query to finish the query.");
        }

        outputResults(spiller, recordsRequest, result);
    }

    private String fixRequestId(String queryId)
    {
        return queryId.replaceAll("[^a-zA-Z0-9-_]", "");
    }

    /**
     * Iterates through all the results that comes back from BigQuery and saves the result to be read by the Athena Connector.
     *
     * @param spiller The {@link BlockSpiller} provided when readWithConstraints() is called.
     * @param recordsRequest The {@link ReadRecordsRequest} provided when readWithConstraints() is called.
     * @param result The {@link TableResult} provided by {@link BigQuery} client after a query has completed executing.
     */
    private void outputResults(BlockSpiller spiller, ReadRecordsRequest recordsRequest, TableResult result)
    {
        for (FieldValueList row : result.iterateAll()) {
            spiller.writeRows((Block block, int rowNum) -> {
                boolean isMatched = true;
                for (Field field : recordsRequest.getSchema().getFields()) {
                    FieldValue fieldValue = row.get(field.getName());
                    Object val = getObjectFromFieldValue(field.getName(), fieldValue,
                        field.getFieldType().getType());
                    isMatched &= block.offerValue(field.getName(), rowNum, val);
                    if (!isMatched) {
                        return 0;
                    }
                }
                return 1;
            });
        }
    }
}
