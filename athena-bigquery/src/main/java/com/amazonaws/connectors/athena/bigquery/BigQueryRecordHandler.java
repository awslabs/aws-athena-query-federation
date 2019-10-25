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
package com.amazonaws.connectors.athena.bigquery;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
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
import com.google.cloud.bigquery.TableResult;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.StringJoiner;

import static com.amazonaws.connectors.athena.bigquery.BigQueryUtils.fixCaseForDatasetName;
import static com.amazonaws.connectors.athena.bigquery.BigQueryUtils.fixCaseForTableName;
import static com.amazonaws.connectors.athena.bigquery.BigQueryUtils.getObjectFromFieldValue;

/**
 * This record handler is an example of how you can implement a lambda that calls bigquery and pulls data.
 * This Lambda requires that your BigQuery table is small enough so that a table scan can be completed
 * within 5-10 mins or this lambda will time out and it will fail.
 */
public class BigQueryRecordHandler
        extends RecordHandler
{
    public static final String PROJECT_NAME = "BQ_PROJECT_NAME";
    private static final Logger logger = LoggerFactory.getLogger(BigQueryRecordHandler.class);
    private static final String sourceType = "bigquery";
    private final BigQuery bigQueryClient;

    public BigQueryRecordHandler()
            throws IOException
    {
        this(AmazonS3ClientBuilder.defaultClient(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                BigQueryUtils.getBigQueryClient()
        );
    }

    @VisibleForTesting
    BigQueryRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, BigQuery bigQueryClient)
    {
        super(amazonS3, secretsManager, sourceType);
        this.bigQueryClient = bigQueryClient;
    }

    private String getProjectName(ReadRecordsRequest request)
    {
        if (System.getenv(PROJECT_NAME) != null) {
            return System.getenv(PROJECT_NAME);
        }
        return request.getCatalogName();
    }

    @Override
    protected void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller spiller, ReadRecordsRequest recordsRequest)
    {
        String datasetName = fixCaseForDatasetName(getProjectName(recordsRequest), recordsRequest.getTableName().getSchemaName(), bigQueryClient);
        String tableName = fixCaseForTableName(getProjectName(recordsRequest), datasetName, recordsRequest.getTableName().getTableName(),
                bigQueryClient);

        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                        buildSql(new TableName(datasetName, tableName),
                                recordsRequest.getSchema(), recordsRequest.getConstraints(), recordsRequest.getSplit()))
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
                        .build();

        logger.info("ReadWithConstraint: {}", recordsRequest.getConstraints().getSummary().toString());

        //TODO:: Incorporate a split ID to the jobid so that this wont kick off multiple bigquery queries.
        JobId jobId = JobId.of(recordsRequest.getQueryId());
        Job queryJob;

        try {
            queryJob = bigQueryClient.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
        }
        catch (BigQueryException bqe) {
            if (bqe.getMessage().contains("Already Exists: Job")) {
                logger.info("Caught exception that this job is already running. ");
                //Return silently because another lambda is already processing this.
                //Ideally when this happens, we would want to get the get the existing queryJob.
                //This would allow this Lambda to timeout while waiting for the query.
                //and rejoin it. This would provide much more time for Lambda to wait for
                //BigQuery to finish its query for up to 15 mins *
                //But for some reason, Presto is creating at least 2 splits, even if
                //we create a single split.
                return;
            }
            throw bqe;
        }

        TableResult result;
        try {
            queryJob = queryJob.waitFor();
            result = queryJob.getQueryResults();
        }
        catch (InterruptedException ie) {
            throw new IllegalStateException("Got interrupted waiting for Big Query to finish the query.");
        }

        for (FieldValueList row : result.iterateAll()) {
            spiller.writeRows((Block block, int rowNum) -> {
                boolean matched = true;
                for (Field field : recordsRequest.getSchema().getFields()) {
                    if (!matched) {
                        break;
                    }
                    FieldValue fieldValue = row.get(field.getName());
                    Object val = getObjectFromFieldValue(field.getName(), fieldValue,
                            field.getFieldType().getType().getTypeID());
                    matched &= constraintEvaluator.apply(field.getName(), val);
                }

                if (matched) {
                    for (Field field : recordsRequest.getSchema().getFields()) {
                        FieldValue fieldValue = row.get(field.getName());
                        Object val = getObjectFromFieldValue(field.getName(), fieldValue,
                                field.getFieldType().getType().getTypeID());
                        if (val == null) {
                            continue;
                        }
                        BlockUtils.setValue(block.getFieldVector(field.getName()), rowNum, val);
                    }
                    return 1;
                }
                return 0;
            });
        }
    }

    @VisibleForTesting
    String buildSql(TableName tableName, Schema schema, Constraints constraints, Split split)
    {
        StringBuilder builder = new StringBuilder("SELECT ");

        StringJoiner sj = new StringJoiner(",");
        if (schema.getFields().isEmpty()) {
            sj.add("*");
        }
        else {
            for (Field field : schema.getFields()) {
                sj.add(field.getName());
            }
        }
        builder.append(sj.toString())
                .append(" from ")
                .append(tableName.getSchemaName())
                .append(".")
                .append(tableName.getTableName());

        logger.info("Executing query: '{}'", builder.toString());

        //TODO::Add "WHERE" clause for the constraints and for the Split Partition Value.
        sj = new StringJoiner(") AND (");
        for (Map.Entry<String, ValueSet> summary : constraints.getSummary().entrySet()) {
            final ValueSet value = summary.getValue();
            final String columnName = summary.getKey();
            if (value.isSingleValue()) {
                //Check Arrow type to see if we
                sj.add(columnName + " = " + getValueForWhereClause(columnName, value.getSingleValue(), value.getType()) + "");
            }
            //TODO:: Finish the rest of converting the range to SQL. Below is not correct.
            // else if (value.isNone() || value.isAll()) {
            //                if (value.getRanges().getRangeCount() == 0) {
            //                    sj.add(columnName + (value.isNone() ? "NOT " : "") + " IN (" + value + ")");
            //                } else {
            //                    //Generate SQL based on Range.
            //                    for (Range range : value.getRanges().getOrderedRanges()) {
            //                        Marker lowValue = range.getLow();
            //                        Marker highValue = range.getHigh();
            //                        if (range.isSingleValue()) {
            //                            sj.add(columnName + " = " + getValueForWhereClause(columnName, lowValue.getValue(), lowValue.getType()));
            //                        } else {
            //                            if (!lowValue.isNullValue()) {
            //                                sj.add(columnName
            //                                    + (lowValue.getBound() == Marker.Bound.EXACTLY ? "<=" : "<")
            //                                    + getValueForWhereClause(columnName, highValue.getValue(), highValue.getType()));
            //                            }
            //                            if (!highValue.isNullValue()) {
            //                                sj.add(columnName
            //                                    + (highValue.getBound() == Marker.Bound.EXACTLY ? ">=" : ">")
            //                                    + getValueForWhereClause(columnName, highValue.getValue(), highValue.getType()));
            //                            }
            //                        }
            //                    }
            //                }
            //            }
        }
        if (!constraints.getSummary().entrySet().isEmpty()) {
            builder.append(" WHERE (")
                    .append(sj.toString())
                    .append(")");
        }

        return builder.toString();
    }

    //Gets the representation of a value that can be used in a where clause, ie String values need to be quoted, numeric doesn't.
    private String getValueForWhereClause(String columnName, Object value, ArrowType arrowType)
    {
        switch (arrowType.getTypeID()) {
            case Int:
            case Decimal:
            case FloatingPoint:
                return value.toString();
            case Bool:
                if ((Boolean) value) {
                    return "true";
                }
                else {
                    return "false";
                }
            case Utf8:
                return "'" + value.toString() + "'";
            default:
                throw new IllegalArgumentException("Unknown type has been encountered during range processing: " + columnName +
                        " Field Type: " + arrowType.getTypeID().name());
        }
    }
}
