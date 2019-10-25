/*-
 * #%L
 * athena-cloudwatch
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
package com.amazonaws.athena.connectors.cloudwatch;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.OutputLogEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Handles data read record requests for the Athena Cloudwatch Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Reads and maps Cloudwatch Logs data for a specific LogStream (split)
 * 2. Attempts to push down time range predicates into Cloudwatch.
 */
public class CloudwatchRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(CloudwatchRecordHandler.class);
    private static final String sourceType = "cloudwatch";

    private final AmazonS3 amazonS3;
    private final AWSLogs awsLogs;

    public CloudwatchRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                AWSLogsClientBuilder.defaultClient());
    }

    @VisibleForTesting
    protected CloudwatchRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AWSLogs awsLogs)
    {
        super(amazonS3, secretsManager, sourceType);
        this.amazonS3 = amazonS3;
        this.awsLogs = awsLogs;
    }

    /**
     * Scans Cloudwatch Logs using the LogStream and optional Time stamp filters.
     *
     * @see RecordHandler
     */
    @Override
    protected void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller spiller, ReadRecordsRequest recordsRequest)
    {
        String continuationToken = null;
        TableName tableName = recordsRequest.getTableName();
        Split split = recordsRequest.getSplit();

        do {
            GetLogEventsResult logEventsResult = awsLogs.getLogEvents(
                    pushDownConstraints(recordsRequest.getConstraints(),
                            new GetLogEventsRequest()
                                    .withLogGroupName(tableName.getSchemaName())
                                    //We use the property instead of the table name because of the special all_streams table
                                    .withLogStreamName(split.getProperty(CloudwatchMetadataHandler.LOG_STREAM_FIELD))
                                    .withNextToken(continuationToken)
                    ));

            if (continuationToken == null || !continuationToken.equals(logEventsResult.getNextForwardToken())) {
                continuationToken = logEventsResult.getNextForwardToken();
            }
            else {
                continuationToken = null;
            }

            Set<String> requiredFields = new HashSet<>();
            recordsRequest.getSchema().getFields().stream().forEach(next -> requiredFields.add(next.getName()));

            for (OutputLogEvent ole : logEventsResult.getEvents()) {
                spiller.writeRows((Block block, int rowNum) -> {
                    //perform predicate pushdown on supported fields
                    boolean matched = constraintEvaluator.apply(CloudwatchMetadataHandler.LOG_STREAM_FIELD, split.getProperty(CloudwatchMetadataHandler.LOG_STREAM_FIELD))
                            && constraintEvaluator.apply(CloudwatchMetadataHandler.LOG_TIME_FIELD, ole.getTimestamp())
                            && constraintEvaluator.apply(CloudwatchMetadataHandler.LOG_MSG_FIELD, ole.getMessage());

                    if (matched) {
                        if (requiredFields.contains(CloudwatchMetadataHandler.LOG_STREAM_FIELD)) {
                            BlockUtils.setValue(block.getFieldVector(CloudwatchMetadataHandler.LOG_STREAM_FIELD),
                                    rowNum,
                                    split.getProperty(CloudwatchMetadataHandler.LOG_STREAM_FIELD));
                        }

                        if (requiredFields.contains(CloudwatchMetadataHandler.LOG_TIME_FIELD)) {
                            BlockUtils.setValue(block.getFieldVector(CloudwatchMetadataHandler.LOG_TIME_FIELD), rowNum, ole.getTimestamp());
                        }

                        if (requiredFields.contains(CloudwatchMetadataHandler.LOG_MSG_FIELD)) {
                            BlockUtils.setValue(block.getFieldVector(CloudwatchMetadataHandler.LOG_MSG_FIELD), rowNum, ole.getMessage());
                        }
                    }

                    return matched ? 1 : 0;
                });
            }

            logger.info("readWithConstraint: LogGroup[{}] LogStream[{}] Continuation[{}] rows[{}]",
                    new Object[] {tableName.getSchemaName(), tableName.getTableName(), continuationToken,
                            logEventsResult.getEvents().size()});
        }
        while (continuationToken != null);
    }

    /**
     * Attempts to push down predicates into Cloudwatch Logs by decorating the Cloudwatch Logs request.
     *
     * @param constraints The constraints for the read as provided by Athena based on the customer's query.
     * @param request The Cloudwatch Logs request to inject predicates to.
     * @return The decorated Cloudwatch Logs request.
     * @note This impl currently only pushing down SortedRangeSet filters (>=, =<, between) on the log time column.
     */
    private GetLogEventsRequest pushDownConstraints(Constraints constraints, GetLogEventsRequest request)
    {
        ValueSet timeConstraint = constraints.getSummary().get(CloudwatchMetadataHandler.LOG_TIME_FIELD);
        if (timeConstraint instanceof SortedRangeSet) {
            //SortedRangeSet is how >, <, between is represented which are easiest and most common when
            //searching logs so we attempt to push that down here as an optimization. SQL can represent complex
            //overlapping ranges which Cloudwatch can not support so this is not a replacement for applying
            //constraints using the ConstraintEvaluator.

            Range basicPredicate = ((SortedRangeSet) timeConstraint).getSpan();

            if (!basicPredicate.getLow().isNullValue()) {
                Long lowerBound = (Long) basicPredicate.getLow().getValue();
                request.setStartTime(lowerBound);
            }

            if (!basicPredicate.getHigh().isNullValue()) {
                Long upperBound = (Long) basicPredicate.getHigh().getValue();
                request.setEndTime(upperBound);
            }
        }

        return request;
    }
}
