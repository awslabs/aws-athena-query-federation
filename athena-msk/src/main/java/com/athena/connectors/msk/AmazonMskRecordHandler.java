/*-
 * #%L
 * athena-msk
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.athena.connectors.msk;

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
import com.athena.connectors.msk.trino.QueryExecutor;
import com.athena.connectors.msk.trino.TrinoRecord;
import com.athena.connectors.msk.trino.TrinoRecordSet;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This record handler is an example of how you can implement a lambda that calls AmazonMSK and pulls data.
 * This Lambda requires that your Kafka topic is small enough so that a table scan can be completed
 * within 5-10 mins or this lambda will time out and it will fail.
 */
public class AmazonMskRecordHandler
        extends RecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AmazonMskRecordHandler.class);

    private final QueryExecutor queryExecutor;

    AmazonMskRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient(),
                AmazonMskUtils.getQueryExecutor()
        );
        LOGGER.debug("  AmazonMskRecordHandler constructor() ");
    }

    @VisibleForTesting
    public AmazonMskRecordHandler(AmazonS3 amazonS3,
                                      AWSSecretsManager secretsManager,
                                      AmazonAthena athena,
                                  QueryExecutor runner)
    {
        super(amazonS3, secretsManager, athena, AmazonMskConstants.KAFKA_SOURCE);
        this.queryExecutor = runner;
        LOGGER.debug(" STEP 2.0  AmazonMskRecordHandler constructor() ");
    }

    /**
     * generates the sql to executes on basis of where condition and executes it.
     *
     * @param spiller            The {@link BlockSpiller} provided when readWithConstraints() is called.
     * @param recordsRequest     The {@link ReadRecordsRequest} provided when readWithConstraints() is called.
     * @param queryStatusChecker
     */
    @Override
    public void readWithConstraint(BlockSpiller spiller,
                                   ReadRecordsRequest recordsRequest,
                                   QueryStatusChecker queryStatusChecker)
    {
        LOGGER.debug(" Inside  readWithConstraint");

        ObjectMapper mapper = new ObjectMapper();
        try {
            System.out.println(mapper.writeValueAsString(recordsRequest));
        }
        catch (JsonProcessingException e) {
            // ignored
        }

        List<String> parameterValues = new ArrayList<>();
        String sqlToExecute = "";
        try {
            final String schemaName = recordsRequest.getTableName().getSchemaName();
            final String tableName =  recordsRequest.getTableName().getTableName();

            LOGGER.debug(" recordset getSchema() : " + recordsRequest.getSchema().toString());
            LOGGER.debug(" recordset getConstraints() : " + recordsRequest.getConstraints().toString());
            LOGGER.debug(" recordset getSplit() : " + recordsRequest.getSplit().toString());
            sqlToExecute = AmazonMskSqlUtils.buildSqlFromSplit(
                    new TableName(schemaName, tableName),
                    recordsRequest.getSchema(),
                    recordsRequest.getConstraints(),
                    recordsRequest.getSplit(),
                    parameterValues);

            LOGGER.info("SQL to execute" + sqlToExecute);

            LOGGER.debug("STEP 2.1.4: " + parameterValues.toString());
        }
        catch (RuntimeException e) {
            LOGGER.error("Error: ", e);
            e.printStackTrace();
        }
        TrinoRecordSet result = queryExecutor.execute(sqlToExecute);
        outputResults(spiller, recordsRequest, result);
    }
    /**
     * Iterates through all the results that comes back from Kafka and saves the result to be read by the Athena Connector.
     *
     * @param spiller        The {@link BlockSpiller} provided when readWithConstraints() is called.
     * @param recordsRequest The {@link ReadRecordsRequest} provided when readWithConstraints() is called.
     * @param result         The {@link //TableResult} provided by {@link //BigQuery} client after a query has completed executing.
     */
    private void outputResults(BlockSpiller spiller, ReadRecordsRequest recordsRequest,
                               TrinoRecordSet result)
    {
        LOGGER.debug(" STEP 2.2  outputResults");

        List<Field> list = recordsRequest.getSchema().getFields();

        LOGGER.debug(" STEP 2.2.0  schema() " + list.toString());

        for (TrinoRecord record : result) {
            spiller.writeRows((Block block, int rowNum) -> {
                boolean isMatched = true;
                int i = 0;
                for (int j = 0; j < record.getFieldCount(); j++) {
                    Field field = null;
                    Object val = record.getField(j) == null ? "" : record.getField(j);

                    if (i < list.size()) {
                        field = list.get(i);
                    }
                    ++i;
                    if (i == list.size()) {
                        i = 0;
                    }

                    isMatched &= block.offerValue(field.getName(), rowNum, getValue(val, field.getType()));
                    if (!isMatched) {
                        return 0;
                    }
                }
                return 1;
            });
        }
    }

    /**
     * converts value ArrowType into sql type
     *
     * @param value
     * @param arrowType
     * @return converted SQL Type value
     */
    private Object getValue(Object value, ArrowType arrowType)
    {
        switch (arrowType.getTypeID()) {
            case Null:
                LOGGER.debug(" Inside AmazonMSKRecordHandler Null: " + value);
                return null;
            case Bool:
                LOGGER.debug(" Inside AmazonMSKRecordHandler Bool: " + value);
                return Boolean.valueOf(value.toString().toLowerCase());
            case Timestamp:
            case Date:
                LOGGER.debug(" Inside AmazonMSKRecordHandler Date: " + value);
                return ArrowTypeConverter.convertToDate(String.valueOf(value));
            case Utf8:
            case NONE:
            default:
                LOGGER.debug(" Inside AmazonMSKRecordHandler Default: " + value);
                return value;
        }
    }
}
