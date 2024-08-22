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
package com.amazonaws.athena.connectors.msk;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.msk.consumer.MskAvroConsumer;
import com.amazonaws.athena.connectors.msk.consumer.MskConsumer;
import com.amazonaws.athena.connectors.msk.consumer.MskDefaultConsumer;
import com.amazonaws.athena.connectors.msk.consumer.MskProtobufConsumer;
import com.amazonaws.athena.connectors.msk.dto.SplitParameters;
import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import static com.amazonaws.athena.connectors.msk.AmazonMskConstants.AVRO_DATA_FORMAT;
import static com.amazonaws.athena.connectors.msk.AmazonMskConstants.PROTOBUF_DATA_FORMAT;

public class AmazonMskRecordHandler
        extends RecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AmazonMskRecordHandler.class);

    AmazonMskRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(
            S3Client.create(),
            SecretsManagerClient.create(),
            AthenaClient.create(),
            configOptions);
    }

    @VisibleForTesting
    public AmazonMskRecordHandler(S3Client amazonS3, SecretsManagerClient secretsManager, AthenaClient athena, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, AmazonMskConstants.MSK_SOURCE, configOptions);
    }

    /**
     * generates the sql to executes on basis of where condition and executes it.
     *
     * @param spiller - instance of {@link BlockSpiller}
     * @param recordsRequest - instance of {@link ReadRecordsRequest}
     * @param queryStatusChecker - instance of {@link QueryStatusChecker}
     */
    @Override
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker) throws Exception
    {
        // Taking the Split parameters in a readable pojo format.
        SplitParameters splitParameters = AmazonMskUtils.createSplitParam(recordsRequest.getSplit().getProperties());
        LOGGER.info("[kafka] {} RecordHandler running", splitParameters);
        GlueRegistryReader registryReader = new GlueRegistryReader();

        String dataFormat = registryReader.getGlueSchemaType(recordsRequest.getTableName().getSchemaName(), recordsRequest.getTableName().getTableName());
        MskConsumer mskConsumer;
        Consumer<?, ?> consumer;

        switch (dataFormat.toLowerCase()) {
            case AVRO_DATA_FORMAT:
                consumer = AmazonMskUtils.getAvroKafkaConsumer(configOptions);
                mskConsumer = new MskAvroConsumer();
                break;
            case PROTOBUF_DATA_FORMAT:
                consumer = AmazonMskUtils.getProtobufKafkaConsumer(configOptions);
                mskConsumer = new MskProtobufConsumer();
                break;
            default:
                consumer = AmazonMskUtils.getKafkaConsumer(recordsRequest.getSchema(), configOptions);
                mskConsumer = new MskDefaultConsumer();
                break;
        }

        try (Consumer<?, ?> kafkaConsumer = consumer) {
            mskConsumer.consume(spiller, recordsRequest, queryStatusChecker, splitParameters, kafkaConsumer);
        }
    }
}
