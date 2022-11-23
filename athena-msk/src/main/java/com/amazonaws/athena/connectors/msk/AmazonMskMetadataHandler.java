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
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connectors.msk.dto.SplitParameters;
import com.amazonaws.athena.connectors.msk.dto.TopicPartitionPiece;
import com.amazonaws.athena.connectors.msk.dto.TopicSchema;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.msk.AmazonMskConstants.MAX_RECORDS_IN_SPLIT;

public class AmazonMskMetadataHandler extends MetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AmazonMskMetadataHandler.class);
    private final Consumer<String, String> kafkaConsumer;

    public AmazonMskMetadataHandler() throws Exception
    {
        this(AmazonMskUtils.getKafkaConsumer());
    }

    @VisibleForTesting
    public AmazonMskMetadataHandler(Consumer<String, String> kafkaConsumer)
    {
        super(AmazonMskConstants.KAFKA_SOURCE);
        this.kafkaConsumer = kafkaConsumer;
    }

    /**
     * It will list the schema name which is set to default.
     *
     * @param blockAllocator - instance of {@link BlockAllocator}
     * @param listSchemasRequest - instance of {@link ListSchemasRequest}
     * @return {@link ListSchemasResponse}
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest)
    {
        LOGGER.info("doListSchemaNames called with Catalog: {}", listSchemasRequest.getCatalogName());
        final List<String> schemas = new ArrayList<>();
        // for now, there is only one default schema
        schemas.add(AmazonMskConstants.KAFKA_SCHEMA);
        LOGGER.info("Found {} schemas!", schemas.size());
        return new ListSchemasResponse(listSchemasRequest.getCatalogName(), schemas);
    }

    /**
     * List all the tables. It pulls all the topic names from Glue registry.
     *
     * @param blockAllocator - instance of {@link BlockAllocator}
     * @param listTablesRequest - instance of {@link ListTablesRequest}
     * @return {@link ListTablesResponse}
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest)
    {
        LOGGER.info("{}: List table names for Catalog {}, Schema {}", listTablesRequest.getQueryId(), listTablesRequest.getCatalogName(), listTablesRequest.getSchemaName());
        List<String> topicList = AmazonMskUtils.getTopicListFromGlueRegistry();
        List<TableName> tableNames  = topicList.stream().map(topic -> new TableName(listTablesRequest.getSchemaName(), topic)).collect(Collectors.toList());
        return new ListTablesResponse(listTablesRequest.getCatalogName(), tableNames, null);
    }

    /**
     * Creates new object of GetTableResponse. It pulls topic schema from Glue
     * registry and converts into arrow schema.
     *
     * @param blockAllocator - instance of {@link BlockAllocator}
     * @param getTableRequest - instance of {@link GetTableRequest}
     * @return {@link GetTableResponse}
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest) throws Exception
    {
        Schema tableSchema = getSchema(getTableRequest.getTableName().getTableName());
        TableName tableName = new TableName(getTableRequest.getTableName().getSchemaName(), tableSchema.getCustomMetadata().get("topicName"));
        return new GetTableResponse(getTableRequest.getCatalogName(), tableName, tableSchema);
    }

    /**
     * Since the kafka partition is not a part of the topic schema as well as
     * not part of topic message data, we should not implement this method.
     *
     * There is no physical schema field that had been used to
     * create the topic partitions, therefor we can not add any partition information
     * in GetTableResponse (in the previous lifecycle method doGetTable). As there is no
     * partition information in the topic schema getPartitions method will not be invoked.
     *
     * NOTE that even if we add some fields for the topic partitions, those fields
     * must be added in the table schema, and it will impact on spiller for
     * writing meaningless data for partition column. In fact, for each record we
     * will be receiving from kafka topic, there will be no such column while schema
     * will contain additional field for partition.
     *
     * @param blockWriter - instance of {@link BlockWriter}
     * @param request - instance of {@link GetTableLayoutRequest}
     * @param queryStatusChecker - instance of {@link QueryStatusChecker}
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
    {
    }

    /**
     * Creating splits for each partition. We are further dividing each topic partition into more pieces
     * to increase the number of Split to result more parallelism.
     *
     * In the split metadata we are keeping the topic name and partition key
     * as well as the start and end offset indexes for each divided partition parts.
     * This information will be used in RecordHandler to initiate kafka consumer.
     *
     * @param allocator - instance of {@link BlockAllocator}
     * @param request - instance of {@link GetSplitsRequest}
     * @return {@link GetSplitsResponse}
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        LOGGER.info("{}: Catalog {}, table {}", request.getQueryId(), request.getTableName().getSchemaName(), request.getTableName().getTableName());
        String topic = request.getTableName().getTableName();

        // Get the available partitions of the topic from kafka server.
        Collection<TopicPartition> topicPartitions = kafkaConsumer.partitionsFor(topic)
                .stream()
                .map(it -> new TopicPartition(it.topic(), it.partition()))
                .collect(Collectors.toList());

        LOGGER.debug("[kafka][split] total partitions {} found for topic: {}", topicPartitions.size(), topic);

        // Get end offset of each topic partitions from kafka server.
        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(topicPartitions);
        if (LOGGER.isDebugEnabled()) {
            endOffsets.forEach((k, v) -> {
                LOGGER.debug("[kafka][split] offset info [topic: {}, partition: {}, end-offset: {}]",
                        k.topic(), k.partition(), v
                );
            });
        }

        Set<Split> splits = new HashSet<>();
        SpillLocation spillLocation = makeSpillLocation(request);
        for (TopicPartition partition : topicPartitions) {
            // Calculate how many pieces we can divide a topic partition.
            List<TopicPartitionPiece>  topicPartitionPieces = pieceTopicPartition(endOffsets.get(partition));
            LOGGER.info("[kafka] Total pieces created {} for partition {} in topic {}",
                    topicPartitionPieces.size(), partition.partition(), partition.topic()
            );

            // And for each piece we will create new split
            for (TopicPartitionPiece topicPartitionPiece : topicPartitionPieces) {
                // In split, we are putting parameters so that later, in RecordHandler we know
                // for which topic and for which partition we will initiate a kafka consumer
                // as well as to consume data from which start offset to which end offset.
                Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                        .add(SplitParameters.TOPIC, partition.topic())
                        .add(SplitParameters.PARTITION, Integer.toString(partition.partition()))
                        .add(SplitParameters.START_OFFSET, Long.toString(topicPartitionPiece.startOffset))
                        .add(SplitParameters.END_OFFSET, Long.toString(topicPartitionPiece.endOffset));
                splits.add(splitBuilder.build());
            }
        }

        LOGGER.debug("[kafka] Total split created {} ", splits.size());
        return new GetSplitsResponse(request.getCatalogName(), splits);
    }

    /**
     * Create the arrow schema for a specific topic. In the metadata
     * we keep the additional information of topic schema and fields.
     *
     * @param tableName - name of the kafka topic as table name
     * @return {@link Schema}
     * @throws Exception - {@link Exception}
     */
    private Schema getSchema(String tableName) throws Exception
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

        // Get topic schema json from GLue registry as translated to TopicSchema pojo
        TopicSchema topicSchema = AmazonMskUtils.getTopicSchemaFromGlueRegistry(tableName);

        // Creating ArrowType for each fields in the topic schema.
        // Also putting the additional column level information
        // into the metadata in ArrowType field.
        topicSchema.getMessage().getFields().forEach(it -> {
            FieldType fieldType = new FieldType(
                    true,
                    AmazonMskUtils.toArrowType(it.getType()),
                    null,
                    Map.of(
                            "mapping", it.getMapping(),
                            "formatHint", it.getFormatHint(),
                            "type", it.getType()
                    )
            );
            Field field = new Field(it.getName(), fieldType, null);
            schemaBuilder.addField(field);
        });

        // Putting the additional schema level information into the metadata in ArrowType schema.
        schemaBuilder.addMetadata("dataFormat", topicSchema.getMessage().getDataFormat());
        schemaBuilder.addMetadata("topicName", topicSchema.getTopicName());

        return schemaBuilder.build();
    }

    /**
     * Splits topic partition into smaller piece and calculates
     * the start and end offsets of each piece.
     *
     * @param endOffset - the last offset of topic partition
     * @return {@link List<TopicPartitionPiece>}
     */
    public  List<TopicPartitionPiece> pieceTopicPartition(long endOffset)
    {
        List<TopicPartitionPiece> topicPartitionPieces = new ArrayList<>();

        // If endOffset + 1 is smaller or equal to MAX_RECORDS_IN_SPLIT then we do not
        // need to piece the topic partition.
        if (endOffset + 1 <= MAX_RECORDS_IN_SPLIT) {
            topicPartitionPieces.add(new TopicPartitionPiece(0, endOffset));
            return topicPartitionPieces;
        }

        // We need to piece the partition basing its end offset.
        // Calculate the number of pieces can be created for the topic partition.
        int pieces = (int) Math.ceil((float) endOffset / (float) MAX_RECORDS_IN_SPLIT);

        // Set the start and end offset for the first piece
        long xOffset = 0;
        long yOffset = MAX_RECORDS_IN_SPLIT;

        // Now we will traverse loop for the calculated pieces and
        // keep recalculating the start and end offsets for each piece
        // until we reach to the end of loop.
        for (int i = 0; i < pieces; i++) {
            topicPartitionPieces.add(new TopicPartitionPiece(xOffset, yOffset));
            xOffset = yOffset + 1;
            yOffset += MAX_RECORDS_IN_SPLIT;

            // The last endOffset of the last piece must not be greater than the endOffset
            // of topic partition, it will be equal to endOffset of the topic partition.
            if (endOffset < yOffset) {
                yOffset = endOffset;
            }
        }

        return topicPartitionPieces;
    }
}
