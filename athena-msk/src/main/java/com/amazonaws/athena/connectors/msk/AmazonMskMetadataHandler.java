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
import com.amazonaws.athena.connector.util.PaginatedRequestIterator;
import com.amazonaws.athena.connectors.msk.dto.SplitParameters;
import com.amazonaws.athena.connectors.msk.dto.TopicPartitionPiece;
import com.amazonaws.athena.connectors.msk.dto.TopicSchema;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.GetRegistryRequest;
import com.amazonaws.services.glue.model.GetRegistryResult;
import com.amazonaws.services.glue.model.ListRegistriesRequest;
import com.amazonaws.services.glue.model.ListRegistriesResult;
import com.amazonaws.services.glue.model.ListSchemasResult;
import com.amazonaws.services.glue.model.RegistryId;
import com.amazonaws.services.glue.model.RegistryListItem;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.msk.AmazonMskConstants.MAX_RECORDS_IN_SPLIT;

public class AmazonMskMetadataHandler extends MetadataHandler
{
    private static final int maxGluePageSize = 100;
    private static final long MAX_RESULTS = 100_000;
    static final long MAX_SPLITS_PER_REQUEST = 1000; // around 45k splits will exceed the 6mb response
    private static final String REGISTRY_MARKER = "{AthenaFederationMSK}";
    private static final Logger LOGGER = LoggerFactory.getLogger(AmazonMskMetadataHandler.class);
    private final Consumer<String, String> kafkaConsumer;

    public AmazonMskMetadataHandler(java.util.Map<String, String> configOptions) throws Exception
    {
        this(AmazonMskUtils.getKafkaConsumer(configOptions), configOptions);
    }

    @VisibleForTesting
    public AmazonMskMetadataHandler(Consumer<String, String> kafkaConsumer, java.util.Map<String, String> configOptions)
    {
        super(AmazonMskConstants.MSK_SOURCE, configOptions);
        this.kafkaConsumer = kafkaConsumer;
    }

    private Stream<String> filteredRegistriesStream(Stream<RegistryListItem> registries)
    {
        return registries
            .filter(r -> r.getDescription() != null && r.getDescription().contains(REGISTRY_MARKER))
            .map(r -> r.getRegistryName());
    }

    private ListRegistriesResult listRegistriesFromGlue(AWSGlue glue, String nextToken)
    {
        ListRegistriesRequest listRequest = new ListRegistriesRequest().withMaxResults(maxGluePageSize);
        listRequest = (nextToken == null) ? listRequest : listRequest.withNextToken(nextToken);
        return glue.listRegistries(listRequest);
    }

    private ListSchemasResult listSchemasFromGlue(AWSGlue glue, String glueRegistryName, int pageSize, String nextToken)
    {
        com.amazonaws.services.glue.model.ListSchemasRequest listRequest = new com.amazonaws.services.glue.model.ListSchemasRequest()
            .withRegistryId(new RegistryId().withRegistryName(glueRegistryName))
            .withMaxResults(Math.min(pageSize, maxGluePageSize));
        listRequest = (nextToken == null) ? listRequest : listRequest.withNextToken(nextToken);
        return glue.listSchemas(listRequest);
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
        AWSGlue glue = AWSGlueClientBuilder.defaultClient();

        Stream<String> allFilteredRegistries = PaginatedRequestIterator.stream((pageToken) -> listRegistriesFromGlue(glue, pageToken), ListRegistriesResult::getNextToken)
            .flatMap(result -> filteredRegistriesStream(result.getRegistries().stream()));
        ListSchemasResponse result = new ListSchemasResponse(listSchemasRequest.getCatalogName(), allFilteredRegistries.collect(Collectors.toList()));
        LOGGER.debug("doListSchemaNames result: {}", result);
        return result;
    }

    private String resolveGlueRegistryName(String glueRegistryName)
    {
        try {
            AWSGlue glue = AWSGlueClientBuilder.defaultClient();
            GetRegistryResult getRegistryResult = glue.getRegistry(new GetRegistryRequest().withRegistryId(new RegistryId().withRegistryName(glueRegistryName)));
            if (!(getRegistryResult.getDescription() != null && getRegistryResult.getDescription().contains(REGISTRY_MARKER))) {
                throw new Exception(String.format("Found a registry with a matching name [%s] but not marked for AthenaFederationMSK", glueRegistryName));
            }
            return getRegistryResult.getRegistryName();
        }
        catch (Exception ex) {
            LOGGER.info("resolveGlueRegistryName falling back to case insensitive search for: {}. Exception: {}", glueRegistryName, ex);
            return findGlueRegistryNameIgnoringCasing(glueRegistryName);
        }
    }

    /**
     * List all the tables. It pulls all the schema names from a Glue registry.
     *
     * @param blockAllocator - instance of {@link BlockAllocator}
     * @param federationListTablesRequest - instance of {@link ListTablesRequest}
     * @return {@link ListTablesResponse}
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest federationListTablesRequest)
    {
        LOGGER.info("doListTables: {}", federationListTablesRequest);
        String glueRegistryNameResolved = resolveGlueRegistryName(federationListTablesRequest.getSchemaName());
        LOGGER.info("Resolved Glue registry name to: {}", glueRegistryNameResolved);
        AWSGlue glue = AWSGlueClientBuilder.defaultClient();
        // In this situation we want to loop through all the pages to return up to the MAX_RESULTS size
        // And only do this if we don't have a token passed in, otherwise if we have a token that takes precedence
        // over the fact that the page size was set to unlimited.
        if (federationListTablesRequest.getPageSize() == UNLIMITED_PAGE_SIZE_VALUE &&
                federationListTablesRequest.getNextToken() == null) {
            LOGGER.info("Request page size is UNLIMITED_PAGE_SIZE_VALUE");

            List<TableName> allTableNames = PaginatedRequestIterator.stream((pageToken) -> listSchemasFromGlue(glue, glueRegistryNameResolved, maxGluePageSize, pageToken), ListSchemasResult::getNextToken)
                .flatMap(currentResult ->
                    currentResult.getSchemas().stream()
                        .map(schemaListItem -> schemaListItem.getSchemaName())
                        .map(glueSchemaName -> new TableName(glueRegistryNameResolved, glueSchemaName))
                )
                .limit(MAX_RESULTS + 1)
                .collect(Collectors.toList());

            if (allTableNames.size() > MAX_RESULTS) {
                throw new RuntimeException(
                    String.format("Exceeded maximum result size. Current doListTables result size: %d", allTableNames.size()));
            }
            ListTablesResponse result = new ListTablesResponse(federationListTablesRequest.getCatalogName(), allTableNames, null);
            LOGGER.debug("doListTables result: {}", result);
            return result;
        }

        // Otherwise don't retrieve all pages, just pass through the page token.
        ListSchemasResult listSchemasResultFromGlue = listSchemasFromGlue(
            glue,
            glueRegistryNameResolved,
            federationListTablesRequest.getPageSize(),
            federationListTablesRequest.getNextToken());
        // Convert the glue response into our own federation response
        List<TableName> tableNames = listSchemasResultFromGlue.getSchemas()
            .stream()
            .map(schemaListItem -> schemaListItem.getSchemaName())
            .map(glueSchemaName -> new TableName(glueRegistryNameResolved, glueSchemaName))
            .collect(Collectors.toList());
        // Pass through whatever token we got from Glue to the user
        ListTablesResponse result = new ListTablesResponse(
            federationListTablesRequest.getCatalogName(),
            tableNames,
            listSchemasResultFromGlue.getNextToken());
        LOGGER.debug("doListTables [paginated] result: {}", result);
        return result;
    }

    private String findGlueRegistryNameIgnoringCasing(String glueRegistryNameIn)
    {
        LOGGER.debug("findGlueRegistryNameIgnoringCasing {}", glueRegistryNameIn);
        AWSGlue glue = AWSGlueClientBuilder.defaultClient();

        // Try to find the registry ignoring the case
        String result = PaginatedRequestIterator.stream((pageToken) -> listRegistriesFromGlue(glue, pageToken), ListRegistriesResult::getNextToken)
            .flatMap(currentResult -> filteredRegistriesStream(currentResult.getRegistries().stream()))
            .filter(r -> r.equalsIgnoreCase(glueRegistryNameIn))
            .findAny()
            .orElseThrow(() -> new RuntimeException(String.format("Could not find Glue Registry: %s", glueRegistryNameIn)));
        LOGGER.debug("findGlueRegistryNameIgnoringCasing result: {}", result);
        return result;
    }

    // Assumes that glueRegistryNameIn is already resolved to the right name
    private String findGlueSchemaNameIgnoringCasing(String glueRegistryNameIn, String glueSchemaNameIn)
    {
        LOGGER.debug("findGlueSchemaNameIgnoringCasing {} {}", glueRegistryNameIn, glueSchemaNameIn);
        AWSGlue glue = AWSGlueClientBuilder.defaultClient();
        // List all schemas under the input registry
        // Find the schema name ignoring the case in this page
        String result = PaginatedRequestIterator.stream((pageToken) -> listSchemasFromGlue(glue, glueRegistryNameIn, maxGluePageSize, pageToken), ListSchemasResult::getNextToken)
            .flatMap(currentResult -> currentResult.getSchemas().stream())
            .map(schemaListItem -> schemaListItem.getSchemaName())
            .filter(glueSchemaName -> glueSchemaName.equalsIgnoreCase(glueSchemaNameIn))
            .findAny()
            .orElseThrow(() -> new RuntimeException(String.format("Could not find Glue Schema: %s", glueSchemaNameIn)));

        // Return the found schema
        LOGGER.debug("findGlueSchemaNameIgnoringCasing result: {}", result);
        return result;
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
        LOGGER.info("doGetTable request: {}", getTableRequest);
        Schema tableSchema = null;
        try {
            tableSchema = getSchema(getTableRequest.getTableName().getSchemaName(), getTableRequest.getTableName().getTableName());
        }
        catch (Exception ex) {
            LOGGER.info("doGetTable falling back on case insensitive resolution. Got exception: {}", ex);
            String glueRegistryNameResolved = findGlueRegistryNameIgnoringCasing(getTableRequest.getTableName().getSchemaName());
            String glueSchemaNameResolved = findGlueSchemaNameIgnoringCasing(glueRegistryNameResolved, getTableRequest.getTableName().getTableName());
            tableSchema = getSchema(glueRegistryNameResolved, glueSchemaNameResolved);
        }
        GetTableResponse result = new GetTableResponse(
            getTableRequest.getCatalogName(),
            new TableName(
                tableSchema.getCustomMetadata().get("glueRegistryName"),
                tableSchema.getCustomMetadata().get("glueSchemaName")),
            tableSchema);
        LOGGER.info("doGetTable result: {}", result);
        return result;
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
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) throws Exception
    {
        LOGGER.info("doGetSplits: {}", request);

        // NOTE: Ideally we could have passed through the metadata using
        // enhancePartitionSchema, but that ends up breaking the logic
        // in doGetTableLayout() that checks to see if the metadata is empty before
        // returning a single partition.
        String glueRegistryName = request.getTableName().getSchemaName();
        String glueSchemaName = request.getTableName().getTableName();
        GlueRegistryReader registryReader = new GlueRegistryReader();
        TopicSchema topicSchema = registryReader.getGlueSchema(glueRegistryName, glueSchemaName, TopicSchema.class);
        String topic =  topicSchema.getTopicName();

        LOGGER.info("Retrieved topicName: {}", topic);

        // Get the available partitions of the topic from kafka server.
        List<TopicPartition> topicPartitions = kafkaConsumer.partitionsFor(topic).stream()
                .map(it -> new TopicPartition(it.topic(), it.partition()))
                .collect(Collectors.toList());
        // consumer does not always return the same order for the partitions. We need to sort it so the continuation token
        // has meaning.
        Collections.sort(topicPartitions, (tp1, tp2) -> tp1.partition() - tp2.partition());

        LOGGER.debug("[KafkaPartition] total partitions {} found for topic: {}", topicPartitions.size(), topic);

        // Get start offset of each topic partitions from kafka server.
        Map<TopicPartition, Long> startOffsets = kafkaConsumer.beginningOffsets(topicPartitions);
        if (LOGGER.isDebugEnabled()) {
            startOffsets.forEach((k, v) -> {
                LOGGER.debug("[KafkaPartitionOffset] start offset info [topic: {}, partition: {}, start-offset: {}]",
                        k.topic(), k.partition(), v
                );
            });
        }

        // Get end offset of each topic partitions from kafka server.
        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(topicPartitions);
        if (LOGGER.isDebugEnabled()) {
            endOffsets.forEach((k, v) -> {
                LOGGER.debug("[KafkaPartitionOffset] end offset info [topic: {}, partition: {}, end-offset: {}]",
                        k.topic(), k.partition(), v
                );
            });
        }

        Set<Split> splits = new HashSet<>();
        SpillLocation spillLocation = makeSpillLocation(request);
        int continuationToken = request.getContinuationToken() == null ? 0 : Integer.parseInt(request.getContinuationToken());
        for (
            int partitionIndex = continuationToken;
            partitionIndex < topicPartitions.size();
            partitionIndex++) {
            TopicPartition partition = topicPartitions.get(partitionIndex);
            // Calculate how many pieces we can divide a topic partition.
            List<TopicPartitionPiece>  topicPartitionPieces = pieceTopicPartition(startOffsets.get(partition), endOffsets.get(partition));
            LOGGER.info("[TopicPartitionPiece] Total pieces created {} for partition {} in topic {}",
                    topicPartitionPieces.size(), partition.partition(), partition.topic()
            );
            if (LOGGER.isDebugEnabled()) {
                topicPartitionPieces.forEach(it -> {
                    LOGGER.debug("TopicPartitionPiece,{},{},{}",
                            partition.partition(), it.startOffset, it.endOffset
                    );
                });
            }

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

            // if this isn't the last partition, and we've read more than our max splits per request, paginate the request.
            if (splits.size() >= MAX_SPLITS_PER_REQUEST && partitionIndex < topicPartitions.size() - 1) { 
                LOGGER.debug("[kafka] Total split created {} exceeded MAX_SPLITS_PER_REQUEST, sending paginated response", splits.size());
                String encodedContinuationToken = String.valueOf(partitionIndex + 1);
                return new GetSplitsResponse(request.getCatalogName(), splits, encodedContinuationToken);
            }
        }
        LOGGER.debug("[kafka] Total split created {} ", splits.size());
        return new GetSplitsResponse(request.getCatalogName(), splits);
    }

    /**
     * Create the arrow schema for a specific topic. In the metadata
     * we keep the additional information of topic schema and fields.
     *
     * @param glueRegistryName - the name of the registry in the glue schema registry.
     * @param glueSchemaName - name of the schema inside the registry above.
     * @return {@link Schema}
     * @throws Exception - {@link Exception}
     */
    private Schema getSchema(String glueRegistryName, String glueSchemaName) throws Exception
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

        // Get topic schema json from GLue registry as translated to TopicSchema pojo
        GlueRegistryReader registryReader = new GlueRegistryReader();
        TopicSchema topicSchema = registryReader.getGlueSchema(glueRegistryName, glueSchemaName, TopicSchema.class);

        // Creating ArrowType for each fields in the topic schema.
        // Also putting the additional column level information
        // into the metadata in ArrowType field.
        topicSchema.getMessage().getFields().forEach(it -> {
            FieldType fieldType = new FieldType(
                    true,
                    AmazonMskUtils.toArrowType(it.getType()),
                    null,
                    com.google.common.collect.ImmutableMap.of(
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

        // NOTE: these values are being shoved in here for usage later in the calling context
        // of doGetTable() since Java doesn't have tuples.
        schemaBuilder.addMetadata("glueRegistryName", glueRegistryName);
        schemaBuilder.addMetadata("glueSchemaName", glueSchemaName);

        return schemaBuilder.build();
    }

    /**
     * Splits topic partition into smaller piece and calculates
     * the start and end offsets of each piece.
     *
     * @param startOffset - the first offset of topic partition
     * @param endOffset - the last offset of topic partition
     * @return {@link List<TopicPartitionPiece>}
     */
    public  List<TopicPartitionPiece> pieceTopicPartition(long startOffset, long endOffset)
    {
        List<TopicPartitionPiece> topicPartitionPieces = new ArrayList<>();

        // If endOffset + 1 is smaller or equal to MAX_RECORDS_IN_SPLIT then we do not
        // need to piece the topic partition.
        if (endOffset + 1 <= startOffset + MAX_RECORDS_IN_SPLIT) {
            topicPartitionPieces.add(new TopicPartitionPiece(startOffset, endOffset));
            return topicPartitionPieces;
        }

        // Get how many offsets are there
        long totalOffset = endOffset - startOffset;

        // We need to piece the partition basing its end offset.
        // Calculate the number of pieces for the topic partition.
        int pieces = (int) Math.ceil((float) totalOffset / (float) MAX_RECORDS_IN_SPLIT);

        // Set the start and end offset for the first piece
        long xOffset = startOffset;
        long yOffset = startOffset + MAX_RECORDS_IN_SPLIT;

        // Now we will traverse on loop for the calculated pieces and
        // keep calculating the start and end offsets for each piece
        // until we reach to the end of loop.
        for (int i = 0; i < pieces; i++) {
            topicPartitionPieces.add(new TopicPartitionPiece(xOffset, yOffset));
            xOffset = yOffset + 1;
            yOffset = xOffset + MAX_RECORDS_IN_SPLIT;

            // The last yOffset of the last piece must not be greater than the endOffset
            // of the topic partition, it will be at least equal to endOffset of the topic partition.
            yOffset = Math.min(yOffset, endOffset);
        }

        return topicPartitionPieces;
    }
}
