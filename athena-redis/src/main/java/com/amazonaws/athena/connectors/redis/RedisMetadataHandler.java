/*-
 * #%L
 * athena-redis
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
package com.amazonaws.athena.connectors.redis;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.glue.DefaultGlueType;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.complex.reader.VarCharReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;

/**
 * Handles metadata requests for the Athena Redis Connector using Glue for schema.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Uses Glue table properties (redis-endpoint, redis-value-type, redis-key-prefix, and redis-keys-zset) to
 * provide schema as well as connectivity details to Redis.
 * 2. Attempts to resolve sensitive fields such as redis-endpoint via SecretsManager so that you can substitute
 * variables with values from by doing something like hostname:port:password=${my_secret}
 */
public class RedisMetadataHandler
        extends GlueMetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(RedisMetadataHandler.class);

    private static final String SOURCE_TYPE = "redis";
    private static final String END_CURSOR = "0";
    //Controls the max splits to generate, relevant keys are spread across this many splits where possible.
    private static final long REDIS_MAX_SPLITS = 10;
    //The page size for Jedis scans.
    private static final int SCAN_COUNT_SIZE = 100;
    protected static final String KEY_COLUMN_NAME = "_key_";
    protected static final String SPLIT_START_INDEX = "start-index";
    protected static final String SPLIT_END_INDEX = "end-index";

    //Defines the table property name used to set the Redis Key Type for the table. (e.g. prefix, zset)
    protected static final String KEY_TYPE = "redis-key-type";
    //Defines the table property name used to set the Redis value type for the table. (e.g. liternal, zset, hash)
    protected static final String VALUE_TYPE_TABLE_PROP = "redis-value-type";
    //Defines the table property name used to configure one or more key prefixes to include in the
    //table (e.g. key-prefix-1-*, key-prefix-2-*)
    protected static final String KEY_PREFIX_TABLE_PROP = "redis-key-prefix";
    //Defines the table property name used to configure one or more zset keys whos values should be used as keys
    //to include in the table.
    protected static final String ZSET_KEYS_TABLE_PROP = "redis-keys-zset";
    protected static final String KEY_PREFIX_SEPERATOR = ",";
    //Defines the table property name used to configure the redis enpoint to query for the data in that table.
    //Connection String format is expected to be host:port or host:port:password_token
    protected static final String REDIS_ENDPOINT_PROP = "redis-endpoint";
    //Defines the value that should be present in the Glue Database URI to enable the DB for Redis.
    protected static final String REDIS_DB_FLAG = "redis-db-flag";

    //Used to filter out Glue tables which lack a redis endpoint.
    private static final TableFilter TABLE_FILTER = (Table table) -> table.getParameters().containsKey(REDIS_ENDPOINT_PROP);
    //Used to filter out Glue databases which lack the REDIS_DB_FLAG in the URI.
    private static final DatabaseFilter DB_FILTER = (Database database) -> database.getLocationUri().contains(REDIS_DB_FLAG);

    private final AWSGlue awsGlue;
    private final JedisPoolFactory jedisPoolFactory;

    public RedisMetadataHandler()
    {
        super(AWSGlueClientBuilder.standard().build(), SOURCE_TYPE);
        this.awsGlue = getAwsGlue();
        this.jedisPoolFactory = new JedisPoolFactory();
    }

    @VisibleForTesting
    protected RedisMetadataHandler(AWSGlue awsGlue,
            EncryptionKeyFactory keyFactory,
            AWSSecretsManager secretsManager,
            JedisPoolFactory jedisPoolFactory,
            String spillBucket,
            String spillPrefix)
    {
        super(awsGlue, keyFactory, secretsManager, SOURCE_TYPE, spillBucket, spillPrefix);
        this.awsGlue = awsGlue;
        this.jedisPoolFactory = jedisPoolFactory;
    }

    /**
     * Used to obtain a Redis client connection for the provided endpoint.
     *
     * @param rawEndpoint The value from the REDIS_ENDPOINT_PROP on the table being queried.
     * @return A Jedis client connection.
     * @notes This method first attempts to resolve any secrets (noted by ${secret_name}) using SecretsManager.
     */
    private Jedis getOrCreateClient(String rawEndpoint)
    {
        String endpoint = resolveSecrets(rawEndpoint);
        return jedisPoolFactory.getOrCreateConn(endpoint);
    }

    /**
     * @see GlueMetadataHandler
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest request)
            throws Exception
    {
        return doListSchemaNames(blockAllocator, request, DB_FILTER);
    }

    /**
     * @see GlueMetadataHandler
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest request)
            throws Exception
    {
        return super.doListTables(blockAllocator, request, TABLE_FILTER);
    }

    /**
     * Retrieves the schema for the request Table from Glue then enriches that result with Redis specific
     * metadata and columns.
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request)
            throws Exception
    {
        GetTableResponse response = super.doGetTable(blockAllocator, request);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        response.getSchema().getFields().forEach((Field field) ->
                schemaBuilder.addField(field.getName(), field.getType(), field.getChildren())
        );

        response.getSchema().getCustomMetadata().entrySet().forEach((Map.Entry<String, String> meta) ->
                schemaBuilder.addMetadata(meta.getKey(), meta.getValue()));

        schemaBuilder.addField(KEY_COLUMN_NAME, Types.MinorType.VARCHAR.getType());

        return new GetTableResponse(response.getCatalogName(), response.getTableName(), schemaBuilder.build());
    }

    @Override
    public void enhancePartitionSchema(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request)
    {
        partitionSchemaBuilder.addStringField(REDIS_ENDPOINT_PROP)
                .addStringField(VALUE_TYPE_TABLE_PROP)
                .addStringField(KEY_PREFIX_TABLE_PROP)
                .addStringField(ZSET_KEYS_TABLE_PROP);
    }

    /**
     * Even though our table doesn't support complex layouts or partitioning, we need to convey that there is at least
     * 1 partition to read as part of the query or Athena will assume partition pruning found no candidate layouts to read.
     * We also use this 1 partition to carry settings that we will need in order to generate splits.
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request)
            throws Exception
    {
        Map<String, String> properties = request.getSchema().getCustomMetadata();
        blockWriter.writeRows((Block block, int rowNum) -> {
            block.setValue(REDIS_ENDPOINT_PROP, rowNum, properties.get(REDIS_ENDPOINT_PROP));
            block.setValue(VALUE_TYPE_TABLE_PROP, rowNum, properties.get(VALUE_TYPE_TABLE_PROP));
            block.setValue(KEY_PREFIX_TABLE_PROP, rowNum, properties.get(KEY_PREFIX_TABLE_PROP));
            block.setValue(ZSET_KEYS_TABLE_PROP, rowNum, properties.get(ZSET_KEYS_TABLE_PROP));
            return 1;
        });
    }

    /**
     * If the table is comprised of multiple key prefixes, then we parallelize those by making them each a split.
     *
     * @note This function essentially takes each key-prefix and makes it a split. For zset keys, it breaks each zset
     * into a max of N split that we have configured to generate as defined by REDIS_MAX_SPLITS.
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest request)
    {
        if (request.getPartitions().getRowCount() != 1) {
            throw new RuntimeException("Unexpected number of partitions encountered.");
        }

        Block partitions = request.getPartitions();
        String redisEndpoint = getValue(partitions, 0, REDIS_ENDPOINT_PROP);
        String redisValueType = getValue(partitions, 0, VALUE_TYPE_TABLE_PROP);

        logger.info("doGetSplits: Preparing splits for {}", BlockUtils.rowToString(partitions, 0));

        KeyType keyType = null;
        Set<String> splitInputs = new HashSet<>();

        String keyPrefix = getValue(partitions, 0, KEY_PREFIX_TABLE_PROP);
        if (keyPrefix != null) {
            //Add the prefixes to the list and set the key type.
            splitInputs.addAll(Arrays.asList(keyPrefix.split(KEY_PREFIX_SEPERATOR)));
            keyType = KeyType.PREFIX;
        }
        else {
            String[] partitionPrefixes = getValue(partitions, 0, ZSET_KEYS_TABLE_PROP).split(KEY_PREFIX_SEPERATOR);

            ScanResult<String> keyCursor = null;
            //Add all the values in the ZSETs ad keys to scan
            for (String next : partitionPrefixes) {
                do {
                    keyCursor = loadKeys(redisEndpoint, next, keyCursor, splitInputs);
                }
                while (keyCursor != null && !END_CURSOR.equals(keyCursor.getCursor()));
            }
            keyType = KeyType.ZSET;
        }

        Set<Split> splits = new HashSet<>();
        for (String next : splitInputs) {
            splits.addAll(makeSplits(request, redisEndpoint, next, keyType, redisValueType));
        }

        return new GetSplitsResponse(request.getCatalogName(), splits, null);
    }

    /**
     * For a given key prefix this method attempts to break up all the matching keys into N buckets (aka N splits).
     *
     * @param request
     * @param endpoint The redis endpoint to query.
     * @param keyPrefix The key prefix to scan.
     * @param keyType The KeyType (prefix or zset).
     * @param valueType The ValueType, used for mapping the values stored at each key to a result row when the split is processed.
     * @return A Set of splits to optionally parallelize reading the values associated with the keyPrefix.
     */
    private Set<Split> makeSplits(GetSplitsRequest request, String endpoint, String keyPrefix, KeyType keyType, String valueType)
    {
        Set<Split> splits = new HashSet<>();
        long numberOfKeys = 1;

        if (keyType == KeyType.ZSET) {
            try (Jedis client = getOrCreateClient(endpoint)) {
                numberOfKeys = client.zcount(keyPrefix, "-inf", "+inf");
                logger.info("makeSplits: ZCOUNT[{}] found [{}]", keyPrefix, numberOfKeys);
            }
        }

        long stride = (numberOfKeys > REDIS_MAX_SPLITS) ? 1 + (numberOfKeys / REDIS_MAX_SPLITS) : numberOfKeys;

        for (long startIndex = 0; startIndex < numberOfKeys; startIndex += stride) {
            long endIndex = startIndex + stride - 1;
            if (endIndex >= numberOfKeys) {
                endIndex = -1;
            }

            //Every split must have a unique location if we wish to spill to avoid failures
            SpillLocation spillLocation = makeSpillLocation(request);

            Split split = Split.newBuilder(spillLocation, makeEncryptionKey())
                    .add(KEY_PREFIX_TABLE_PROP, keyPrefix)
                    .add(KEY_TYPE, keyType.getId())
                    .add(VALUE_TYPE_TABLE_PROP, valueType)
                    .add(REDIS_ENDPOINT_PROP, endpoint)
                    .add(SPLIT_START_INDEX, String.valueOf(startIndex))
                    .add(SPLIT_END_INDEX, String.valueOf(endIndex))
                    .build();

            splits.add(split);

            logger.info("makeSplits: Split[{}]", split);
        }

        return splits;
    }

    /**
     * For the given zset prefix, find all values and treat each of those values are a key to scan before returning
     * the scan continuation token.
     *
     * @param connStr The Jedis connection string for the table.
     * @param prefix The zset key prefix to scan.
     * @param redisCursor The previous Redis cursor (aka continuation token).
     * @param keys The collections of keys we collected so far. Any new keys we find are added to this.
     * @return The Redis cursor to use when continuing the scan.
     */
    private ScanResult<String> loadKeys(String connStr, String prefix, ScanResult<String> redisCursor, Set<String> keys)
    {
        try (Jedis client = getOrCreateClient(connStr)) {
            String cursor = (redisCursor == null) ? SCAN_POINTER_START : redisCursor.getCursor();
            ScanParams scanParam = new ScanParams();
            scanParam.count(SCAN_COUNT_SIZE);
            scanParam.match(prefix);

            ScanResult<String> newCursor = client.scan(cursor, scanParam);
            keys.addAll(newCursor.getResult());
            return newCursor;
        }
    }

    /**
     * Overrides the default Glue Type to Apache Arrow Type mapping so that we can fail fast on tables which define
     * types that are not supported by this connector.
     */
    @Override
    protected Field convertField(String name, String type)
    {
        return FieldBuilder.newBuilder(name, DefaultGlueType.fromId(type).getArrowType()).build();
    }

    private String getValue(Block block, int row, String fieldName)
    {
        VarCharReader reader = block.getFieldReader(fieldName);
        reader.setPosition(row);
        if (reader.isSet()) {
            Text result = reader.readText();
            return (result == null) ? null : result.toString();
        }

        return null;
    }
}
