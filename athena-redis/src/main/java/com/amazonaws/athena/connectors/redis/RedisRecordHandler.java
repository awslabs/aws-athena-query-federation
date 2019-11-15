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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.KEY_COLUMN_NAME;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.KEY_PREFIX_TABLE_PROP;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.KEY_TYPE;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.REDIS_ENDPOINT_PROP;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.SPLIT_END_INDEX;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.SPLIT_START_INDEX;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.VALUE_TYPE_TABLE_PROP;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;

/**
 * Handles data read record requests for the Athena Redis Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Supporting literal, zset, and hash value types.
 * 2. Attempts to resolve sensitive configuration fields such as redis-endpoint via SecretsManager so that you can
 * substitute variables with values from by doing something like hostname:port:password=${my_secret}
 */
public class RedisRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(RedisRecordHandler.class);

    private static final String SOURCE_TYPE = "redis";
    private static final String END_CURSOR = "0";

    //The page size for Jedis scans.
    private static final int SCAN_COUNT_SIZE = 100;

    private final JedisPoolFactory jedisPoolFactory;
    private final AmazonS3 amazonS3;

    public RedisRecordHandler()
    {
        this(AmazonS3ClientBuilder.standard().build(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient(),
                new JedisPoolFactory());
    }

    @VisibleForTesting
    protected RedisRecordHandler(AmazonS3 amazonS3,
            AWSSecretsManager secretsManager,
            AmazonAthena athena,
            JedisPoolFactory jedisPoolFactory)
    {
        super(amazonS3, secretsManager, athena, SOURCE_TYPE);
        this.amazonS3 = amazonS3;
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
     * @see RecordHandler
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
    {
        Split split = recordsRequest.getSplit();
        ScanResult<String> keyCursor = null;

        final AtomicLong rowsMatched = new AtomicLong(0);
        int numRows = 0;
        do {
            Set<String> keys = new HashSet<>();
            //Load all the keys associated with this split
            keyCursor = loadKeys(split, keyCursor, keys);

            //Scan the data associated with all the keys.
            for (String nextKey : keys) {
                if (!queryStatusChecker.isQueryRunning()) {
                    return;
                }
                try (Jedis client = getOrCreateClient(split.getProperty(REDIS_ENDPOINT_PROP))) {
                    ValueType valueType = ValueType.fromId(split.getProperty(VALUE_TYPE_TABLE_PROP));
                    List<Field> fieldList = recordsRequest.getSchema().getFields().stream()
                            .filter((Field next) -> !KEY_COLUMN_NAME.equals(next.getName())).collect(Collectors.toList());

                    switch (valueType) {
                        case LITERAL:   //The key value is a row with single column
                            loadLiteralRow(client, nextKey, spiller, fieldList);
                            break;
                        case HASH:
                            loadHashRow(client, nextKey, spiller, fieldList);
                            break;
                        case ZSET:
                            loadZSetRows(client, nextKey, spiller, fieldList);
                            break;
                        default:
                            throw new RuntimeException("Unsupported value type " + valueType);
                    }
                }
            }
        }
        while (keyCursor != null && !END_CURSOR.equals(keyCursor.getCursor()));
    }

    /**
     * For the given key prefix, find all actual keys depending on the type of the key.
     *
     * @param split The split for this request, mostly used to get the redis endpoint and config details.
     * @param redisCursor The previous Redis cursor (aka continuation token).
     * @param keys The collections of keys we collected so far. Any new keys we find are added to this.
     * @return The Redis cursor to use when continuing the scan.
     */
    private ScanResult<String> loadKeys(Split split, ScanResult<String> redisCursor, Set<String> keys)
    {
        try (Jedis client = getOrCreateClient(split.getProperty(REDIS_ENDPOINT_PROP))) {
            KeyType keyType = KeyType.fromId(split.getProperty(KEY_TYPE));
            String keyPrefix = split.getProperty(KEY_PREFIX_TABLE_PROP);
            if (keyType == KeyType.ZSET) {
                long start = Long.valueOf(split.getProperty(SPLIT_START_INDEX));
                long end = Long.valueOf(split.getProperty(SPLIT_END_INDEX));
                keys.addAll(client.zrange(keyPrefix, start, end));
                return new ScanResult<String>(END_CURSOR, Collections.EMPTY_LIST);
            }
            else {
                String cursor = (redisCursor == null) ? SCAN_POINTER_START : redisCursor.getCursor();
                ScanParams scanParam = new ScanParams();
                scanParam.count(SCAN_COUNT_SIZE);
                scanParam.match(split.getProperty(KEY_PREFIX_TABLE_PROP));

                ScanResult<String> newCursor = client.scan(cursor, scanParam);
                keys.addAll(newCursor.getResult());
                return newCursor;
            }
        }
    }

    private void loadLiteralRow(Jedis client, String keyString, BlockSpiller spiller, List<Field> fieldList)
    {
        spiller.writeRows((Block block, int row) -> {
            if (fieldList.size() != 1) {
                throw new RuntimeException("Ambiguous field mapping, more than 1 field for literal value type.");
            }

            Field field = fieldList.get(0);
            Object value = ValueConverter.convert(field, client.get(keyString));
            boolean literalMatched = block.offerValue(KEY_COLUMN_NAME, row, keyString);
            literalMatched &= block.offerValue(field.getName(), row, value);
            return literalMatched ? 1 : 0;
        });
    }

    private void loadHashRow(Jedis client, String keyString, BlockSpiller spiller, List<Field> fieldList)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean hashMatched = block.offerValue(KEY_COLUMN_NAME, row, keyString);

            Map<String, String> rawValues = new HashMap<>();
            //Glue only supports lowercase column names / also could do a better job only fetching the columns
            //that are needed
            client.hgetAll(keyString).forEach((key, entry) -> rawValues.put(key.toLowerCase(), entry));

            for (Field hfield : fieldList) {
                Object hvalue = ValueConverter.convert(hfield, rawValues.get(hfield.getName()));
                if (hashMatched && !block.offerValue(hfield.getName(), row, hvalue)) {
                    return 0;
                }
            }

            return 1;
        });
    }

    private void loadZSetRows(Jedis client, String keyString, BlockSpiller spiller, List<Field> fieldList)
    {
        if (fieldList.size() != 1) {
            throw new RuntimeException("Ambiguous field mapping, more than 1 field for ZSET value type.");
        }

        Field zfield = fieldList.get(0);
        String cursor = SCAN_POINTER_START;
        do {
            ScanResult<Tuple> result = client.zscan(keyString, cursor);
            cursor = result.getCursor();
            for (Tuple nextElement : result.getResult()) {
                spiller.writeRows((Block block, int rowNum) -> {
                    Object zvalue = ValueConverter.convert(zfield, nextElement.getElement());
                    boolean zsetMatched = block.offerValue(KEY_COLUMN_NAME, rowNum, keyString);
                    zsetMatched &= block.offerValue(zfield.getName(), rowNum, zvalue);
                    return zsetMatched ? 1 : 0;
                });
            }
        }
        while (cursor != null && !END_CURSOR.equals(cursor));
    }

    /**
     * @param split The split for this request, mostly used to get the redis endpoint and config details.
     * @param keyString The key to read.
     * @param spiller The BlockSpiller to write results into.
     * @param startPos The starting postion in the block
     * @return The number of rows created in the result block.
     */
}
