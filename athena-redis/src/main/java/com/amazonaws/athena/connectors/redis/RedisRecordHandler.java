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
import com.amazonaws.athena.connectors.redis.lettuce.RedisCommandsWrapper;
import com.amazonaws.athena.connectors.redis.lettuce.RedisConnectionFactory;
import com.amazonaws.athena.connectors.redis.lettuce.RedisConnectionWrapper;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScoredValueScanCursor;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.KEY_COLUMN_NAME;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.KEY_PREFIX_TABLE_PROP;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.KEY_TYPE;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.REDIS_CLUSTER_FLAG;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.REDIS_DB_NUMBER;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.REDIS_ENDPOINT_PROP;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.REDIS_SSL_FLAG;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.SPLIT_END_INDEX;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.SPLIT_START_INDEX;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.VALUE_TYPE_TABLE_PROP;
import static io.lettuce.core.ScanCursor.FINISHED;
import static io.lettuce.core.ScanCursor.INITIAL;

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

    private final RedisConnectionFactory redisConnectionFactory;
    private final AmazonS3 amazonS3;

    public RedisRecordHandler()
    {
        this(AmazonS3ClientBuilder.standard().build(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient(),
                new RedisConnectionFactory());
    }

    @VisibleForTesting
    protected RedisRecordHandler(AmazonS3 amazonS3,
            AWSSecretsManager secretsManager,
            AmazonAthena athena,
            RedisConnectionFactory redisConnectionFactory)
    {
        super(amazonS3, secretsManager, athena, SOURCE_TYPE);
        this.amazonS3 = amazonS3;
        this.redisConnectionFactory = redisConnectionFactory;
    }

    /**
     * Used to obtain a Redis client connection for the provided endpoint.
     *
     * @param rawEndpoint The value from the REDIS_ENDPOINT_PROP on the table being queried.
     * @param sslEnabled The value from the REDIS_SSL_FLAG on the table being queried.
     * @param isCluster The value from the REDIS_CLUSTER_FLAG on the table being queried.
     * @param dbNumber The value from the REDIS_DB_NUMBER on the table being queried.
     * @return A Lettuce client connection.
     * @notes This method first attempts to resolve any secrets (noted by ${secret_name}) using SecretsManager.
     */
    private RedisConnectionWrapper<String, String> getOrCreateClient(String rawEndpoint, boolean sslEnabled,
                                                                     boolean isCluster, String dbNumber)
    {
        String endpoint = resolveSecrets(rawEndpoint);
        return redisConnectionFactory.getOrCreateConn(endpoint, sslEnabled, isCluster, dbNumber);
    }

    /**
     * @see RecordHandler
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
    {
        Split split = recordsRequest.getSplit();
        ScanCursor keyCursor = null;
        boolean sslEnabled = Boolean.parseBoolean(split.getProperty(REDIS_SSL_FLAG));
        boolean isCluster = Boolean.parseBoolean(split.getProperty(REDIS_CLUSTER_FLAG));
        String dbNumber = split.getProperty(REDIS_DB_NUMBER);
        ValueType valueType = ValueType.fromId(split.getProperty(VALUE_TYPE_TABLE_PROP));
        List<Field> fieldList = recordsRequest.getSchema().getFields().stream()
                .filter((Field next) -> !KEY_COLUMN_NAME.equals(next.getName())).collect(Collectors.toList());

        RedisConnectionWrapper<String, String> connection = getOrCreateClient(split.getProperty(REDIS_ENDPOINT_PROP),
                                                                              sslEnabled, isCluster, dbNumber);
        RedisCommandsWrapper<String, String> syncCommands = connection.sync();

        do {
            Set<String> keys = new HashSet<>();
            //Load all the keys associated with this split
            keyCursor = loadKeys(syncCommands, split, keyCursor, keys);

            //Scan the data associated with all the keys.
            for (String nextKey : keys) {
                if (!queryStatusChecker.isQueryRunning()) {
                    return;
                }
                switch (valueType) {
                    case LITERAL:   //The key value is a row with single column
                        loadLiteralRow(syncCommands, nextKey, spiller, fieldList);
                        break;
                    case HASH:
                        loadHashRow(syncCommands, nextKey, spiller, fieldList);
                        break;
                    case ZSET:
                        loadZSetRows(syncCommands, nextKey, spiller, fieldList);
                        break;
                    default:
                        throw new RuntimeException("Unsupported value type " + valueType);
                }
            }
        }
        while (keyCursor != null && !keyCursor.isFinished());
    }

    /**
     * For the given key prefix, find all actual keys depending on the type of the key.
     *
     * @param syncCommands The Lettuce Client
     * @param split The split for this request, mostly used to get the redis endpoint and config details.
     * @param redisCursor The previous Redis cursor (aka continuation token).
     * @param keys The collections of keys we collected so far. Any new keys we find are added to this.
     * @return The Redis cursor to use when continuing the scan.
     */
    private ScanCursor loadKeys(RedisCommandsWrapper<String, String> syncCommands, Split split,
                                        ScanCursor redisCursor, Set<String> keys)
    {
        KeyType keyType = KeyType.fromId(split.getProperty(KEY_TYPE));
        String keyPrefix = split.getProperty(KEY_PREFIX_TABLE_PROP);
        if (keyType == KeyType.ZSET) {
            long start = Long.valueOf(split.getProperty(SPLIT_START_INDEX));
            long end = Long.valueOf(split.getProperty(SPLIT_END_INDEX));
            keys.addAll(syncCommands.zrange(keyPrefix, start, end));
            return FINISHED;
        }
        else {
            ScanCursor cursor = (redisCursor == null) ? INITIAL : redisCursor;
            ScanArgs scanArgs = new ScanArgs();
            scanArgs.limit(SCAN_COUNT_SIZE);
            scanArgs.match(split.getProperty(KEY_PREFIX_TABLE_PROP));

            KeyScanCursor<String> newCursor = syncCommands.scan(cursor, scanArgs);
            keys.addAll(newCursor.getKeys());
            return newCursor;
        }
    }

    private void loadLiteralRow(RedisCommandsWrapper<String, String> syncCommands, String keyString, BlockSpiller spiller, List<Field> fieldList)
    {
        spiller.writeRows((Block block, int row) -> {
            if (fieldList.size() != 1) {
                throw new RuntimeException("Ambiguous field mapping, more than 1 field for literal value type.");
            }

            Field field = fieldList.get(0);
            Object value = ValueConverter.convert(field, syncCommands.get(keyString));
            boolean literalMatched = block.offerValue(KEY_COLUMN_NAME, row, keyString);
            literalMatched &= block.offerValue(field.getName(), row, value);
            return literalMatched ? 1 : 0;
        });
    }

    private void loadHashRow(RedisCommandsWrapper<String, String> syncCommands, String keyString, BlockSpiller spiller,
                             List<Field> fieldList)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean hashMatched = block.offerValue(KEY_COLUMN_NAME, row, keyString);

            Map<String, String> rawValues = new HashMap<>();
            //Glue only supports lowercase column names / also could do a better job only fetching the columns
            //that are needed
            syncCommands.hgetall(keyString).forEach((key, entry) -> rawValues.put(key.toLowerCase(), entry));

            for (Field hfield : fieldList) {
                Object hvalue = ValueConverter.convert(hfield, rawValues.get(hfield.getName()));
                if (hashMatched && !block.offerValue(hfield.getName(), row, hvalue)) {
                    return 0;
                }
            }

            return 1;
        });
    }

    private void loadZSetRows(RedisCommandsWrapper<String, String> syncCommands, String keyString, BlockSpiller spiller,
                              List<Field> fieldList)
    {
        if (fieldList.size() != 1) {
            throw new RuntimeException("Ambiguous field mapping, more than 1 field for ZSET value type.");
        }

        Field zfield = fieldList.get(0);
        ScoredValueScanCursor<String> cursor = null;
        do {
            cursor = syncCommands.zscan(keyString, cursor == null ? INITIAL : cursor);
            for (ScoredValue<String> nextElement : cursor.getValues()) {
                spiller.writeRows((Block block, int rowNum) -> {
                    Object zvalue = ValueConverter.convert(zfield, nextElement.getValue());
                    boolean zsetMatched = block.offerValue(KEY_COLUMN_NAME, rowNum, keyString);
                    zsetMatched &= block.offerValue(zfield.getName(), rowNum, zvalue);
                    return zsetMatched ? 1 : 0;
                });
            }
        }
        while (!cursor.isFinished());
    }

    /**
     * @param split The split for this request, mostly used to get the redis endpoint and config details.
     * @param keyString The key to read.
     * @param spiller The BlockSpiller to write results into.
     * @param startPos The starting postion in the block
     * @return The number of rows created in the result block.
     */
}
