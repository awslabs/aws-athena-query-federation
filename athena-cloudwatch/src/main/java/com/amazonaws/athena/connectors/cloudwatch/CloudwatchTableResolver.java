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

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogGroup;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.amazonaws.athena.connectors.cloudwatch.CloudwatchMetadataHandler.ALL_LOG_STREAMS_TABLE;

/**
 * This class helps with resolving the differences in casing between cloudwatch log and Presto. Presto expects all
 * databases, tables, and columns to be lower case. This class allows us to use cloudwatch logGroups and logStreams
 * which may have captial letters in them without issue. It does so by caching LogStreams and LogStreams and doing
 * a case insentive search over them. It will first try to do a targeted get to reduce the penalty for LogGroups
 * and LogStreams which don't have capitalization. It also has an optimization for LAMBDA which is a common
 * cause of capitalized LogStreams by doing a targeted replace for LAMBDA's pattern.
 */
public class CloudwatchTableResolver
{
    private static final Logger logger = LoggerFactory.getLogger(CloudwatchTableResolver.class);

    private CloudWatchLogsClient logsClient;
    //Used to handle Throttling events using an AIMD strategy for congestion control.
    private ThrottlingInvoker invoker;
    //The LogStream pattern that is capitalized by LAMBDA
    private static final String LAMBDA_PATTERN = "$latest";
    //The LogStream pattern to replace
    private static final String LAMBDA_ACTUAL_PATTERN = "$LATEST";
    //The schema cache that is presto casing to cloudwatch casing
    private final LoadingCache<String, String> schemaCache;
    //The table cache that is presto casing to cloudwatch casing
    private final LoadingCache<TableName, CloudwatchTableName> tableCache;

    /**
     * Constructs an instance of the table resolver.
     *
     * @param invoker The ThrottlingInvoker to use to handle throttling events.
     * @param logsClient The AWSLogs client to use for cache misses.
     * @param maxSchemaCacheSize The max number of schemas to cache.
     * @param maxTableCacheSize The max tables to cache.
     */
    public CloudwatchTableResolver(ThrottlingInvoker invoker, CloudWatchLogsClient logsClient, long maxSchemaCacheSize, long maxTableCacheSize)
    {
        this.invoker = invoker;
        this.logsClient = logsClient;
        this.tableCache = CacheBuilder.newBuilder()
                .maximumSize(maxTableCacheSize)
                .build(
                        new CacheLoader<TableName, CloudwatchTableName>()
                        {
                            public CloudwatchTableName load(TableName schemaName)
                                    throws TimeoutException
                            {
                                return loadLogStreams(schemaName.getSchemaName(), schemaName.getTableName());
                            }
                        });

        this.schemaCache = CacheBuilder.newBuilder()
                .maximumSize(maxSchemaCacheSize)
                .build(
                        new CacheLoader<String, String>()
                        {
                            public String load(String schemaName)
                                    throws TimeoutException
                            {
                                return loadLogGroups(schemaName);
                            }
                        });
    }

    /**
     * Loads the requested LogStream as identified by the TableName.
     *
     * @param logGroup The properly cased schema name.
     * @param logStream The table name to validate.
     * @return The CloudwatchTableName or null if not found.
     * @note This method also primes the cache with other CloudwatchTableNames found along the way while scaning Cloudwatch.
     */
    private CloudwatchTableName loadLogStreams(String logGroup, String logStream)
            throws TimeoutException
    {
        //As an optimization, see if the table name is an exact match (meaning likely no casing issues)
        CloudwatchTableName result = loadLogStream(logGroup, logStream);
        if (result != null) {
            return result;
        }

        logger.info("loadLogStreams: Did not find a match for the table, falling back to LogGroup scan for  {}:{}",
                logGroup, logStream);
        DescribeLogStreamsRequest.Builder validateTableRequestBuilder = DescribeLogStreamsRequest.builder().logGroupName(logGroup);
        DescribeLogStreamsResponse validateTableResponse;
        do {
            validateTableResponse = invoker.invoke(() -> logsClient.describeLogStreams(validateTableRequestBuilder.build()));
            for (LogStream nextStream : validateTableResponse.logStreams()) {
                String logStreamName = nextStream.logStreamName();
                CloudwatchTableName nextCloudwatch = new CloudwatchTableName(logGroup, logStreamName);
                tableCache.put(nextCloudwatch.toTableName(), nextCloudwatch);
                if (nextCloudwatch.getLogStreamName().equalsIgnoreCase(logStream)) {
                    //We stop loading once we find the one we care about. This is an optimization that
                    //attempt to exploit the fact that we likely access more recent logstreams first.
                    logger.info("loadLogStreams: Matched {} for {}", nextCloudwatch, logStream);
                    return nextCloudwatch;
                }
            }
            validateTableRequestBuilder.nextToken(validateTableResponse.nextToken());
        }
        while (validateTableResponse.nextToken() != null);

        //We could not find a match
        throw new IllegalArgumentException("No such table " + logGroup + " " + logStream);
    }

    /**
     * Optomizaiton that attempts to load a specific  LogStream as identified by the TableName.
     *
     * @param logGroup The properly cased schema name.
     * @param logStream The table name to validate.
     * @return The CloudwatchTableName or null if not found.
     * @note This method also primes the cache with other CloudwatchTableNames found along the way while scanning Cloudwatch.
     */
    private CloudwatchTableName loadLogStream(String logGroup, String logStream)
            throws TimeoutException
    {
        if (ALL_LOG_STREAMS_TABLE.equalsIgnoreCase(logStream)) {
            return new CloudwatchTableName(logGroup, ALL_LOG_STREAMS_TABLE);
        }

        String effectiveTableName = logStream;
        if (effectiveTableName.contains(LAMBDA_PATTERN)) {
            logger.info("loadLogStream: Appears to be a lambda log_stream, substituting Lambda pattern {} for {}",
                    LAMBDA_PATTERN, effectiveTableName);
            effectiveTableName = effectiveTableName.replace(LAMBDA_PATTERN, LAMBDA_ACTUAL_PATTERN);
        }
        DescribeLogStreamsRequest request = DescribeLogStreamsRequest.builder().logGroupName(logGroup)
                .logStreamNamePrefix(effectiveTableName).build();
        DescribeLogStreamsResponse response = invoker.invoke(() -> logsClient.describeLogStreams(request));
        for (LogStream nextStream : response.logStreams()) {
            String logStreamName = nextStream.logStreamName();
            CloudwatchTableName nextCloudwatch = new CloudwatchTableName(logGroup, logStreamName);
            if (nextCloudwatch.getLogStreamName().equalsIgnoreCase(logStream)) {
                logger.info("loadLogStream: Matched {} for {}:{}", nextCloudwatch, logGroup, logStream);
                return nextCloudwatch;
            }
        }

        return null;
    }

    /**
     * Loads the requested LogGroup as identified by the schemaName.
     *
     * @param schemaName The schemaName to load.
     * @return The actual LogGroup name in cloudwatch.
     * @note This method also primes the cache with other LogGroups found along the way while scanning Cloudwatch.
     */
    private String loadLogGroups(String schemaName)
            throws TimeoutException
    {
        //As an optimization, see if the table name is an exact match (meaning likely no casing issues)
        String result = loadLogGroup(schemaName);
        if (result != null) {
            return result;
        }

        logger.info("loadLogGroups: Did not find a match for the schema, falling back to LogGroup scan for  {}", schemaName);
        DescribeLogGroupsRequest.Builder validateSchemaRequestBuilder = DescribeLogGroupsRequest.builder();
        DescribeLogGroupsResponse validateSchemaResponse;
        do {
            validateSchemaResponse = invoker.invoke(() -> logsClient.describeLogGroups(validateSchemaRequestBuilder.build()));
            for (LogGroup next : validateSchemaResponse.logGroups()) {
                String nextLogGroupName = next.logGroupName();
                schemaCache.put(schemaName, nextLogGroupName);
                if (nextLogGroupName.equalsIgnoreCase(schemaName)) {
                    logger.info("loadLogGroups: Matched {} for {}", nextLogGroupName, schemaName);
                    return nextLogGroupName;
                }
            }
            validateSchemaRequestBuilder.nextToken(validateSchemaResponse.nextToken());
        }
        while (validateSchemaResponse.nextToken() != null);

        //We could not find a match
        throw new IllegalArgumentException("No such schema " + schemaName);
    }

    /**
     * Optomizaiton that attempts to load a specific  LogStream as identified by the TableName.
     *
     * @param schemaName The schemaName to load.
     * @return The CloudwatchTableName or null if not found.
     */
    private String loadLogGroup(String schemaName)
            throws TimeoutException
    {
        DescribeLogGroupsRequest request = DescribeLogGroupsRequest.builder().logGroupNamePrefix(schemaName).build();
        DescribeLogGroupsResponse response = invoker.invoke(() -> logsClient.describeLogGroups(request));
        for (LogGroup next : response.logGroups()) {
            String nextLogGroupName = next.logGroupName();
            if (nextLogGroupName.equalsIgnoreCase(schemaName)) {
                logger.info("loadLogGroup: Matched {} for {}", nextLogGroupName, schemaName);
                return nextLogGroupName;
            }
        }

        return null;
    }

    /**
     * Used to validate and convert the given TableName to a properly cased and qualified CloudwatchTableName.
     *
     * @param tableName The TableName to validate and convert.
     * @return The CloudwatchTableName for the provided TableName or throws if the TableName could not be resolved to a
     * CloudwatchTableName. This method mostly handles resolving case mismatches and ensuring the input is a valid entity
     * in Cloudwatch.
     */
    public CloudwatchTableName validateTable(TableName tableName)
    {
        String actualSchema = validateSchema(tableName.getSchemaName());
        CloudwatchTableName actual = null;
        try {
            actual = tableCache.get(new TableName(actualSchema, tableName.getTableName()));
            if (actual == null) {
                throw new IllegalArgumentException("Unknown table[" + tableName + "]");
            }
            return actual;
        }
        catch (ExecutionException ex) {
            throw new RuntimeException("Exception while attempting to validate table " + tableName, ex);
        }
    }

    /**
     * Used to validate and convert the given schema name to a properly cased and qualified CloudwatchTableName.
     *
     * @param schema The TableName to validate and convert.
     * @return The cloudwatch LogGroup (aka schema name) or throws if the schema name could not be resolved to a
     * LogGroup. This method mostly handles resolving case mismatches and ensuring the input is a valid entity
     * in Cloudwatch.
     */
    public String validateSchema(String schema)
    {
        String actual = null;
        try {
            actual = schemaCache.get(schema);
            if (actual == null) {
                throw new IllegalArgumentException("Unknown schema[" + schema + "]");
            }

            return actual;
        }
        catch (ExecutionException ex) {
            throw new RuntimeException("Exception while attempting to validate schema " + schema, ex);
        }
    }
}
