/*-
 * #%L
 * athena-example
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
package com.amazonaws.connectors.athena.elasticsearch;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
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
import com.amazonaws.athena.connector.lambda.metadata.glue.GlueFieldLexer;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is responsible for providing Athena with metadata about the domain (aka databases), indices, contained
 * in your Elasticsearch instance. Additionally, this class tells Athena how to split up reads against this source.
 * This gives you control over the level of performance and parallelism your source can support.
 */
public class ElasticsearchMetadataHandler
        extends GlueMetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchMetadataHandler.class);

    // Used to denote the 'type' of this connector for diagnostic purposes.
    private static final String SOURCE_TYPE = "elasticsearch";

    //The Env variable name used to indicate that we want to disable the use of Glue DataCatalog for supplemental
    //metadata and instead rely solely on the connector's schema inference capabilities.
    private static final String GLUE_ENV = "disable_glue";

    // Env. variable that indicates whether the service is with Amazon ES Service (true) and thus the domain-
    // names and associated endpoints can be auto-discovered via the AWS ES SDK. Or, the Elasticsearch service
    // is external to Amazon (false), and the domain_mapping environment variable should be used instead.
    private static final String AUTO_DISCOVER_ENDPOINT = "auto_discover_endpoint";
    private boolean autoDiscoverEndpoint;

    // Env. variable that holds the mappings of the domain-names to their respective endpoints. The contents of
    // this environment variable is fed into the domainSplitter to populate the domainMap where the key = domain-name,
    // and the value = endpoint.
    private static final String DOMAIN_MAPPING = "domain_mapping";
    // A Map of the domain-names and their respective endpoints.
    private Map<String, String> domainMap;

    // Env. variable that holds the query timeout period for the Cluster-Health queries.
    private static final String QUERY_TIMEOUT_CLUSTER = "query_timeout_cluster";
    private final long queryTimeout;

    /**
     * Key used to store shard information in the Split's properties map (later used by the Record Handler).
     */
    protected static final String SHARD_KEY = "shard";
    /**
     * Value used in combination with the shard ID to store shard information in the Split's properties map (later
     * used by the Record Handler). The completed value is sent as a request preference to retrieve a specific shard
     * from the Elasticsearch instance (e.g. "_shards:5" - retrieve shard number 5).
     */
    private static final String SHARD_VALUE = "_shards:";

    private final AWSGlue awsGlue;
    private final AwsRestHighLevelClientFactory clientFactory;
    private final ElasticsearchDomainMapProvider domainMapProvider;

    private ElasticsearchGlueTypeMapper glueTypeMapper;

    public ElasticsearchMetadataHandler()
    {
        //Disable Glue if the env var is present and not explicitly set to "false"
        super((System.getenv(GLUE_ENV) != null && !"false".equalsIgnoreCase(System.getenv(GLUE_ENV))), SOURCE_TYPE);
        this.awsGlue = getAwsGlue();
        this.autoDiscoverEndpoint = getEnv(AUTO_DISCOVER_ENDPOINT).equalsIgnoreCase("true");
        this.domainMapProvider = new ElasticsearchDomainMapProvider(this.autoDiscoverEndpoint);
        this.domainMap = domainMapProvider.getDomainMap(resolveSecrets(getEnv(DOMAIN_MAPPING)));
        this.clientFactory = new AwsRestHighLevelClientFactory(this.autoDiscoverEndpoint);
        this.glueTypeMapper = new ElasticsearchGlueTypeMapper();
        this.queryTimeout = Long.parseLong(getEnv(QUERY_TIMEOUT_CLUSTER));
    }

    @VisibleForTesting
    protected ElasticsearchMetadataHandler(AWSGlue awsGlue,
                                           EncryptionKeyFactory keyFactory,
                                           AWSSecretsManager awsSecretsManager,
                                           AmazonAthena athena,
                                           String spillBucket,
                                           String spillPrefix,
                                           ElasticsearchDomainMapProvider domainMapProvider,
                                           AwsRestHighLevelClientFactory clientFactory,
                                           long queryTimeout)
    {
        super(awsGlue, keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
        this.awsGlue = awsGlue;
        this.domainMapProvider = domainMapProvider;
        this.domainMap = this.domainMapProvider.getDomainMap(null);
        this.clientFactory = clientFactory;
        this.glueTypeMapper = new ElasticsearchGlueTypeMapper();
        this.queryTimeout = queryTimeout;
    }

    /**
     * Get an environment variable using System.getenv().
     * @param var is the environment variable.
     * @return the contents of the environment variable or an empty String if it's not defined.
     */
    protected final String getEnv(String var)
    {
        String result = System.getenv(var);

        return result == null ? "" : result;
    }

    /**
     * Used to get the list of domains (aka databases) for the Elasticsearch service.
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @return A ListSchemasResponse which primarily contains a Set<String> of schema names and a catalog name
     * corresponding the Athena catalog that was queried.
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        logger.debug("doListSchemaNames: enter - " + request);

        return new ListSchemasResponse(request.getCatalogName(), domainMap.keySet());
    }

    /**
     * Used to get the list of indices contained in the specified domain.
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog and database they are querying.
     * @return A ListTablesResponse which primarily contains a List<TableName> enumerating the tables in this
     * catalog, database tuple. It also contains the catalog name corresponding the Athena catalog that was queried.
     * @throws RuntimeException when the domain does not exist in the map, or the client is unable to retrieve the
     * indices from the Elasticsearch instance.
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
            throws RuntimeException
    {
        logger.debug("doListTables: enter - " + request);

        List<TableName> indices = new ArrayList<>();

        try {
            String endpoint = getDomainEndpoint(request.getSchemaName());
            AwsRestHighLevelClient client = clientFactory.getOrCreateClient(endpoint);
            try {
                for (String index : client.getAliases()) {
                    // Ignore all system indices starting with period `.` (e.g. .kibana, .tasks, etc...)
                    if (index.startsWith(".")) {
                        logger.info("Ignoring system index: {}", index);
                        continue;
                    }

                    indices.add(new TableName(request.getSchemaName(), index));
                }
            }
            catch (IOException error) {
                throw new RuntimeException("Error retrieving indices: " + error.getMessage(), error);
            }
        }
        catch (RuntimeException error) {
            throw new RuntimeException("Error processing request to list indices: " + error.getMessage(), error);
        }

        return new ListTablesResponse(request.getCatalogName(), indices);
    }

    /**
     * Used to get definition (field names, types, descriptions, etc...) of a Table.
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @return A GetTableResponse which primarily contains:
     * 1. An Apache Arrow Schema object describing the table's columns, types, and descriptions.
     * 2. A Set<String> of partition column names (or empty if the table isn't partitioned).
     * 3. A TableName object confirming the schema and table name the response is for.
     * 4. A catalog name corresponding the Athena catalog that was queried.
     * @throws RuntimeException when the domain does not exist in the map, or the client is unable to retrieve mapping
     * information for the index from the Elasticsearch instance.
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
            throws RuntimeException
    {
        logger.debug("doGetTable: enter - " + request);

        Schema schema = null;

        // Look at GLUE catalog first.
        try {
            if (awsGlue != null) {
                schema = super.doGetTable(allocator, request).getSchema();
                logger.info("doGetTable: Retrieved schema for table[{}] from AWS Glue.", request.getTableName());
            }
        }
        catch (Exception error) {
            logger.warn("doGetTable: Unable to retrieve table[{}:{}] from AWS Glue.",
                    request.getTableName().getSchemaName(),
                    request.getTableName().getTableName(),
                    error);
        }

        // Supplement GLUE catalog if not present.
        if (schema == null) {
            String index = request.getTableName().getTableName();
            try {
                String endpoint = getDomainEndpoint(request.getTableName().getSchemaName());
                AwsRestHighLevelClient client = clientFactory.getOrCreateClient(endpoint);
                try {
                    Map<String, Object> mappings = client.getMapping(index);
                    schema = ElasticsearchSchemaUtils.parseMapping(mappings);
                }
                catch (IOException error) {
                    throw new RuntimeException("Error retrieving mapping information for index (" +
                            index + "): " + error.getMessage(), error);
                }
            }
            catch (RuntimeException error) {
                throw new RuntimeException("Error processing request to map index (" +
                        index + "): " + error.getMessage(), error);
            }
        }

        return new GetTableResponse(request.getCatalogName(), request.getTableName(),
                (schema == null) ? SchemaBuilder.newBuilder().build() : schema, Collections.emptySet());
    }

    /**
     * Elasticsearch does not support partitioning so this method is a NoOp.
     * @param blockWriter Used to write rows (partitions) into the Apache Arrow response.
     * @param request Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
    {
        // NoOp - Elasticsearch does not support partitioning.
    }

    /**
     * Used to split-up the reads required to scan the requested index by shard. Cluster-health information is
     * retrieved for shards associated with the specified index. A split will then be generated for each shard that
     * is primary and active.
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details of the catalog, domain, and index being queried, as well as any filter predicate.
     * @return A GetSplitsResponse which primarily contains:
     * 1. A Set<Split> each containing a domain and endpoint, and the shard to be retrieved by the Record handler.
     * 2. (Optional) A continuation token which allows you to paginate the generation of splits for large queries.
     * @throws RuntimeException when the domain does not exist in the map, or an error occurs while processing the
     * cluster/shard health information.
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
            throws RuntimeException
    {
        logger.debug("doGetSplits: enter - " + request);

        // Create set of splits
        Set<Split> splits = new HashSet<>();
        // Get domain
        String domain = request.getTableName().getSchemaName();
        // Get index
        String index = request.getTableName().getTableName();

        try {
            String endpoint = getDomainEndpoint(domain);
            AwsRestHighLevelClient client = clientFactory.getOrCreateClient(endpoint);
            try {
                Set<Integer> shardIds = client.getShardIds(index, queryTimeout);
                for (Integer shardId : shardIds) {
                    // Every split must have a unique location if we wish to spill to avoid failures
                    SpillLocation spillLocation = makeSpillLocation(request);
                    // Create a new split (added to the splits set) that includes the domain and endpoint, and
                    // shard information (to be used later by the Record Handler).
                    splits.add(new Split(spillLocation, makeEncryptionKey(), ImmutableMap
                            .of(domain, endpoint, SHARD_KEY, SHARD_VALUE + shardId.toString())));
                }
            }
            catch (IOException error) {
                throw new RuntimeException("Error retrieving shard-health information: " + error.getMessage(), error);
            }
        }
        catch (RuntimeException error) {
            throw new RuntimeException("Error trying to generate splits for index (" +
                    index + "): " + error.getMessage(), error);
        }

        return new GetSplitsResponse(request.getCatalogName(), splits);
    }

    /**
     * Gets an endpoint from the domain mapping. For AWS Elasticsearch Service, if the domain does not exist in
     * the domain map, refresh the latter by calling the AWS ES SDK (it's possible that the domain was added
     * after the last connector refresh).
     * @param domain is used for searching the domain map for the corresponding endpoint.
     * @return endpoint corresponding to the domain or a null if domain does not exist in the map.
     * @throws RuntimeException when the endpoint does not exist in the domain map even after a map refresh.
     */
    private String getDomainEndpoint(String domain)
            throws RuntimeException
    {
        String endpoint = domainMap.get(domain);

        if (endpoint == null && autoDiscoverEndpoint) {
            logger.warn("Unable to find domain ({}) in map! Attempting to refresh map...", domain);
            domainMap = domainMapProvider.getDomainMap(null);
            endpoint = domainMap.get(domain);
        }

        if (endpoint == null) {
            throw new RuntimeException("Unable to find domain: " + domain);
        }

        return endpoint;
    }

    /**
     * @see GlueMetadataHandler
     */
    @Override
    protected Field convertField(String fieldName, String glueType)
    {
        logger.debug("convertField - fieldName: {}, glueType: {}", fieldName, glueType);

        return GlueFieldLexer.lex(fieldName, glueType, glueTypeMapper);
    }
}
