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
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
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
import com.amazonaws.athena.connector.lambda.metadata.glue.DefaultGlueType;
import com.amazonaws.athena.connector.lambda.metadata.glue.GlueFieldLexer;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private final AWSGlue awsGlue;
    private final AwsRestHighLevelClientFactory clientFactory;
    private final ElasticsearchSchemaUtils schemaUtils;
    private final ElasticsearchDomainMapper domainMapper;

    /**
     * This class is used for mapping the Glue data type to Apache Arrow.
     */
    private class ElasticsearchTypeMapper
            implements GlueFieldLexer.BaseTypeMapper
    {
        private static final String SCALING_FACTOR = "scaling_factor";
        private final Pattern scaledFloatPattern = Pattern.compile("SCALED_FLOAT\\(\\d+(\\.\\d+)?\\)");
        private final Pattern scalingFactorPattern = Pattern.compile("\\d+(\\.\\d+)?");

        /**
         * Gets the Arrow type equivalent for the Glue type string representation using the DefaultGlueType.toArrowType
         * conversion routine.
         * @param type is the string representation of a Glue data type to be converted to Apache Arrow.
         * @return an Arrow data type.
         */
        @Override
        public ArrowType getType(String type)
        {
            return DefaultGlueType.toArrowType(type);
        }

        /**
         * Creates a Field object based on the name and type. Special logic is done to extract the scaling factor
         * for a scaled_float data type.
         * @param name is the name of the field.
         * @param type is the string representation of a Glue data type to be converted to Apache Arrow.
         * @return a new Field.
         */
        @Override
        public Field getField(String name, String type)
        {
            if (getType(type) == null) {
                Matcher scaledFloat = scaledFloatPattern.matcher(type);
                if (scaledFloat.find() && scaledFloat.group().length() == type.length()) {
                    Matcher scalingFactor = scalingFactorPattern.matcher(scaledFloat.group());
                    if (scalingFactor.find()) {
                        return new Field(name, new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                                Collections.singletonMap(SCALING_FACTOR, scalingFactor.group())), null);
                    }
                }
                return null;
            }

            return FieldBuilder.newBuilder(name, getType(type)).build();
        }
    }

    private ElasticsearchTypeMapper mapper;

    public ElasticsearchMetadataHandler()
    {
        //Disable Glue if the env var is present and not explicitly set to "false"
        super((System.getenv(GLUE_ENV) != null && !"false".equalsIgnoreCase(System.getenv(GLUE_ENV))), SOURCE_TYPE);
        this.awsGlue = getAwsGlue();
        this.schemaUtils = new ElasticsearchSchemaUtils();
        this.autoDiscoverEndpoint = getEnv(AUTO_DISCOVER_ENDPOINT).equalsIgnoreCase("true");
        this.domainMapper = new ElasticsearchDomainMapper(this.autoDiscoverEndpoint);
        this.domainMap = domainMapper.getDomainMapping(resolveSecrets(getEnv(DOMAIN_MAPPING)));
        this.clientFactory = new AwsRestHighLevelClientFactory(this.autoDiscoverEndpoint);
        this.mapper = new ElasticsearchTypeMapper();
    }

    @VisibleForTesting
    protected ElasticsearchMetadataHandler(AWSGlue awsGlue,
                                           EncryptionKeyFactory keyFactory,
                                           AWSSecretsManager awsSecretsManager,
                                           AmazonAthena athena,
                                           String spillBucket,
                                           String spillPrefix,
                                           ElasticsearchDomainMapper domainMapper,
                                           AwsRestHighLevelClientFactory clientFactory)
    {
        super(awsGlue, keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
        this.awsGlue = awsGlue;
        this.schemaUtils = new ElasticsearchSchemaUtils();
        this.domainMapper = domainMapper;
        this.domainMap = this.domainMapper.getDomainMapping(null);
        this.clientFactory = clientFactory;
        this.mapper = new ElasticsearchTypeMapper();
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
        logger.info("doListSchemaNames: enter - " + request);

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
        logger.info("doListTables: enter - " + request);

        List<TableName> indices = new ArrayList<>();

        try {
            String endpoint = getDomainEndpoint(request.getSchemaName());
            AwsRestHighLevelClient client = clientFactory.getClient(endpoint);
            try {
                for (String index : client.getAliases()) {
                    // Add all Indices except for kibana.
                    if (index.contains("kibana")) {
                        continue;
                    }

                    indices.add(new TableName(request.getSchemaName(), index));
                }
            }
            catch (IOException error) {
                throw new RuntimeException("Error retrieving indices: " + error.getMessage());
            }
            finally {
                client.shutdown();
            }
        }
        catch (RuntimeException error) {
            throw new RuntimeException("Error processing request to list indices: " + error.getMessage());
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
        logger.info("doGetTable: enter - " + request);

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
                AwsRestHighLevelClient client = clientFactory.getClient(endpoint);
                try {
                    LinkedHashMap<String, Object> mappings = client.getMapping(index);
                    schema = schemaUtils.parseMapping(mappings);
                }
                catch (IOException error) {
                    throw new RuntimeException("Error retrieving mapping information for index (" +
                            index + "): " + error.getMessage());
                }
                finally {
                    client.shutdown();
                }
            }
            catch (RuntimeException error) {
                throw new RuntimeException("Error processing request to map index (" +
                        index + "): " + error.getMessage());
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
     * Used to split-up the reads required to scan the requested index/indices. This initial implementation supports
     * a single split solution.
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details of the catalog, database, table, andpartition(s) being queried as well as
     * any filter predicate.
     * @return A GetSplitsResponse which primarily contains:
     * 1. A Set<Split> which represent read operations Amazon Athena must perform by calling your read function.
     * 2. (Optional) A continuation token which allows you to paginate the generation of splits for large queries.
     * @note A Split is a mostly opaque object to Amazon Athena. Amazon Athena will use the optional SpillLocation and
     * optional EncryptionKey for pipelined reads but all properties you set on the Split are passed to your read
     * function to help you perform the read.
     * @throws RuntimeException when the domain does not exist in the map.
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
            throws RuntimeException
    {
        logger.info("doGetSplits: enter - " + request);

        String domain = request.getTableName().getSchemaName();

        // Every split must have a unique location if we wish to spill to avoid failures
        SpillLocation spillLocation = makeSpillLocation(request);

        // Create split
        Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey());

        try {
            String endpoint = getDomainEndpoint(domain);
            // Add domain and endpoint to the split to be used by the Record Handler.
            splitBuilder.add(domain, endpoint);
        }
        catch (RuntimeException error) {
            throw new RuntimeException("Error trying to generate splits: " + error.getMessage());
        }

        return new GetSplitsResponse(request.getCatalogName(), splitBuilder.build());
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
            domainMap = domainMapper.getDomainMapping(null);
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
        logger.info("convertField - fieldName: {}, glueType: {}", fieldName, glueType);

        return GlueFieldLexer.lex(fieldName, glueType, mapper);
    }
}
