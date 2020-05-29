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
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is part of an tutorial that will walk you through how to build a connector for your
 * custom data source. The README for this module (athena-elasticsearch) will guide you through preparing
 * your development environment, modifying this elasticsearch Metadatahandler, building, deploying, and then
 * using your new source in an Athena query.
 * <p>
 * More specifically, this class is responsible for providing Athena with metadata about the schemas (aka databases),
 * tables, and table partitions that your source contains. Lastly, this class tells Athena how to split up reads against
 * this source. This gives you control over the level of performance and parallelism your source can support.
 * <p>
 * For more examples, please see the other connectors in this repository (e.g. athena-cloudwatch, athena-docdb, etc...)
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

    // Env. variable that holds the mappings of the domain-names to their respective endpoints. The contents of
    // this environment variable is fed into the domainSplitter to populate the domainMap where the key = domain-name,
    // and the value = endpoint.
    private static final String DOMAIN_MAPPING = "domain_mapping";
    // A Map of the domain-names and their respective endpoints.
    private Map<String, String> domainMap;

    private final AWSGlue awsGlue;
    private final AwsRestHighLevelClientFactory clientFactory;
    private final ElasticsearchSchemaUtil schemaUtil;
    private final ElasticsearchHelper helper;

    public ElasticsearchMetadataHandler()
    {
        //Disable Glue if the env var is present and not explicitly set to "false"
        super((System.getenv(GLUE_ENV) != null && !"false".equalsIgnoreCase(System.getenv(GLUE_ENV))), SOURCE_TYPE);
        this.awsGlue = getAwsGlue();
        this.schemaUtil = new ElasticsearchSchemaUtil();
        this.helper = new ElasticsearchHelper();
        this.domainMap = helper.getDomainMapping(resolveSecrets(this.helper.getEnv(DOMAIN_MAPPING)));
        this.clientFactory = this.helper.getClientFactory();
    }

    @VisibleForTesting
    protected ElasticsearchMetadataHandler(AWSGlue awsGlue,
                                           EncryptionKeyFactory keyFactory,
                                           AWSSecretsManager awsSecretsManager,
                                           AmazonAthena athena,
                                           String spillBucket,
                                           String spillPrefix,
                                           ElasticsearchHelper helper)
    {
        super(awsGlue, keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
        this.awsGlue = awsGlue;
        this.schemaUtil = new ElasticsearchSchemaUtil();
        this.helper = helper;
        this.domainMap = this.helper.getDomainMapping(resolveSecrets(this.helper.getEnv(DOMAIN_MAPPING)));
        this.clientFactory = this.helper.getClientFactory();
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
      */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
    {
        logger.info("doListTables: enter - " + request);

        List<TableName> indices = new ArrayList<>();
        String endpoint = getDomainEndpoint(request.getSchemaName());

        if (!endpoint.isEmpty()) {
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
                logger.error("Error getting indices:", error);
            }
            finally {
                client.shutdown();
            }
        }
        else {
            logger.warn("Domain does not exist in map: " + request.getSchemaName());
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
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
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
            String endpoint = getDomainEndpoint(request.getTableName().getSchemaName());

            if (!endpoint.isEmpty()) {
                String index = request.getTableName().getTableName();
                AwsRestHighLevelClient client = clientFactory.getClient(endpoint);
                try {
                    LinkedHashMap<String, Object> mappings = client.getMapping(index);
                    LinkedHashMap<String, Object> meta = new LinkedHashMap<>();

                    // Elasticsearch does not have a dedicated array type. All fields can contain one or more elements
                    // so long as they are of the same type. For this reasons, users will have to add a _meta property
                    // to the indices they intend on using with Athena. This property is used in the building of the
                    // Schema to indicate which fields should be considered a LIST.
                    if (mappings.containsKey("_meta")) {
                        meta.putAll((LinkedHashMap) mappings.get("_meta"));
                    }

                    schema = schemaUtil.parseMapping(mappings, meta);
                }
                catch (IOException error) {
                    logger.error("Error mapping index:", error);
                }
                finally {
                    client.shutdown();
                }
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
     * @throws Exception
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
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
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        logger.info("doGetSplits: enter - " + request);

        //Every split must have a unique location if we wish to spill to avoid failures
        SpillLocation spillLocation = makeSpillLocation(request);

        //Since our connector does not support parallel reads we return a fixed split.
        return new GetSplitsResponse(request.getCatalogName(),
                Split.newBuilder(spillLocation, makeEncryptionKey()).build());
    }

    /**
     * Gets an endpoint from the domain mapping.
     * @param domain is used for searching the domain map for the corresponding endpoint.
     * @return endpoint corresponding to the domain or an empty string if domain does not exist in the map.
     */
    private String getDomainEndpoint(String domain)
    {
        String endpoint = "";

        if (domainMap.containsKey(domain)) {
            return domainMap.get(domain);
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

        return GlueFieldLexer.lex(fieldName, glueType.toLowerCase());
    }
}
