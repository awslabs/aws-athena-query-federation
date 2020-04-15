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
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
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
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

// Apache APIs
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Schema;

//DO NOT REMOVE - this will not be _unused_ when customers go through the tutorial and uncomment
//the TODOs
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Elasticsearch APIs
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;

// AWS SDK 1.x ES APIs
import com.amazonaws.services.elasticsearch.AWSElasticsearch;
import com.amazonaws.services.elasticsearch.AWSElasticsearchClientBuilder;
import com.amazonaws.services.elasticsearch.model.DomainInfo;
import com.amazonaws.services.elasticsearch.model.ListDomainNamesResult;
import com.amazonaws.services.elasticsearch.model.ListDomainNamesRequest;
import com.amazonaws.services.elasticsearch.model.DescribeElasticsearchDomainsResult;
import com.amazonaws.services.elasticsearch.model.DescribeElasticsearchDomainsRequest;
import com.amazonaws.services.elasticsearch.model.ElasticsearchDomainStatus;

// Guava
import com.google.common.base.Splitter;

// XML
import com.fasterxml.jackson.databind.ObjectMapper;

// Common
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Base64;

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
 * For more elasticsearchs, please see the other connectors in this repository (e.g. athena-cloudwatch, athena-docdb, etc...)
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
    private static final Map<String, String> domainMap = new HashMap<>();

    // Splitter for inline map properties extracted from the DOMAIN_MAPPING.
    private static final Splitter.MapSplitter domainSplitter = Splitter.on(",").trimResults().withKeyValueSeparator("=");

    // AWS ES Client for retrieving domain mapping info from the Amazon Elasticsearch Service.
    private static final AWSElasticsearch awsEsClient = AWSElasticsearchClientBuilder.defaultClient();

    private final AWSGlue awsGlue;
    private String username, password;

    public ElasticsearchMetadataHandler()
    {
        //Disable Glue if the env var is present and not explicitly set to "false"
        super((System.getenv(GLUE_ENV) != null && !"false".equalsIgnoreCase(System.getenv(GLUE_ENV))), SOURCE_TYPE);
        this.awsGlue = getAwsGlue();
        this.autoDiscoverEndpoint = System.getenv(AUTO_DISCOVER_ENDPOINT) != null &&
                System.getenv(AUTO_DISCOVER_ENDPOINT).equalsIgnoreCase("true");
        this.username = "";
        this.password = "";

        setDomainMapping();
        setUsernamePassword();
    }

    @VisibleForTesting
    protected ElasticsearchMetadataHandler(AWSGlue awsGlue,
                                           EncryptionKeyFactory keyFactory,
                                           AWSSecretsManager awsSecretsManager,
                                           AmazonAthena athena,
                                           String spillBucket,
                                           String spillPrefix)
    {
        super(awsGlue, keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
        this.awsGlue = awsGlue;
        this.autoDiscoverEndpoint = System.getenv(AUTO_DISCOVER_ENDPOINT) != null &&
                System.getenv(AUTO_DISCOVER_ENDPOINT).equalsIgnoreCase("true");
        this.username = "";
        this.password = "";

        setDomainMapping();
        setUsernamePassword();
    }

    /**
     * getDomainList
     *
     * Gets a list of domain names.
     *
     * @return a list of domain-names stored in the domainMap.
     */
    private final Set<String> getDomainList()
    {
        return domainMap.keySet();
    }

    /**
     * getDomainEndpoint
     *
     * Gets the domain's endpoint.
     *
     * @param domainName is the name associated with the endpoint.
     * @return a String containing the endpoint associated with the domainName, or an empty String if the domain-
     * name doesn't exist in the map.
     */
    private final String getDomainEndpoint(String domainName)
    {
        String endpoint = "";

        if (domainMap.containsKey(domainName)) {
            endpoint = domainMap.get(domainName);
        }

        return endpoint;
    }

    /**
     * setDomainMapping(String, String)
     *
     * Populates the domainMap with domainName and domainEndpoint.
     * NOTE: Only gets called from @Test
     */
    @VisibleForTesting
    public final void setDomainMapping(String domainName, String domainEndpoint)
    {
        // NOTE: Only gets called from @Test.
        domainMap.put(domainName, domainEndpoint);
    }

    /**
     * setDomainMapping(List<ElasticsearchDomainStatus>)
     *
     * Sets the domainMap with domain-names and corresponding endpoints retrieved from the AWS ES SDK.
     *
     * @param domainStatusList is a list of status objects returned by a listDomainNames request to the AWS ES SDK.
     */
    private final void setDomainMapping(List<ElasticsearchDomainStatus> domainStatusList)
    {
        domainMap.clear();
        for (ElasticsearchDomainStatus domainStatus : domainStatusList) {
            domainMap.put(domainStatus.getDomainName(), domainStatus.getEndpoint());
        }
    }

    /**
     * setDomainMapping(Map<String, String>)
     *
     * Sets the domainMap with domain-names and corresponding endpoints stored in a Map object.
     *
     * @param mapping is a map of domain-names and their corresponding endpoints.
     */
    private final void setDomainMapping(Map<String, String> mapping)
    {
        domainMap.clear();
        domainMap.putAll(mapping);
    }

    /**
     * setDomainMapping()
     *
     * Populates the domainMap with domain-mapping from either the domain_mapping environment variable
     * (auto_discover_endpoint is false), or from the AWS ES SDK (auto_discover_endpoint is true).
     * This method is called from the constructor(s) and from doListSchemaNames(). The call from the latter is used to
     * refresh the domainMap with the underlying assumption that domain(s) could have been added or deleted.
     */
    private void setDomainMapping()
    {
        logger.info("setDomainMapping: enter");

        if (autoDiscoverEndpoint) {
            // Get domain mapping via the AWS ES SDK (1.x).
            // NOTE: Gets called at construction and each call to doListSchemaNames() when autoDiscoverEndpoint is true.
            try {
                ListDomainNamesResult listDomainNamesResult = awsEsClient.listDomainNames(new ListDomainNamesRequest());
                List<String> domainNames = new ArrayList<>();
                for (DomainInfo domainInfo : listDomainNamesResult.getDomainNames()) {
                    domainNames.add(domainInfo.getDomainName());
                }

                DescribeElasticsearchDomainsRequest describeDomainsRequest = new DescribeElasticsearchDomainsRequest();
                describeDomainsRequest.setDomainNames(domainNames);
                DescribeElasticsearchDomainsResult describeDomainsResult =
                        awsEsClient.describeElasticsearchDomains(describeDomainsRequest);

                setDomainMapping(describeDomainsResult.getDomainStatusList());
            }
            catch (Exception error) {
                logger.error("Error getting list of domain names:", error);
            }
        }
        else {
            // Get domain mapping from environment variables.
            String domainMapping = System.getenv(DOMAIN_MAPPING);
            if (domainMapping != null) {
                setDomainMapping(domainSplitter.split(domainMapping));
            }
        }
    }

    /**
     * setUsernamePassword
     *
     * Sets the username and password from Secrets Manager when autoDiscoverEndpoint=false.
     */
    private void setUsernamePassword()
    {
        logger.info("setUsernamePassword: enter");

        if (autoDiscoverEndpoint)
            return;

        AWSSecretsManager awsSecretsManager =
                AWSSecretsManagerClientBuilder.standard().
                        withCredentials(new DefaultAWSCredentialsProviderChain()).build();

        try {
            final String secretName = "elasticsearch-creds";
            GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretName);
            GetSecretValueResult getSecretValueResult;
            getSecretValueResult = awsSecretsManager.getSecretValue(getSecretValueRequest);
            String secretString;

            if (getSecretValueResult.getSecretString() != null) {
                secretString = getSecretValueResult.getSecretString();
            } else {
                secretString = new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array());
            }

            Map<String, String> secrets = new ObjectMapper().readValue(secretString, HashMap.class);

            username = secrets.get("username");
            password = secrets.get("password");
        }
        catch (Exception error) {
            logger.error("Error accessing Secrets Manager:", error);
        }
    }

    /**
     * getClient
     *
     * Returns a client based on the autoDiscoverEndpoint environment variable.
     *
     * @param endpoint is the Elasticsearch cluster's domain endpoint.
     * @return an Elasticsearch Rest client. If autoDiscoverEndpoint = true, the client is injected
     *          with AWS Credentials. If autoDiscoverEndpoint = false and username/password are not
     *          empty, it is injected with username/password credentials. Otherwise a default client
     *          with no credentials is returned.
     */
    private RestHighLevelClient getClient(String endpoint)
    {
        logger.info("getClient: enter - " + endpoint);

        if (autoDiscoverEndpoint) {
            // For Amazon Elasticsearch service use client with injected AWS Credentials.
            return new AwsRestHighLevelClient.Builder(endpoint).
                    setCredentials(new DefaultAWSCredentialsProviderChain()).build();
        }
        else if (!username.isEmpty() && !password.isEmpty()) {
            // Create a client injected with username/password credentials.
            return new AwsRestHighLevelClient.Builder(endpoint).
                    setCredentials(username, password).build();
        }

        // Default client.
        return new AwsRestHighLevelClient.Builder(endpoint).build();
    }

    /**
     * Used to get the list of domains (aka databases) for the Elasticsearch service.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @return A ListSchemasResponse which primarily contains a Set<String> of schema names and a catalog name
     * corresponding the Athena catalog that was queried.
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        logger.info("doListSchemaNames: enter - " + request);

        if (autoDiscoverEndpoint) {
            // Since adding/deleting domains in Amazon Elasticsearch Service doesn't re-initialize
            // the connector, the domainMap needs to be refreshed each time for Amazon ES.
            setDomainMapping();
        }

        return new ListSchemasResponse(request.getCatalogName(), getDomainList());
    }

    /**
      * Used to get the list of indices contained in the specified domain.
      *
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
            try (RestHighLevelClient client = getClient(endpoint)) {
                GetAliasesRequest getAliasesRequest = new GetAliasesRequest();
                GetAliasesResponse getAliasesResponse =
                        client.indices().getAlias(getAliasesRequest, RequestOptions.DEFAULT);

                for (String index : getAliasesResponse.getAliases().keySet()) {
                    // Add all Indices except for kibana.
                    if (index.contains("kibana")) {
                        continue;
                    }

                    indices.add(new TableName(request.getSchemaName(), index));
                }
            } catch (Exception error) {
                logger.error("Error getting indices:", error);
            }
        }
        else {
            logger.warn("Domain does not exist in map: " + request.getSchemaName());
        }

        return new ListTablesResponse(request.getCatalogName(), indices);
    }

    /**
     * Used to get definition (field names, types, descriptions, etc...) of a Table.
     *
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

                try (RestHighLevelClient client = getClient(endpoint)) {
                    GetMappingsRequest mappingsRequest = new GetMappingsRequest();
                    mappingsRequest.indices(index);
                    GetMappingsResponse mappingsResponse =
                            client.indices().getMapping(mappingsRequest, RequestOptions.DEFAULT);

                    LinkedHashMap<String, Object> mapping =
                            (LinkedHashMap<String, Object>) mappingsResponse.mappings().get(index).sourceAsMap();

                    LinkedHashMap<String, Object> _meta = new LinkedHashMap<>();
                    if (mapping.containsKey("_meta")) {
                        _meta.putAll((LinkedHashMap<String, Object>) mapping.get("_meta"));
                    }

                    schema = ElasticsearchHelper.parseMapping(mapping, _meta);
                } catch (Exception error) {
                    logger.error("Error mapping index:", error);
                }
            }
        }

        return new GetTableResponse(request.getCatalogName(), request.getTableName(),
                (schema == null) ? SchemaBuilder.newBuilder().build() : schema, Collections.emptySet());
    }

    /**
     * Used to get the partitions that must be read from the request table in order to satisfy the requested predicate.
     *
     * @param blockWriter Used to write rows (partitions) into the Apache Arrow response.
     * @param request Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @note Partitions are partially opaque to Amazon Athena in that it only understands your partition columns and
     * how to filter out partitions that do not meet the query's constraints. Any additional columns you add to the
     * partition data are ignored by Athena but passed on to calls on GetSplits.
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        for (int year = 2000; year < 2018; year++) {
            for (int month = 1; month < 12; month++) {
                for (int day = 1; day < 31; day++) {

                    final int yearVal = year;
                    final int monthVal = month;
                    final int dayVal = day;
                    blockWriter.writeRows((Block block, int row) -> {
                    boolean matched = true;
                    matched &= block.setValue("year", row, yearVal);
                    matched &= block.setValue("month", row, monthVal);
                    matched &= block.setValue("day", row, dayVal);
                    //If all fields matches then we wrote 1 row during this call so we return 1
                    return matched ? 1 : 0;
                    });
                }
            }
        }
    }

    /**
     * Used to split-up the reads required to scan the requested batch of partition(s).
     *
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

        String catalogName = request.getCatalogName();
        Set<Split> splits = new HashSet<>();

        Block partitions = request.getPartitions();

        FieldReader day = partitions.getFieldReader("day");
        FieldReader month = partitions.getFieldReader("month");
        FieldReader year = partitions.getFieldReader("year");
        for (int i = 0; i < partitions.getRowCount(); i++) {
            //Set the readers to the partition row we area on
            year.setPosition(i);
            month.setPosition(i);
            day.setPosition(i);

            Split split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
             .add("year", String.valueOf(year.readInteger()))
             .add("month", String.valueOf(month.readInteger()))
             .add("day", String.valueOf(day.readInteger()))
             .build();

            splits.add(split);
        }

        logger.info("doGetSplits: exit - " + splits.size());
        return new GetSplitsResponse(catalogName, splits);
    }
}
