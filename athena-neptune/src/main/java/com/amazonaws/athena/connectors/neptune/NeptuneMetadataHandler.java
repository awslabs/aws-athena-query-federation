/*-
 * #%L
 * athena-neptune
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
package com.amazonaws.athena.connectors.neptune;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
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
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connectors.neptune.propertygraph.PropertyGraphHandler;
import com.amazonaws.athena.connectors.neptune.qpt.NeptuneQueryPassthrough;
import com.amazonaws.athena.connectors.neptune.rdf.NeptuneSparqlConnection;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * This class is part of an tutorial that will walk you through how to build a
 * connector for your custom data source. The README for this module
 * (athena-example) will guide you through preparing your development
 * environment, modifying this example Metadatahandler, building, deploying, and
 * then using your new source in an Athena query.
 * <p>
 * More specifically, this class is responsible for providing Athena with
 * metadata about the schemas (aka databases), tables, and table partitions that
 * your source contains. Lastly, this class tells Athena how to split up reads
 * against this source. This gives you control over the level of performance and
 * parallelism your source can support.
 * <p>
 * For more examples, please see the other connectors in this repository (e.g.
 * athena-cloudwatch, athena-docdb, etc...)
 */
public class NeptuneMetadataHandler extends GlueMetadataHandler
{
    private final Logger logger = LoggerFactory.getLogger(NeptuneMetadataHandler.class);
    private static final String SOURCE_TYPE = "neptune"; // Used to denote the 'type' of this connector for diagnostic
                                                         // purposes.
    private final GlueClient glue;
    private final String glueDBName;

    private NeptuneConnection neptuneConnection = null;
    private final NeptuneQueryPassthrough queryPassthrough = new NeptuneQueryPassthrough();

    public NeptuneMetadataHandler(java.util.Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
        this.glue = getAwsGlue();
        // The original interface passed "false" for "disable_glue" previously so this requires
        // check is enforcing this connector's contract.
        requireNonNull(this.glue);
        this.glueDBName = configOptions.get("glue_database_name");
        this.neptuneConnection = NeptuneConnection.createConnection(configOptions);
    }

    @VisibleForTesting
    protected NeptuneMetadataHandler(
        GlueClient glue,
        NeptuneConnection neptuneConnection,
        EncryptionKeyFactory keyFactory,
        SecretsManagerClient awsSecretsManager,
        AthenaClient athena,
        String spillBucket,
        String spillPrefix,
        java.util.Map<String, String> configOptions)
    {
        super(glue, keyFactory, awsSecretsManager, athena, Constants.SOURCE_TYPE, spillBucket, spillPrefix, configOptions);
        this.glue = glue;
        this.glueDBName = configOptions.get("glue_database_name");
    }

    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = ImmutableMap.builder();
        queryPassthrough.addQueryPassthroughCapabilityIfEnabled(capabilities, configOptions);

        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }
    
    /**
     * Since the entire Neptune cluster is considered as a single graph database,
     * just return the glue database name provided as a single database (schema)
     * name.
     * 
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request   Provides details on who made the request and which Athena
     *                  catalog they are querying.
     * @return A ListSchemasResponse which primarily contains a Set<String> of
     *         schema names and a catalog name corresponding the Athena catalog that
     *         was queried.
     * @see GlueMetadataHandler
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        logger.info("doListSchemaNames: enter - " + request);

        Set<String> schemas = new HashSet<>();
        schemas.add(glueDBName);
        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    /**
     * Used to get the list of tables that this data source contains. In this case,
     * fetch list of tables in the Glue database provided.
     * 
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request   Provides details on who made the request and which Athena
     *                  catalog and database they are querying.
     * @return A ListTablesResponse which primarily contains a List<TableName>
     *         enumerating the tables in this catalog, database tuple. It also
     *         contains the catalog name corresponding the Athena catalog that was
     *         queried.
     * @see GlueMetadataHandler
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
    {
        logger.info("doListTables: enter - " + request);

        List<TableName> tables = new ArrayList<>();
        GetTablesRequest getTablesRequest = GetTablesRequest.builder()
                .databaseName(request.getSchemaName())
                .build();

        GetTablesResponse getTablesResponse = glue.getTables(getTablesRequest);
        List<Table> glueTableList = getTablesResponse.tableList();
        String schemaName = request.getSchemaName();
        glueTableList.forEach(e -> {
            tables.add(new TableName(schemaName, e.name()));
        });

        return new ListTablesResponse(request.getCatalogName(), tables, null);
    }

    /**
     * Used to get definition (field names, types, descriptions, etc...) of a Table.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request   Provides details on who made the request and which Athena
     *                  catalog, database, and table they are querying.
     * @return A GetTableResponse which primarily contains: 1. An Apache Arrow
     *         Schema object describing the table's columns, types, and
     *         descriptions. 2. A Set<String> of partition column names (or empty if
     *         the table isn't partitioned). 3. A TableName object confirming the
     *         schema and table name the response is for. 4. A catalog name
     *         corresponding the Athena catalog that was queried.
     * @throws Exception
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request) throws Exception
    {
        logger.info("doGetTable: enter - " + request.getTableName());
        Schema tableSchema = null;
        try {
            if (glue != null) {
                tableSchema = super.doGetTable(blockAllocator, request).getSchema();        
                logger.info("doGetTable: Retrieved schema for table[{}] from AWS Glue.", request.getTableName());
            }
        } 
        catch (RuntimeException ex) {
            logger.warn("doGetTable: Unable to retrieve table[{}:{}] from AWS Glue.",
                    request.getTableName().getSchemaName(), request.getTableName().getTableName(), ex);
        }
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), tableSchema);
    }

    /**
     * Our table doesn't support complex layouts or partitioning so we simply make
     * this method a NoOp.
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request,
            QueryStatusChecker queryStatusChecker) throws Exception 
    {
        // No implemenation as connector doesn't support partitioning
    }

    /**
     * Used to split-up the reads required to scan the requested batch of
     * partition(s).
     *
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param request        Provides details of the catalog, database, table,
     *                       andpartition(s) being queried as well as any filter
     *                       predicate.
     * @return A GetSplitsResponse which primarily contains: 1. A Set<Split> which
     *         represent read operations Amazon Athena must perform by calling your
     *         read function. 2. (Optional) A continuation token which allows you to
     *         paginate the generation of splits for large queries.
     * @note A Split is a mostly opaque object to Amazon Athena. Amazon Athena will
     *       use the optional SpillLocation and optional EncryptionKey for pipelined
     *       reads but all properties you set on the Split are passed to your read
     *       function to help you perform the read.
     */

    /*
     * Since our connector does not support parallel scans we generate a single
     * Split and include the connection details as a property on the split so that
     * the RecordHandler has easy access to it.
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest request) 
    {
        // Every split must have a unique location if we wish to spill to avoid failures
        SpillLocation spillLocation = makeSpillLocation(request);

        // Since our connector does not support parallel reads we return a fixed split.
        return new GetSplitsResponse(request.getCatalogName(),
                Split.newBuilder(spillLocation, makeEncryptionKey()).build());
    }

    @Override
    protected Field convertField(String name, String glueType) 
    {
        return GlueFieldLexer.lex(name, glueType);
    }

    @Override
    public GetTableResponse doGetQueryPassthroughSchema(BlockAllocator allocator, GetTableRequest request) throws Exception
    {
        Map<String, String> qptArguments = request.getQueryPassthroughArguments();
        queryPassthrough.verify(qptArguments);
        String schemaName = qptArguments.get(NeptuneQueryPassthrough.DATABASE);
        String tableName = qptArguments.get(NeptuneQueryPassthrough.COLLECTION);
        TableName tableNameObj = new TableName(schemaName, tableName);
        request = new GetTableRequest(request.getIdentity(), request.getQueryId(),
                request.getCatalogName(), tableNameObj, request.getQueryPassthroughArguments());

        GetTableResponse getTableResponse = doGetTable(allocator, request);
        List<Field> fields = getTableResponse.getSchema().getFields();
        Schema schema;
        Enums.GraphType graphType = Enums.GraphType.PROPERTYGRAPH;

        if (configOptions.get(Constants.CFG_GRAPH_TYPE) != null) {
            graphType = Enums.GraphType.valueOf(configOptions.get(Constants.CFG_GRAPH_TYPE).toUpperCase());
        }

        switch (graphType){
            case PROPERTYGRAPH:
                Client client = neptuneConnection.getNeptuneClientConnection();
                GraphTraversalSource graphTraversalSource = neptuneConnection.getTraversalSource(client);
                String gremlinQuery = qptArguments.get(NeptuneQueryPassthrough.QUERY);
                gremlinQuery = gremlinQuery.concat(".limit(1)");
                logger.info("NeptuneMetadataHandler doGetQueryPassthroughSchema gremlinQuery with limit: " + gremlinQuery);
                Object object = new PropertyGraphHandler(neptuneConnection).getResponseFromGremlinQuery(graphTraversalSource, gremlinQuery);
                GraphTraversal graphTraversalForSchema = (GraphTraversal) object;
                if (graphTraversalForSchema.hasNext()) {
                    Object responseObj = graphTraversalForSchema.next();
                    if (responseObj instanceof Map && gremlinQuery.contains(Constants.GREMLIN_QUERY_SUPPORT_TYPE)) {
                        logger.info("NeptuneMetadataHandler doGetQueryPassthroughSchema gremlinQuery with valueMap");
                        Map graphTraversalObj = (Map) responseObj;
                        schema = getSchemaFromResults(getTableResponse, graphTraversalObj, fields);
                        return new GetTableResponse(request.getCatalogName(), tableNameObj, schema);
                    }
                    else {
                        throw new RuntimeException("Unsupported gremlin query format: We are currently supporting only valueMap gremlin queries. " +
                                "Please make sure you are using valueMap gremlin query. " +
                                "Example for valueMap query is g.V().hasLabel(\\\"airport\\\").valueMap().limit(5)");
                    }
                }
                else {
                    throw new NoSuchElementException("No data available for gremlin query: " + gremlinQuery);
                }

            case RDF:
                String sparqlQuery = qptArguments.get(NeptuneQueryPassthrough.QUERY);
                sparqlQuery = sparqlQuery.contains("limit") ? sparqlQuery : sparqlQuery.concat("\nlimit 1");
                logger.info("NeptuneMetadataHandler doGetQueryPassthroughSchema sparql query with limit: " + sparqlQuery);
                NeptuneSparqlConnection neptuneSparqlConnection = (NeptuneSparqlConnection) neptuneConnection;
                neptuneSparqlConnection.runQuery(sparqlQuery);
                String strim = getTableResponse.getSchema().getCustomMetadata().get(Constants.SCHEMA_STRIP_URI);
                boolean trimURI = strim == null ? false : Boolean.parseBoolean(strim);
                if (neptuneSparqlConnection.hasNext()) {
                    Map<String, Object> resultsMap = neptuneSparqlConnection.next(trimURI);
                    schema = getSchemaFromResults(getTableResponse, resultsMap, fields);
                    return new GetTableResponse(request.getCatalogName(), tableNameObj, schema);
                }
                else {
                    throw new NoSuchElementException("No data available for sparql query: " + sparqlQuery);
                }

            default:
                throw new IllegalArgumentException("Unsupported graphType: " + graphType);
        }
    }

    private Schema getSchemaFromResults(GetTableResponse getTableResponse, Map resultsMap, List<Field> fields)
    {
        Schema schema;
        //In case of 'gremlin/sparql query' is fetching all columns then we can use same schema from glue
        //otherwise we will build schema from gremlin/sparql query result column names.
        if (resultsMap != null && resultsMap.size() == fields.size()) {
            schema = getTableResponse.getSchema();
        }
        else {
            SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
            //Building schema from gremlin/sparql query results and list of fields from glue response.
            //It's require only when we are selecting limited columns.
            resultsMap.forEach((columnName, columnValue) -> buildSchema(columnName.toString(), fields, schemaBuilder));
            Map<String, String> metaData = getTableResponse.getSchema().getCustomMetadata();
            for (Map.Entry<String, String> map : metaData.entrySet()) {
                schemaBuilder.addMetadata(map.getKey(), map.getValue());
            }
            schema = schemaBuilder.build();
        }
        return schema;
    }

    private void buildSchema(String columnName, List<Field> fields, SchemaBuilder schemaBuilder)
    {
        for (Field field : fields) {
            if (field.getName().equals(columnName)) {
                schemaBuilder.addField(field);
                break;
            }
        }
    }
}
