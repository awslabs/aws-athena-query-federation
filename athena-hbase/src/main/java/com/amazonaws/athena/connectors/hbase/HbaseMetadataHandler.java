/*-
 * #%L
 * athena-hbase
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
package com.amazonaws.athena.connectors.hbase;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.metadata.glue.GlueFieldLexer;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handles metadata requests for the Athena HBase Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Uses a Glue table property (hbase-metadata-flag) to indicate that the table (whose name matched the HBase table
 * name) can indeed be used to supplement metadata from HBase itself.
 * 2. Uses a Glue table property (hbase-native-storage-flag) to indicate that the table is stored in HBase
 * using native byte storage (e.g. int as 4 BYTES instead of int serialized as a String).
 * 3. Attempts to resolve sensitive fields such as HBase connection strings via SecretsManager so that you can substitute
 * variables with values from by doing something like hostname:port:password=${my_secret}
 */
public class HbaseMetadataHandler
        extends GlueMetadataHandler
{
    //FLAG used to indicate the given table is stored using HBase native formatting not as strings
    protected static final String HBASE_NATIVE_STORAGE_FLAG = "hbase-native-storage-flag";
    //Field name used to store the connection string as a property on Split objects.
    protected static final String HBASE_CONN_STR = "connStr";
    //Field name used to store the HBase scan start key as a property on Split objects.
    protected static final String START_KEY_FIELD = "start_key";
    //Field name used to store the HBase scan end key as a property on Split objects.
    protected static final String END_KEY_FIELD = "end_key";
    //Field name used to store the HBase region id as a property on Split objects.
    protected static final String REGION_ID_FIELD = "region_id";
    //Field name used to store the HBase region name as a property on Split objects.
    protected static final String REGION_NAME_FIELD = "region_name";
    private static final Logger logger = LoggerFactory.getLogger(HbaseMetadataHandler.class);
    //The Env variable name used to store the default HBase connection string if no catalog specific
    //env variable is set.
    private static final String DEFAULT_HBASE = "default_hbase";
    //The Glue table property that indicates that a table matching the name of an HBase table
    //is indeed enabled for use by this connector.
    private static final String HBASE_METADATA_FLAG = "hbase-metadata-flag";
    //Used to filter out Glue tables which lack HBase metadata flag.
    private static final TableFilter TABLE_FILTER = (Table table) -> table.getParameters().containsKey(HBASE_METADATA_FLAG);
    //The Env variable name used to indicate that we want to disable the use of Glue DataCatalog for supplemental
    //metadata and instead rely solely on the connector's schema inference capabilities.
    private static final String GLUE_ENV_VAR = "disable_glue";
    //Used to denote the 'type' of this connector for diagnostic purposes.
    private static final String SOURCE_TYPE = "hbase";
    //The number of rows to scan when attempting to infer schema from an HBase table.
    private static final int NUM_ROWS_TO_SCAN = 10;
    private final AWSGlue awsGlue;
    private final HbaseConnectionFactory connectionFactory;

    public HbaseMetadataHandler()
    {
        super((System.getenv(GLUE_ENV_VAR) == null) ? AWSGlueClientBuilder.standard().build() : null, SOURCE_TYPE);
        this.awsGlue = getAwsGlue();
        this.connectionFactory = new HbaseConnectionFactory();
    }

    @VisibleForTesting
    protected HbaseMetadataHandler(AWSGlue awsGlue,
            EncryptionKeyFactory keyFactory,
            AWSSecretsManager secretsManager,
            HbaseConnectionFactory connectionFactory,
            String spillBucket,
            String spillPrefix)
    {
        super(awsGlue, keyFactory, secretsManager, SOURCE_TYPE, spillBucket, spillPrefix);
        this.awsGlue = awsGlue;
        this.connectionFactory = connectionFactory;
    }

    private Connection getOrCreateConn(MetadataRequest request)
    {
        String endpoint = resolveSecrets(getConnStr(request));
        return connectionFactory.getOrCreateConn(endpoint);
    }

    /**
     * Retrieves the HBase connection details from an env variable matching the catalog name, if no such
     * env variable exists we fall back to the default env variable defined by DEFAULT_HBASE.
     */
    private String getConnStr(MetadataRequest request)
    {
        String conStr = System.getenv(request.getCatalogName());
        if (conStr == null) {
            logger.info("getConnStr: No environment variable found for catalog {} , using default {}",
                    request.getCatalogName(), DEFAULT_HBASE);
            conStr = System.getenv(DEFAULT_HBASE);
        }
        return conStr;
    }

    /**
     * List namespaces in your HBase instance treating each as a 'schema' (aka database)
     *
     * @see GlueMetadataHandler
     */
    @Override
    protected ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest request)
            throws IOException
    {
        Connection conn = getOrCreateConn(request);
        Admin admin = conn.getAdmin();
        List<String> schemas = new ArrayList<>();
        NamespaceDescriptor[] namespaces = admin.listNamespaceDescriptors();
        for (int i = 0; i < namespaces.length; i++) {
            NamespaceDescriptor namespace = namespaces[i];
            schemas.add(namespace.getName());
        }
        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    /**
     * List tables in the requested schema in your HBase instance treating the requested schema as an HBase
     * namespace.
     *
     * @see GlueMetadataHandler
     */
    @Override
    protected ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest request)
            throws IOException
    {
        Connection conn = getOrCreateConn(request);
        Admin admin = conn.getAdmin();
        List<com.amazonaws.athena.connector.lambda.domain.TableName> tableNames = new ArrayList<>();

        TableName[] tables = admin.listTableNamesByNamespace(request.getSchemaName());
        for (int i = 0; i < tables.length; i++) {
            TableName tableName = tables[i];
            tableNames.add(new com.amazonaws.athena.connector.lambda.domain.TableName(request.getSchemaName(),
                    tableName.getNameAsString().replace(request.getSchemaName() + ":", "")));
        }
        return new ListTablesResponse(request.getCatalogName(), tableNames);
    }

    /**
     * If Glue is enabled as a source of supplemental metadata we look up the requested Schema/Table in Glue and
     * filters out any results that don't have the HBASE_METADATA_FLAG set. If no matching results were found in Glue,
     * then we resort to inferring the schema of the HBase table using HbaseSchemaUtils.inferSchema(...). If there
     * is no such table in HBase the operation will fail.
     *
     * @see GlueMetadataHandler
     */
    @Override
    protected GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request)
            throws Exception
    {
        logger.info("doGetTable: enter", request.getTableName());
        Schema origSchema = null;
        try {
            if (awsGlue != null) {
                origSchema = super.doGetTable(blockAllocator, request, TABLE_FILTER).getSchema();
            }
        }
        catch (RuntimeException ex) {
            logger.warn("doGetTable: Unable to retrieve table[{}:{}] from AWSGlue.",
                    request.getTableName().getSchemaName(),
                    request.getTableName().getTableName(),
                    ex);
        }

        if (origSchema == null) {
            Connection conn = getOrCreateConn(request);
            origSchema = HbaseSchemaUtils.inferSchema(conn, request.getTableName(), NUM_ROWS_TO_SCAN);
        }

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        origSchema.getFields().forEach((Field field) ->
                schemaBuilder.addField(field.getName(), field.getType(), field.getChildren())
        );

        origSchema.getCustomMetadata().entrySet().forEach((Map.Entry<String, String> meta) ->
                schemaBuilder.addMetadata(meta.getKey(), meta.getValue()));

        schemaBuilder.addField(HbaseSchemaUtils.ROW_COLUMN_NAME, Types.MinorType.VARCHAR.getType());

        Schema schema = schemaBuilder.build();
        logger.info("doGetTable: return {}", schema);
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema);
    }

    /**
     * Even though our table doesn't support complex layouts or partitioning, we need to convey that there is at least
     * 1 partition to read as part of the query or Athena will assume partition pruning found no candidate layouts to read.
     *
     * @see GlueMetadataHandler
     */
    @Override
    protected GetTableLayoutResponse doGetTableLayout(BlockAllocator blockAllocator, GetTableLayoutRequest request)
    {
        //Even though our table doesn't support complex layouts or partitioning, we need to convey that there is at least
        //1 partition to read as part of the query or Athena will assume partition pruning found no candidate layouts to read.
        Block partitions = BlockUtils.newBlock(blockAllocator,
                "partitionId",
                Types.MinorType.INT.getType(),
                0);

        return new GetTableLayoutResponse(request.getCatalogName(), request.getTableName(), partitions, new HashSet<>());
    }

    /**
     * If the table is spread across multiple region servers, then we parallelize the scan by making each region server a split.
     *
     * @see GlueMetadataHandler
     */
    @Override
    protected GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest request)
            throws IOException
    {
        Set<Split> splits = new HashSet<>();
        Connection conn = getOrCreateConn(request);
        Admin admin = conn.getAdmin();

        //We can read each region in parallel
        for (HRegionInfo info : admin.getTableRegions(HbaseSchemaUtils.getQualifiedTable(request.getTableName()))) {
            Split.Builder splitBuilder = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                    .add(HBASE_CONN_STR, getConnStr(request))
                    .add(START_KEY_FIELD, new String(info.getStartKey()))
                    .add(END_KEY_FIELD, new String(info.getEndKey()))
                    .add(REGION_ID_FIELD, String.valueOf(info.getRegionId()))
                    .add(REGION_NAME_FIELD, info.getRegionNameAsString());

            splits.add(splitBuilder.build());
        }

        return new GetSplitsResponse(request.getCatalogName(), splits, null);
    }

    /**
     * @see GlueMetadataHandler
     */
    @Override
    protected Field convertField(String name, String glueType)
    {
        return GlueFieldLexer.lex(name, glueType);
    }
}
